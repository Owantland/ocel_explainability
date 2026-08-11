"""
Showcase: does _graph_dissimilarity()'s 4-component score (feat/type/edge/struct)
actually respond to the *specific* kind of difference it's meant to measure?

So far the metric (explainer.py:_graph_dissimilarity, adapted from Zhai et al. 2025)
has only ever been exercised on pairs of real test-set traces, where all 4 components
move at once in whatever way the data happens to differ -- there's no direct evidence
that each component is sensitive to its own difference type specifically, rather than
just correlating with "how different are these two graphs overall".

This script takes ONE real logistics test graph and builds 6 deliberately controlled
variants against it:
  1. self          -- an exact clone (control: everything should read ~0)
  2. feature-only   -- shift Container's raw feature values, no node/edge count change
  3. type-only       -- add one edgeless HandlingUnit node (copied features, so it
                         doesn't also perturb d_feat), no edge change
  4. edge-type-only  -- swap one edge's relation type (remove one Truck->Container edge,
                         add one CustomerOrder->Events edge), holding total edge count
                         and node counts fixed
  5. structure-only  -- remove 4 existing Events->Events edges, deflating total edge
                         count for one type only (see the note below on why this also
                         nudges d_edge -- that's a real, honest property of the metric,
                         not a bug in this demo)
  6. combined        -- all four edits applied together, to confirm `total` correctly
                         aggregates a graph that's different in every dimension at once

Note on d_edge vs. d_struct: both are edge-count-derived, so they are not fully
independent. Multiset-Jaccard similarity (d_edge) is not scale-invariant -- changing
one edge type's count (up or down) changes that type's min/max ratio even though every
*other* type's count is untouched, so scenario 5 will show a real secondary d_edge
movement alongside d_struct. This script reports that honestly rather than engineering
an artificially clean edge/structure separation.

Reuses explainer.py's actual `_graph_dissimilarity()` method -- no reimplementation.
"""
import os
import torch
import numpy as np
import pandas as pd

import explainer as exp

DATABASE = 'logistics'
CANT = 1000
OUT_DIR = f"files/explainer_outputs/{DATABASE}/dissimilarity_showcase"
os.makedirs(OUT_DIR, exist_ok=True)

e = exp.Explainer(DATABASE, CANT)

# Pick a base graph with a reasonable number of nodes across several types.
base = None
for g in e.test_data:
    if g['HandlingUnit'].num_nodes >= 3 and g['Container'].num_nodes >= 2 and g['Events'].num_nodes >= 5:
        base = g
        break
assert base is not None, "no suitable base graph found"

print("Base graph node counts:", {nt: base[nt].num_nodes for nt in base.node_types})
print("Base graph total edges:", sum(base[et].edge_index.size(1) for et in base.edge_types))

scenarios = {}

# 1. Self-comparison control
scenarios['1_self'] = base.clone()

# 2. Feature-only: shift Container's raw feature values (AmountofHandlingUnits, Weight)
g2 = base.clone()
g2['Container'].x = g2['Container'].x * 3.0 + 5.0
scenarios['2_feature_only'] = g2

# 3. Node-type-count-only: add one edgeless HandlingUnit node, features copied from an
#    existing HandlingUnit node so it doesn't also perturb d_feat.
g3 = base.clone()
g3['HandlingUnit'].x = torch.cat([g3['HandlingUnit'].x, g3['HandlingUnit'].x[0:1].clone()], dim=0)
scenarios['3_type_only'] = g3

# 4. Edge-type-only: remove one ('Truck','to','Container') edge (+ its 'rev_to' mirror),
#    add one ('CustomerOrder','to','Events') edge (+ its 'rev_to' mirror) elsewhere --
#    total edge count and node counts held fixed.
g4 = base.clone()


def drop_edge(g, edge_type, col):
    ei = g[edge_type].edge_index
    keep = torch.ones(ei.size(1), dtype=torch.bool)
    keep[col] = False
    g[edge_type].edge_index = ei[:, keep]


def add_edge(g, edge_type, src_idx, dst_idx):
    ei = g[edge_type].edge_index
    new_col = torch.tensor([[src_idx], [dst_idx]], dtype=ei.dtype)
    g[edge_type].edge_index = torch.cat([ei, new_col], dim=1)


drop_edge(g4, ('Truck', 'to', 'Container'), 0)
drop_edge(g4, ('Container', 'rev_to', 'Truck'), 0)
add_edge(g4, ('CustomerOrder', 'to', 'Events'), 0, 0)
add_edge(g4, ('Events', 'rev_to', 'CustomerOrder'), 0, 0)
scenarios['4_edge_type_only'] = g4

# 5. Structure-only (edge-count deflation): remove up to 4 existing
#    ('Events','to','Events') edges -- deflates total edge count via a single edge
#    type. Capped so at least 2 edges of that type survive, so this reads as "count
#    reduced" rather than "type removed entirely".
g5 = base.clone()
ei = g5[('Events', 'to', 'Events')].edge_index
n_drop = min(4, max(ei.size(1) - 2, 0))
g5[('Events', 'to', 'Events')].edge_index = ei[:, n_drop:]
scenarios['5_structure_only'] = g5

# 6. Combined: all four edits together
g6 = base.clone()
g6['Container'].x = g6['Container'].x * 3.0 + 5.0
g6['HandlingUnit'].x = torch.cat([g6['HandlingUnit'].x, g6['HandlingUnit'].x[0:1].clone()], dim=0)
drop_edge(g6, ('Truck', 'to', 'Container'), 0)
drop_edge(g6, ('Container', 'rev_to', 'Truck'), 0)
add_edge(g6, ('CustomerOrder', 'to', 'Events'), 0, 0)
add_edge(g6, ('Events', 'rev_to', 'CustomerOrder'), 0, 0)
ei6 = g6[('Events', 'to', 'Events')].edge_index
g6[('Events', 'to', 'Events')].edge_index = ei6[:, n_drop:]
scenarios['6_combined'] = g6

# ── Score each scenario against the (unmodified) base ────────────────────────
rows = []
for name, g in scenarios.items():
    total, comps = e._graph_dissimilarity(base, g)
    rows.append({'scenario': name, **comps, 'total': total})
    print(f"{name:20s}  feat={comps['feat']:.3f}  type={comps['type']:.3f}  "
          f"edge={comps['edge']:.3f}  struct={comps['struct']:.3f}  total={total:.3f}")

table = pd.DataFrame(rows)

# Delta-from-self: self-comparison (scenario 1) is NOT guaranteed to be all-zero --
# d_feat compares each seed node to the *average* of all same-typed nodes on the other
# side, so a node type with several distinct-featured instances (e.g. Container here)
# gives a nonzero d_feat even for an identical graph. Reporting deltas relative to the
# self row isolates "how much extra dissimilarity did this specific edit introduce",
# which is the cleaner signal for showing whether each component responds specifically.
self_row = table[table['scenario'] == '1_self'][['feat', 'type', 'edge', 'struct']].iloc[0]
for comp in ['feat', 'type', 'edge', 'struct']:
    table[f'delta_{comp}'] = table[comp] - self_row[comp]

csv_path = f"{OUT_DIR}/dissimilarity_showcase.csv"
table.to_csv(csv_path, index=False)
print(f"\nSaved {csv_path}")
print(f"\nNote: self-comparison's d_feat = {self_row['feat']:.3f}, not 0 -- d_feat compares "
      f"each seed node to the AVERAGE of all same-typed nodes on the other side, so it is "
      f"not a strict identity metric when a node type (here, Container) has multiple "
      f"distinct-featured instances. Delta-from-self columns isolate each edit's true effect.")
print(table.to_string(index=False))

# ── Visualize the actual graph structures, not just the scores ───────────────
# Reuses explainer.py's own _hetero_to_nx() (same helper behind _plot_cf_graph_structures'
# real query-vs-CF comparison). NOT _draw_hetero_nx(), though -- that recomputes an
# independent layout per call, which is right when comparing two genuinely different
# traces but wrong here: it would place the same node at a different spot in every
# panel, making it impossible to visually spot what actually changed. Instead, compute
# ONE layout (on the union of all scenarios, i.e. the graph with the extra node) and
# reuse those exact positions in every panel, so an edge appearing/disappearing or a
# node appearing is visible as a direct diff against the same backdrop.
import matplotlib.pyplot as plt
import networkx as nx

PANEL_TITLES = {
    '1_self':           'base graph\n(unaltered)',
    '2_feature_only':   'feature values altered\n(Container x scaled)',
    '3_type_only':      'node-type count altered\n(+1 HandlingUnit, edgeless)',
    '4_edge_type_only': 'edge-type composition altered\n(Truck-Container -> CustomerOrder-Events)',
    '5_structure_only': f'edge count altered\n(-{n_drop} Events-Events edges)',
    '6_combined':        'combined\n(all four edits)',
}
panel_order = ['1_self', '2_feature_only', '3_type_only',
               '4_edge_type_only', '5_structure_only', '6_combined']

palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
           "#937860", "#DA8BC3", "#8C8C8C"]
all_types = sorted(base.node_types)
type_colors = {nt: palette[i % len(palette)] for i, nt in enumerate(all_types)}
seed_key = (e.kpi_viewpoint, 0)

# Union graph (combined scenario is a superset of every other scenario's node set)
# for a single shared layout.
G_union = e._hetero_to_nx(scenarios['6_combined'])
try:
    pos = nx.kamada_kawai_layout(G_union)
except Exception:
    pos = nx.spring_layout(G_union, seed=42, k=0.9)
# The extra HandlingUnit node is edgeless in every scenario that has it, so
# kamada_kawai (which needs edges to place a node meaningfully) may push it far away
# or overlap it -- nudge it next to the HandlingUnit node it was copied from instead.
new_hu_key = ('HandlingUnit', base['HandlingUnit'].num_nodes)  # the appended node's key
if new_hu_key in pos:
    anchor = ('HandlingUnit', 0)
    pos[new_hu_key] = (pos[anchor][0] + 0.08, pos[anchor][1] + 0.08)


from collections import Counter

G_base = e._hetero_to_nx(base)
base_node_set = set(G_base.nodes)
base_edge_counts = Counter((u, v, d['edge_type']) for u, v, d in G_base.edges(data=True))

RED = '#e34948'    # palette.md slot 6 -- removed
GREEN = '#1baf7a'  # palette.md slot 2 -- added (reused here as a "changed" accent, not identity)


ORANGE = '#eda100'  # palette.md slot 3 -- feature values changed (no structural change to show)


def draw_with_fixed_layout(G, ax, title, is_base=False, feature_changed_type=None):
    node_set = set(G.nodes)
    new_nodes = node_set - base_node_set if not is_base else set()
    edge_counts = Counter((u, v, d['edge_type']) for u, v, d in G.edges(data=True))
    added_keys = (edge_counts - base_edge_counts) if not is_base else Counter()
    removed_keys = (base_edge_counts - edge_counts) if not is_base else Counter()
    feat_changed_nodes = ({n for n in G.nodes if n[0] == feature_changed_type}
                           if feature_changed_type else set())

    node_colors = [type_colors.get(attrs['node_type'], 'gray') for _, attrs in G.nodes(data=True)]
    node_sizes = [420 if node == seed_key else (280 if node in new_nodes else 180) for node in G.nodes]
    edgecolors = ['black' if node == seed_key else
                  (GREEN if node in new_nodes else
                   (ORANGE if node in feat_changed_nodes else 'none')) for node in G.nodes]
    linewidths = [1.8 if node == seed_key else
                  (2.5 if (node in new_nodes or node in feat_changed_nodes) else 0) for node in G.nodes]
    sub_pos = {n: pos[n] for n in G.nodes}
    nx.draw_networkx_nodes(G, sub_pos, ax=ax, node_color=node_colors, node_size=node_sizes,
                            edgecolors=edgecolors, linewidths=linewidths, alpha=0.9)

    # Draw unchanged edges as recessive gray, added edges as a thick green highlight.
    # Removed edges are drawn as dashed red *using the base graph's own edge list*
    # (they don't exist in G, so they're not something draw_networkx_edges can find here).
    normal_edges, added_edges = [], []
    for u, v, d in G.edges(data=True):
        key = (u, v, d['edge_type'])
        (added_edges if added_keys.get(key, 0) > 0 else normal_edges).append((u, v))
        if added_keys.get(key, 0) > 0:
            added_keys[key] -= 1
    nx.draw_networkx_edges(G, sub_pos, ax=ax, edgelist=normal_edges, edge_color="gray",
                            width=1.0, alpha=0.4, arrows=True, connectionstyle="arc3,rad=0.1")
    if added_edges:
        nx.draw_networkx_edges(G, sub_pos, ax=ax, edgelist=added_edges, edge_color=GREEN,
                                width=2.4, alpha=0.95, arrows=True, connectionstyle="arc3,rad=0.1")
    if removed_keys:
        removed_edges = []
        rk = Counter(removed_keys)
        for u, v, d in G_base.edges(data=True):
            key = (u, v, d['edge_type'])
            if rk.get(key, 0) > 0:
                removed_edges.append((u, v))
                rk[key] -= 1
        nx.draw_networkx_edges(G_base, sub_pos, ax=ax, edgelist=removed_edges, edge_color=RED,
                                width=2.4, alpha=0.95, style='dashed', arrows=True,
                                connectionstyle="arc3,rad=0.1")

    labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes}
    nx.draw_networkx_labels(G, sub_pos, ax=ax, labels=labels, font_size=6)
    ax.set_title(title, fontsize=10)
    ax.axis("off")


FEATURE_CHANGED_TYPE = {'2_feature_only': 'Container', '6_combined': 'Container'}

fig, axes = plt.subplots(2, 3, figsize=(19, 12))
for ax, name in zip(axes.flat, panel_order):
    g = scenarios[name]
    G = e._hetero_to_nx(g)
    n_nodes = sum(g[nt].num_nodes for nt in g.node_types)
    n_edges = sum(g[et].edge_index.size(1) for et in g.edge_types)
    title = f"{PANEL_TITLES[name]}\n{n_nodes} nodes, {n_edges} edges"
    draw_with_fixed_layout(G, ax, title, is_base=(name == '1_self'),
                            feature_changed_type=FEATURE_CHANGED_TYPE.get(name))

type_handles = [plt.Line2D([0], [0], marker="o", color="w", label=nt,
                            markerfacecolor=c, markersize=10)
                for nt, c in type_colors.items()]
diff_handles = [
    plt.Line2D([0], [0], marker="o", color="w", label="node added",
               markerfacecolor="w", markeredgecolor=GREEN, markeredgewidth=2.2, markersize=10),
    plt.Line2D([0], [0], marker="o", color="w", label="node's features changed",
               markerfacecolor="w", markeredgecolor=ORANGE, markeredgewidth=2.2, markersize=10),
    plt.Line2D([0], [0], color=GREEN, lw=2.4, label="edge added"),
    plt.Line2D([0], [0], color=RED, lw=2.4, linestyle='dashed', label="edge removed"),
]
fig.legend(handles=type_handles + diff_handles, loc="lower center",
           ncol=min(len(all_types) + len(diff_handles), 9), fontsize=8.5, bbox_to_anchor=(0.5, -0.02))
fig.suptitle("Graph structure comparison: base vs. each targeted alteration "
             "(shared node layout -- differences are directly visible)", fontsize=13)
plt.tight_layout(rect=[0, 0.04, 1, 0.96])
struct_path = f"{OUT_DIR}/dissimilarity_showcase_graphs.png"
plt.savefig(struct_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"Saved {struct_path}")

# ── Heatmap: which component reacts to which alteration ──────────────────────
# Built from the same `table` computed above (delta-from-self columns) -- kept in
# the same script as the CSV/structure-diagram outputs so all three regenerate
# together and can never drift out of sync with each other.
from matplotlib.colors import LinearSegmentedColormap

SCENARIO_LABELS = {
    '1_self':           'self\n(control)',
    '2_feature_only':   'feature\nvalues',
    '3_type_only':      'node-type\ncount',
    '4_edge_type_only': 'edge-type\ncomposition',
    '5_structure_only': 'edge count\n(structure)',
    '6_combined':        'combined',
}
COMPONENTS = ['feat', 'type', 'edge', 'struct']
COMPONENT_LABELS = ['d_feat', 'd_type', 'd_edge', 'd_struct']

heat_rows = table['scenario'].tolist()
row_labels = [SCENARIO_LABELS[r] for r in heat_rows]
heat_data = table[[f'delta_{c}' for c in COMPONENTS]].values

INK_PRIMARY = '#0b0b0b'
INK_SECONDARY = '#52514e'
INK_MUTED = '#898781'
SURFACE = '#fcfcfb'

# palette.md sequential blue ramp, light -> dark (steps 100..700)
blue_steps = ['#cde2fb', '#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95', '#0d366b']
cmap = LinearSegmentedColormap.from_list('seq_blue', blue_steps, N=256)

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Helvetica Neue', 'Arial', 'DejaVu Sans']

fig2, ax2 = plt.subplots(figsize=(7.5, 6), facecolor=SURFACE)
fig2.patch.set_facecolor(SURFACE)
ax2.set_facecolor(SURFACE)

vmax = heat_data.max()
im = ax2.imshow(heat_data, cmap=cmap, vmin=0, vmax=vmax, aspect='auto')

ax2.set_xticks(np.arange(-0.5, len(COMPONENTS), 1), minor=True)
ax2.set_yticks(np.arange(-0.5, len(heat_rows), 1), minor=True)
ax2.grid(which='minor', color=SURFACE, linewidth=3)
ax2.tick_params(which='minor', length=0)

ax2.set_xticks(range(len(COMPONENTS)))
ax2.set_xticklabels(COMPONENT_LABELS, color=INK_SECONDARY, fontsize=10.5)
ax2.set_yticks(range(len(heat_rows)))
ax2.set_yticklabels(row_labels, color=INK_SECONDARY, fontsize=9.5)
ax2.tick_params(axis='both', length=0)
for spine in ax2.spines.values():
    spine.set_visible(False)

for i in range(len(heat_rows)):
    for j in range(len(COMPONENTS)):
        v = heat_data[i, j]
        frac = v / vmax if vmax > 0 else 0
        txt_color = '#ffffff' if frac > 0.55 else INK_PRIMARY
        ax2.text(j, i, f'{v:.3f}', ha='center', va='center', fontsize=9.5,
                  color=txt_color, fontweight='medium')

ax2.set_title('Dissimilarity-component sensitivity to targeted graph alterations',
              color=INK_PRIMARY, fontsize=12.5, fontweight='bold', pad=14, loc='left')
fig2.text(0.02, 0.955, 'delta from self-comparison baseline (logistics test graph, one alteration per row)',
           color=INK_MUTED, fontsize=9)

cbar = fig2.colorbar(im, ax=ax2, fraction=0.046, pad=0.03)
cbar.outline.set_visible(False)
cbar.ax.tick_params(labelsize=8.5, colors=INK_MUTED, length=0)
cbar.set_label('Δ dissimilarity component', color=INK_SECONDARY, fontsize=9.5)

plt.tight_layout(rect=[0, 0, 1, 0.96])
heatmap_path = f"{OUT_DIR}/dissimilarity_showcase.png"
plt.savefig(heatmap_path, dpi=150, facecolor=SURFACE, bbox_inches='tight')
plt.close()
print(f"Saved {heatmap_path}")
