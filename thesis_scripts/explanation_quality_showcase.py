"""Explainability-metrics showcase, single trace (order 1801, Order Management /
Orders -> PayOrder -- the live config, same trace used by every other single-trace
showcase this session).

Closes a gap: fidelity_comparison_summary.py already shows LOO vs. GNNExplainer
characterization/sparsity AGGREGATED across many traces per KPI, but no static
figure walks through what Fidelity+/Fidelity-/Characterization/Node sparsity/Edge
sparsity actually mean for ONE concrete trace -- thesis_parts/models.txt's
"Fidelity metrics" section describes exactly that as the stakeholder-facing view,
and dashboard.py already renders it live (characterization tile + raw JSON), but
never as a saved image.

explain_trace_shapley() already computes the 5-key 'metrics' dict internally via
evaluate_explanation_quality() (explainer.py:198-277, node_top_k=10/edge_top_k=15
hardcoded at that callsite) but doesn't return the two perturbed graphs it built to
get there. This script reconstructs those same two graphs (mirroring
explainer.py:203-244 exactly, same inputs) purely for visualization, and verifies
the reconstruction against the already-obtained metrics as a self-check.

Caveat baked into the figure: node feature zeroing is invisible in a structural
node-link plot (node color is by type, not feature values) -- only edge-set
differences are visually apparent between panels. Explanation nodes are
bold-bordered in all 3 panels (same visual language as the seed-node highlight
used throughout this session) so "what counts as the explanation" stays visible
regardless.

Usage: python3 explanation_quality_showcase.py
"""
import os
import random
import textwrap

import matplotlib.pyplot as plt
import torch

import explainer as exp

ORDER_ID = 1801
DATABASE = "order_management"
CANT = 2000
KPI_LABEL = "Order Management / Orders -> PayOrder"

OUT_DIR = "thesis_parts/figures_tables"
GRAPHS_PATH = os.path.join(OUT_DIR, "explanation_quality_showcase_graphs.png")
TABLE_PATH = os.path.join(OUT_DIR, "explanation_quality_showcase_metrics_table.png")

PALETTE = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
           "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"]

NODE_TOP_K = 10
EDGE_TOP_K = 15


def _build_explanation_sets(node_importances, edge_importances, viewpoint, object_idx):
    """Mirrors evaluate_explanation_quality's explanation-set construction exactly
    (explainer.py:203-210)."""
    explanation_nodes_by_type = {}
    for nt, i, _shift, _large, _signed in node_importances[:NODE_TOP_K]:
        explanation_nodes_by_type.setdefault(nt, set()).add(i)
    explanation_nodes_by_type.setdefault(viewpoint, set()).add(object_idx)

    explanation_edges_by_type = {}
    for et, e, _shift, _large, _signed in edge_importances[:EDGE_TOP_K]:
        explanation_edges_by_type.setdefault(et, set()).add(e)

    return explanation_nodes_by_type, explanation_edges_by_type


def _build_complement(graph, explanation_nodes_by_type, explanation_edges_by_type, device):
    """Fidelity+ graph: explanation removed, everything else kept. Mirrors
    explainer.py:212-224 exactly."""
    complement = graph.clone()
    for nt, idx_set in explanation_nodes_by_type.items():
        for i in idx_set:
            complement[nt].x[i] = 0.0
    for et, pos_set in explanation_edges_by_type.items():
        edge_index = complement[et].edge_index
        num_edges = edge_index.size(1)
        keep = torch.tensor(
            [pos not in pos_set for pos in range(num_edges)],
            dtype=torch.bool, device=device,
        )
        complement[et].edge_index = edge_index[:, keep]
    return complement


def _build_subgraph(graph, explanation_nodes_by_type, explanation_edges_by_type, device):
    """Fidelity- graph: only the explanation kept. Mirrors explainer.py:228-244
    exactly."""
    subgraph = graph.clone()
    for nt in subgraph.node_types:
        keep_idx = explanation_nodes_by_type.get(nt, set())
        n = subgraph[nt].x.size(0)
        for i in range(n):
            if i not in keep_idx:
                subgraph[nt].x[i] = 0.0
    for et in subgraph.edge_types:
        edge_index = subgraph[et].edge_index
        num_edges = edge_index.size(1)
        keep_pos = explanation_edges_by_type.get(et, set())
        keep = torch.tensor(
            [pos in keep_pos for pos in range(num_edges)],
            dtype=torch.bool, device=device,
        )
        subgraph[et].edge_index = edge_index[:, keep]
    return subgraph


def _draw_panel(e, graph, ax, type_colors, seed_key, explanation_keys, title):
    """Same drawing primitive as _draw_hetero_nx, but bold-borders EVERY node in
    explanation_keys (not just the single seed node) -- _draw_hetero_nx itself only
    supports one highlighted seed_key, so this is a local variant rather than a
    change to that shared production method."""
    import networkx as nx

    G = e._hetero_to_nx(graph)
    try:
        pos = nx.kamada_kawai_layout(G)
    except Exception:
        pos = nx.spring_layout(G, seed=42, k=0.9)

    node_colors = [type_colors.get(attrs['node_type'], 'gray') for _, attrs in G.nodes(data=True)]
    is_explained = [node in explanation_keys for node in G.nodes]
    node_sizes = [420 if exp_ else 180 for exp_ in is_explained]
    edgecolors = ['black' if exp_ else 'none' for exp_ in is_explained]
    linewidths = [2.2 if node == seed_key else (1.4 if exp_ else 0)
                  for node, exp_ in zip(G.nodes, is_explained)]

    nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colors, node_size=node_sizes,
                            edgecolors=edgecolors, linewidths=linewidths, alpha=0.9)
    nx.draw_networkx_edges(G, pos, ax=ax, edge_color="gray", width=1.0, alpha=0.5,
                            arrows=True, connectionstyle="arc3,rad=0.1")

    n_edges = sum(graph[et].edge_index.size(1) for et in graph.edge_types)
    ax.set_title(f"{title}\n({G.number_of_nodes()} nodes, {n_edges} edges)", fontsize=10)
    ax.axis("off")


def _render_metrics_table(metrics, save_path):
    rows = [
        ("Fidelity+", "|ŷ − Φ(p⁻ᵐ, X⁻ᵐ)|", f"{metrics['fidelity_plus']:.2f}h",
         "Higher is better (removing the explanation should shift the prediction)"),
        ("Fidelity−", "|ŷ − Φ(pᵐ, Xᵐ)|", f"{metrics['fidelity_minus']:.2f}h",
         "Closer to 0 is better (explanation alone should reproduce the prediction)"),
        ("Characterization", "Fidelity+ / (Fidelity+ + Fidelity−)",
         f"{metrics['characterization_score']:.3f}", "Higher is better, bounded [0, 1]"),
        ("Node sparsity", "1 − |m_N| / |N|", f"{metrics['node_sparsity']:.1%}",
         "Share of the graph excluded from the explanation"),
        ("Edge sparsity", "1 − |m_A| / |A|", f"{metrics['edge_sparsity']:.1%}",
         "Share of edges excluded from the explanation"),
    ]
    col_labels = ["Metric", "Formula", "Value", "Interpretation"]
    wrapped_rows = [
        [metric, formula, value, "\n".join(textwrap.wrap(interp, width=42))]
        for metric, formula, value, interp in rows
    ]
    n_lines = [max(cell.count("\n") for cell in row) + 1 for row in wrapped_rows]
    # Header counts as 1 line; each row's fractional height is lines/total so the
    # rows sum to exactly the axes height (1.0) -- avoids the double-scaling
    # overflow of combining explicit set_height() with table.scale().
    total_lines = sum(n_lines) + 1
    header_height = 1.0 / total_lines

    fig, ax = plt.subplots(figsize=(13, total_lines * 0.45 + 1.2))
    ax.axis('off')
    ax.set_title(f"Explanation-quality metrics — {KPI_LABEL}, order_id={ORDER_ID}", fontsize=12)
    table = ax.table(cellText=wrapped_rows, colLabels=col_labels, loc='center', cellLoc='left',
                      colWidths=[0.14, 0.26, 0.09, 0.51])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    for col in range(len(col_labels)):
        table[0, col].set_height(header_height)
    for r_idx, lines in enumerate(n_lines, start=1):
        for col in range(len(col_labels)):
            table[r_idx, col].set_height(lines / total_lines)
    for col in range(len(col_labels)):
        table[0, col].set_facecolor("#EAEAEA")
        table[0, col].set_text_props(weight='bold')
    for r_idx in range(1, len(rows) + 1):
        if r_idx % 2 == 0:
            for col in range(len(col_labels)):
                table[r_idx, col].set_facecolor("#F5F5F5")
        table[r_idx, 0].set_text_props(weight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {save_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    e = exp.Explainer(DATABASE, CANT)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    # _permutation_shapley (explainer.py) draws its permutation orderings from the
    # global `random` module with no seed, so the cited characterization score
    # drifted ~9% across reruns -- seed it here so this specific figure/number is
    # reproducible without changing the unseeded default any other caller relies on.
    random.seed(42)
    result = e.explain_trace_shapley(ORDER_ID, top_k=5, n_samples=100)
    metrics = result['metrics']
    node_importances = result['node_importances']
    edge_importances = result['edge_importances']

    graph = e._locate_test_graph(ORDER_ID, None)
    viewpoint = e.kpi_viewpoint
    object_idx = 0
    baseline_value = e._predict_value_for_graph(graph, object_idx)

    explanation_nodes_by_type, explanation_edges_by_type = _build_explanation_sets(
        node_importances, edge_importances, viewpoint, object_idx)
    complement = _build_complement(graph, explanation_nodes_by_type, explanation_edges_by_type, e.device)
    subgraph = _build_subgraph(graph, explanation_nodes_by_type, explanation_edges_by_type, e.device)

    pred_complement = e._predict_value_for_graph(graph, object_idx, perturbed_graph=complement)
    pred_subgraph = e._predict_value_for_graph(graph, object_idx, perturbed_graph=subgraph)
    recomputed_fid_plus = abs(baseline_value - pred_complement) / 3600.0
    recomputed_fid_minus = abs(baseline_value - pred_subgraph) / 3600.0

    print(f"Self-check: fidelity_plus  reconstructed={recomputed_fid_plus:.6f}h  "
          f"vs. metrics={metrics['fidelity_plus']:.6f}h")
    print(f"Self-check: fidelity_minus reconstructed={recomputed_fid_minus:.6f}h  "
          f"vs. metrics={metrics['fidelity_minus']:.6f}h")
    assert abs(recomputed_fid_plus - metrics['fidelity_plus']) < 1e-6, "Fidelity+ mismatch!"
    assert abs(recomputed_fid_minus - metrics['fidelity_minus']) < 1e-6, "Fidelity- mismatch!"
    print("Self-check PASSED: reconstructed graphs exactly reproduce the already-computed metrics.")

    explanation_keys = {(nt, i) for nt, idx_set in explanation_nodes_by_type.items() for i in idx_set}
    seed_key = (viewpoint, object_idx)
    node_types = sorted(graph.node_types)
    type_colors = {nt: PALETTE[i % len(PALETTE)] for i, nt in enumerate(node_types)}

    fig, axes = plt.subplots(1, 3, figsize=(19, 7))
    _draw_panel(e, graph, axes[0], type_colors, seed_key, explanation_keys,
                f"Baseline (full graph)\npred={baseline_value / 3600.0:.1f}h")
    _draw_panel(e, complement, axes[1], type_colors, seed_key, explanation_keys,
                f"Fidelity+: explanation removed\npred={pred_complement / 3600.0:.1f}h "
                f"(shift={recomputed_fid_plus:.1f}h)")
    _draw_panel(e, subgraph, axes[2], type_colors, seed_key, explanation_keys,
                f"Fidelity−: explanation only\npred={pred_subgraph / 3600.0:.1f}h "
                f"(shift={recomputed_fid_minus:.1f}h)")

    handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=9,
                          label=nt) for nt, c in type_colors.items()]
    handles.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='white',
                              markeredgecolor='black', markeredgewidth=1.8, markersize=9,
                              label="Explanation node (bold border)"))
    fig.legend(handles=handles, loc='lower center', ncol=len(handles), fontsize=8,
               bbox_to_anchor=(0.5, -0.02))

    fig.suptitle(
        f"{KPI_LABEL} — order_id={ORDER_ID}: graphs behind the fidelity metrics\n"
        f"Node feature zeroing is invisible in a structural plot (node color is by type, "
        f"not feature values) — only edge-set differences and the bold explanation-node "
        f"border are visually apparent between panels",
        fontsize=11)
    plt.tight_layout(rect=[0, 0.04, 1, 0.88])
    plt.savefig(GRAPHS_PATH, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {GRAPHS_PATH}")

    _render_metrics_table(metrics, TABLE_PATH)


if __name__ == '__main__':
    main()
