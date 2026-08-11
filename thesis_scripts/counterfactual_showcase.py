"""Counterfactual explanation showcase, single trace (order 1801, Order Management /
Orders -> PayOrder -- config is already live, no switching needed).

explain_counterfactual() already saves 5 PNGs + 1 CSV, but bundles the query and best-CF
graphs into ONE combined side-by-side image (cf_graph_structure_comparison.png) rather
than two separate files, and never renders the full ranked candidate list as a table
(only as a CSV). This script:
  1. Calls explain_counterfactual() once -- gets the standard 5 artifacts AND the full
     ranked `results` list (up to n_results candidates, each with its own graph).
  2. Splits the query/CF graphs into two SEPARATE images by calling the same underlying
     drawing primitives the method already uses (_hetero_to_nx + _draw_hetero_nx, which
     draws onto a given axis) directly on two fresh single-axis figures -- not a crop of
     the combined image.
  3. Copies the method's other 4 already-separate artifacts into thesis_parts/figures_tables/
     with showcase-prefixed names -- no new computation needed for these.
  4. Renders cf_dissimilarity.csv's full ranked candidate list as a table image (this
     doesn't exist yet -- only the CSV does).

Usage: python3 counterfactual_showcase.py
"""
import os
import shutil

import matplotlib.pyplot as plt
import pandas as pd
import torch

import explainer as exp

ORDER_ID = 1801
DATABASE = "order_management"
CANT = 2000
KPI_LABEL = "Order Management / Orders -> PayOrder"

OUT_DIR = "thesis_parts/figures_tables"

PALETTE = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
           "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"]


def _draw_single_graph(e, graph, order_id, hours, seed_key, id_map, title, save_path):
    G = e._hetero_to_nx(graph)
    node_types = sorted({a['node_type'] for _, a in G.nodes(data=True)})
    type_colors = {nt: PALETTE[i % len(PALETTE)] for i, nt in enumerate(node_types)}

    fig, ax = plt.subplots(figsize=(9, 8))
    e._draw_hetero_nx(G, ax, type_colors, seed_key=seed_key, title=title, id_map=id_map)

    handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=9,
                          label=nt) for nt, c in type_colors.items()]
    ax.legend(handles=handles, loc='upper right', fontsize=8, title="Node type")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {save_path}")


def _render_candidates_table(results, save_path):
    col_labels = ["Rank", "Order ID", "Predicted (h)", "feat", "type", "edge", "struct", "Total"]
    cell_rows = []
    for i, r in enumerate(results, 1):
        c = r['components']
        cell_rows.append([
            str(i), str(r['order_id']), f"{r['predicted_hours']:.1f}",
            f"{c['feat']:.3f}", f"{c['type']:.3f}", f"{c['edge']:.3f}", f"{c['struct']:.3f}",
            f"{r['dissimilarity']:.3f}",
        ])

    fig, ax = plt.subplots(figsize=(9, 0.5 * len(cell_rows) + 1.2))
    ax.axis('off')
    ax.set_title(f"Closest counterfactual candidates — {KPI_LABEL}, order_id={ORDER_ID}",
                 fontsize=12)
    table = ax.table(cellText=cell_rows, colLabels=col_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.8)
    table.auto_set_column_width(col=list(range(len(col_labels))))
    for col in range(len(col_labels)):
        table[0, col].set_facecolor("#EAEAEA")
        table[0, col].set_text_props(weight='bold')
    for r_idx in range(1, len(cell_rows) + 1):
        if r_idx % 2 == 0:
            for col in range(len(col_labels)):
                table[r_idx, col].set_facecolor("#F5F5F5")
    if len(cell_rows) > 0:
        for col in range(len(col_labels)):
            table[1, col].set_facecolor("#DCE6F1")
            table[1, col].set_text_props(weight='bold')

    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {save_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    e = exp.Explainer(DATABASE, CANT)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    results = e.explain_counterfactual(ORDER_ID, n_results=3)
    if not results:
        raise SystemExit("No counterfactuals found for this order/config -- nothing to showcase.")

    cf_dir = os.path.join(e.path_dict['explainer_path'], f"order_{ORDER_ID}_cf")

    # 1/2: query graph and best-CF graph as two SEPARATE images (same drawing primitives
    # explain_counterfactual() already uses internally, just one graph per figure).
    query_graph = e._locate_test_graph(ORDER_ID, None)
    query_pred = e._predict_value_for_graph(query_graph, 0) / 3600.0
    id_map_q = e._decode_all_identifiers(query_graph, ORDER_ID, None)
    seed_key = (e.kpi_viewpoint, 0)

    best = results[0]
    id_map_cf = e._decode_all_identifiers(best['graph'], best['order_id'], best['n_events'])

    _draw_single_graph(
        e, query_graph, ORDER_ID, query_pred, seed_key, id_map_q,
        f"Query — {KPI_LABEL}, order_id={ORDER_ID} (pred={query_pred:.1f}h)",
        os.path.join(OUT_DIR, "cf_showcase_query_graph.png"))

    _draw_single_graph(
        e, best['graph'], best['order_id'], best['predicted_hours'], seed_key, id_map_cf,
        f"Identified counterfactual — order_id={best['order_id']} "
        f"(pred={best['predicted_hours']:.1f}h, dissimilarity={best['dissimilarity']:.3f})",
        os.path.join(OUT_DIR, "cf_showcase_counterfactual_graph.png"))

    # 3: copy the method's other 4 already-separate artifacts, showcase-prefixed.
    copy_map = {
        "cf_node_type_comparison.png": "cf_showcase_node_type_comparison.png",
        "cf_dissimilarity_breakdown.png": "cf_showcase_dissimilarity_breakdown.png",
        "cf_event_type_diff.png": "cf_showcase_event_type_diff.png",
        f"cf_{e.kpi_viewpoint.lower()}_feature_diff.png": "cf_showcase_viewpoint_feature_diff.png",
    }
    for src_name, dst_name in copy_map.items():
        src = os.path.join(cf_dir, src_name)
        dst = os.path.join(OUT_DIR, dst_name)
        if os.path.exists(src):
            shutil.copyfile(src, dst)
            print(f"Copied {src} -> {dst}")
        else:
            print(f"WARNING: expected artifact not found: {src}")

    # 4: render the full ranked candidate table (cf_dissimilarity.csv has the data, but
    # no rendered table image exists yet).
    _render_candidates_table(results, os.path.join(OUT_DIR, "cf_showcase_candidates_table.png"))


if __name__ == '__main__':
    main()
