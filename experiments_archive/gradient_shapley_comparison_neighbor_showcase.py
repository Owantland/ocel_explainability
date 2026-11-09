"""Gradient-attribution vs. LOO-Shapley showcase for a NEIGHBOR node type -- the
companion to gradient_shapley_comparison_showcase.py (which covers the viewpoint
node's own features). Same trace (order 1801, Order Management / Orders -> PayOrder).

explain_trace_ig()'s mean-pooling averages a neighbor type's attribution across EVERY
node of that type in the graph, which does not correspond to Shapley's single
top-ranked neighbor instance (top_neighbor_feature_importances). To get a genuinely
comparable single-node gradient attribution, this script bypasses explain_trace_ig()
and calls _compute_attribution_for_graph() directly -- it returns PER-NODE (not
pooled) arrays, {node_type: [N_nodes_of_type, F_features]} -- then indexes the exact
row for the same node instance Shapley identified as the top neighbor
(node_importances[0]'s (node_type, node_idx)).

Usage: python3 gradient_shapley_comparison_neighbor_showcase.py
"""
import os

import matplotlib.pyplot as plt
import torch

import explainer as exp

ORDER_ID = 1801
DATABASE = "order_management"
CANT = 2000
KPI_LABEL = "Order Management / Orders -> PayOrder"

OUT_DIR = "thesis_parts/figures_tables"

METHOD_FILES = {
    'InputXGradient': os.path.join(OUT_DIR, "gradient_shapley_comparison_neighbor_inputxgradient.png"),
    'IntegratedGradients': os.path.join(OUT_DIR, "gradient_shapley_comparison_neighbor_integratedgradients.png"),
}


def _decode(feature_names, f):
    return feature_names[f] if f < len(feature_names) else f"feat_{f}"


def _shapley_panel(ax, top_feats, feature_names, node_type):
    rows = sorted(
        [(_decode(feature_names, f), signed_shift / 3600.0) for f, shift, large, signed_shift in top_feats],
        key=lambda r: r[1])
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]
    ax.barh(range(len(labels)), values, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel("Signed Shapley shift (hours)")
    ax.set_title(f"LOO-Shapley (production method)\ntop neighbor: {node_type} node", fontsize=10)
    ax.grid(True, axis='x', alpha=0.3)
    return labels


def _gradient_panel(ax, method, node_row, feature_names, label_order):
    name_to_val = {feature_names[i]: node_row[i] for i in range(len(feature_names))}
    values = [name_to_val.get(lbl, 0.0) for lbl in label_order]
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]
    ax.barh(range(len(label_order)), values, color=colors)
    ax.set_yticks(range(len(label_order)))
    ax.set_yticklabels(label_order, fontsize=9)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel(f"{method} attribution (raw, unitless)")
    ax.set_title(f"{method}\n(same single node instance, not type-pooled)", fontsize=10)
    ax.grid(True, axis='x', alpha=0.3)


def main():
    e = exp.Explainer(DATABASE, CANT)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    shapley_result = e.explain_trace_shapley(ORDER_ID, top_k=5, n_samples=100)
    top_feats = shapley_result['top_neighbor_feature_importances']
    if not top_feats:
        raise SystemExit("top_neighbor_feature_importances is empty for this trace/top_k -- "
                          "no single top neighbor node was identified.")
    top_node_type, top_node_idx = shapley_result['node_importances'][0][0], shapley_result['node_importances'][0][1]
    print(f"Top neighbor node identified by LOO: {top_node_type}#{top_node_idx}")

    explain_subgraph = e._locate_test_graph(ORDER_ID, None)
    feature_names = e.feature_names.get(top_node_type, [])

    for method, png_path in METHOD_FILES.items():
        masks = e._compute_attribution_for_graph(explain_subgraph, method=method)
        node_row = masks[top_node_type][top_node_idx]

        fig, axes = plt.subplots(1, 2, figsize=(13, max(3, len(top_feats) * 0.5 + 1)))
        label_order = _shapley_panel(axes[0], top_feats, feature_names, top_node_type)
        _gradient_panel(axes[1], method, node_row, feature_names, label_order)

        fig.suptitle(
            f"{KPI_LABEL} — order_id={ORDER_ID}, {top_node_type}#{top_node_idx} node features "
            f"(top LOO-identified neighbor)\n"
            f"LOO-Shapley (hours) vs. {method} (raw attribution) — units differ, "
            f"compare ranking/sign only, not magnitude",
            fontsize=11)
        plt.tight_layout(rect=[0, 0, 1, 0.88])
        plt.savefig(png_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved {png_path}")


if __name__ == '__main__':
    main()
