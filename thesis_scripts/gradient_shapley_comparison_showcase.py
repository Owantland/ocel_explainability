"""Gradient-attribution vs. LOO-Shapley showcase, single trace (order 1801,
Order Management / Orders -> PayOrder -- the same trace used by every other
single-trace showcase this session). Produces TWO separate images:
  1. LOO-Shapley vs. InputXGradient, viewpoint node's own features
  2. LOO-Shapley vs. IntegratedGradients, viewpoint node's own features

Scoped to the VIEWPOINT node's own features only, not neighbor nodes: explain_trace_ig()
mean-pools attribution across every node of a given type in the graph, which for the
viewpoint (exactly 1 instance per graph) is a no-op -- an exact match to
explain_trace_shapley()'s seed_feature_importances (also viewpoint-only). For any
neighbor type with multiple instances, that same mean-pool would average across ALL
nodes of that type, which does NOT correspond to Shapley's single top-ranked neighbor --
comparing that would need new code indexing the raw per-node attribution mask, not
attempted here.

Units are NOT directly comparable in magnitude: Shapley values are signed hours (shift in
predicted remaining time), gradient attribution values are raw, unitless Captum output.
Each panel gets its own x-axis label/scale; the comparison is about relative feature
ranking and sign agreement, not magnitude equivalence -- stated directly in the figure
titles so it isn't misread.

Usage: python3 gradient_shapley_comparison_showcase.py
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
IG_SAVE_DIR = f"files/explainer_outputs/{DATABASE}/order_{ORDER_ID}_ig_shapley_compare"

METHOD_FILES = {
    'InputXGradient': os.path.join(OUT_DIR, "gradient_shapley_comparison_inputxgradient.png"),
    'IntegratedGradients': os.path.join(OUT_DIR, "gradient_shapley_comparison_integratedgradients.png"),
}


def _shapley_panel(ax, viewpoint, seed_feats, feature_names):
    """Left panel: LOO-Shapley seed_feature_importances, converted seconds->hours
    (matches plot_feature_importances()'s own conversion -- these tuples are raw
    seconds, not pre-converted). seed_feats' first tuple element is a FEATURE INDEX,
    not a name -- decode it via feature_names, exactly like plot_feature_importances()
    does internally (`names[f] if f < len(names) else f"feat_{f}"`)."""
    def _decode(f):
        return feature_names[f] if f < len(feature_names) else f"feat_{f}"

    rows = sorted(
        [(_decode(f), signed_shift / 3600.0) for f, shift, large, signed_shift in seed_feats],
        key=lambda r: r[1])
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]
    ax.barh(range(len(labels)), values, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel("Signed Shapley shift (hours)")
    ax.set_title("LOO-Shapley (production method)", fontsize=10)
    ax.grid(True, axis='x', alpha=0.3)
    return labels


def _gradient_panel(ax, method, signed_scores, feature_names, label_order):
    """Right panel: gradient attribution for the same viewpoint node, in the SAME
    feature order as the Shapley panel (label_order) for direct row-for-row
    alignment, even though the underlying units differ."""
    name_to_val = {feature_names[i]: signed_scores[i] for i in range(len(feature_names))}
    values = [name_to_val.get(lbl, 0.0) for lbl in label_order]
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]
    ax.barh(range(len(label_order)), values, color=colors)
    ax.set_yticks(range(len(label_order)))
    ax.set_yticklabels(label_order, fontsize=9)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel(f"{method} attribution (raw, unitless)")
    ax.set_title(f"{method}", fontsize=10)
    ax.grid(True, axis='x', alpha=0.3)


def main():
    e = exp.Explainer(DATABASE, CANT)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    shapley_result = e.explain_trace_shapley(ORDER_ID, top_k=5, n_samples=100)
    seed_feats = shapley_result['seed_feature_importances']
    viewpoint = e.kpi_viewpoint

    ig_result = e.explain_trace_ig(ORDER_ID, methods=('InputXGradient', 'IntegratedGradients'),
                                    save_dir=IG_SAVE_DIR)

    for method, png_path in METHOD_FILES.items():
        signed_scores = ig_result[method]['signed'][viewpoint]
        feature_names = e.feature_names.get(viewpoint, [])

        fig, axes = plt.subplots(1, 2, figsize=(13, max(3, len(seed_feats) * 0.5 + 1)))
        label_order = _shapley_panel(axes[0], viewpoint, seed_feats, feature_names)
        _gradient_panel(axes[1], method, signed_scores, feature_names, label_order)

        fig.suptitle(
            f"{KPI_LABEL} — order_id={ORDER_ID}, {viewpoint} node features\n"
            f"LOO-Shapley (hours) vs. {method} (raw attribution) — units differ, "
            f"compare ranking/sign only, not magnitude",
            fontsize=11)
        plt.tight_layout(rect=[0, 0, 1, 0.88])
        plt.savefig(png_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved {png_path}")


if __name__ == '__main__':
    main()
