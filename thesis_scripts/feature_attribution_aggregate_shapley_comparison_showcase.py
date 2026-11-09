"""Aggregate feature-attribution vs. LOO-Shapley showcase, single KPI (Order
Management / Orders -> PayOrder -- the live config, chosen since both source files
below are already fresh for it, no rerun needed).

The aggregate analog of gradient_shapley_comparison_showcase.py's single-trace
comparison. Pure CSV -> figure post-processing, reusing two already-computed
outputs at matching (node_type, feature_name) granularity:
  - explain_aggregate_shapley()'s aggregate_feature_bars.csv (label format
    "{node_type}.{feature_name}", mean_signed_shift in hours) -- the top ~20
    feature-level labels, Shapley-requantified.
  - explain_feature_attribution()'s ig_attribution_{method}.csv (node_type,
    feature_dim, feature_name, mean_signed, mean_abs -- raw, unitless).

Units are NOT comparable in magnitude (signed hours vs. raw gradient output) --
same disclaimer as the single-trace comparison; this is about ranking/sign
agreement, not equivalence.

Usage: python3 feature_attribution_aggregate_shapley_comparison_showcase.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd

KPI_LABEL = "Order Management / Orders -> PayOrder"
DATABASE = "order_management"
TASK_ID = "TimeFrom_Orders_to_PayOrder"

SHAPLEY_CSV = f"files/explainer_outputs/{DATABASE}/aggregate_shapley_{TASK_ID}/aggregate_feature_bars.csv"
ATTRIBUTION_DIR = f"files/explainer_outputs/{DATABASE}/kpi_{TASK_ID}/attribution"

OUT_DIR = "thesis_parts/figures_tables"
METHOD_FILES = {
    'InputXGradient': os.path.join(OUT_DIR, "feature_attribution_aggregate_shapley_comparison_inputxgradient.png"),
    'IntegratedGradients': os.path.join(OUT_DIR, "feature_attribution_aggregate_shapley_comparison_integratedgradients.png"),
}

TOP_N = 8
# explain_aggregate_shapley()'s LOO-identification pool -- NOT the CSV's 'count'
# column, which is only the per-label Shapley-revisit count (<=revisit_n=3) and
# would misleadingly read as "n=3 traces" in the title.
N_TRACES = 50


def _shapley_panel(ax, top_rows):
    rows = sorted(
        [(r.label, r.mean_signed_shift) for r in top_rows.itertuples()],
        key=lambda r: r[1])
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]
    ax.barh(range(len(labels)), values, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel("Mean signed Shapley shift (hours)")
    ax.set_title("LOO-Shapley aggregate (production method)", fontsize=10)
    ax.grid(True, axis='x', alpha=0.3)
    return labels


def _gradient_panel(ax, method, grad_lookup, label_order):
    values = []
    for lbl in label_order:
        node_type, feature_name = lbl.split('.', 1)
        values.append(grad_lookup.get((node_type, feature_name), 0.0))
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]
    ax.barh(range(len(label_order)), values, color=colors)
    ax.set_yticks(range(len(label_order)))
    ax.set_yticklabels(label_order, fontsize=9)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel(f"Mean {method} attribution (raw, unitless)")
    ax.set_title(f"{method} aggregate", fontsize=10)
    ax.grid(True, axis='x', alpha=0.3)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    shapley_df = pd.read_csv(SHAPLEY_CSV)
    shapley_df['abs_rank'] = shapley_df['mean_signed_shift'].abs()
    top_rows = shapley_df.sort_values('abs_rank', ascending=False).head(TOP_N)

    for method, png_path in METHOD_FILES.items():
        grad_csv = os.path.join(ATTRIBUTION_DIR, f"ig_attribution_{method.lower()}.csv")
        grad_df = pd.read_csv(grad_csv)
        grad_lookup = {(r.node_type, r.feature_name): r.mean_signed for r in grad_df.itertuples()}

        fig, axes = plt.subplots(1, 2, figsize=(14, max(3, TOP_N * 0.5 + 1)))
        label_order = _shapley_panel(axes[0], top_rows)
        _gradient_panel(axes[1], method, grad_lookup, label_order)

        fig.suptitle(
            f"{KPI_LABEL} — aggregate feature attribution vs. LOO-Shapley (n={N_TRACES} traces)\n"
            f"LOO-Shapley (hours) vs. {method} (raw attribution) — units differ, "
            f"compare ranking/sign only, not magnitude",
            fontsize=11)
        plt.tight_layout(rect=[0, 0, 1, 0.88])
        plt.savefig(png_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved {png_path}")


if __name__ == '__main__':
    main()
