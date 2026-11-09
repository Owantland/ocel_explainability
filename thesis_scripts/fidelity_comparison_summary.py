"""LOO vs. GNNExplainer characterization/sparsity summary -- the figure behind
thesis_structure.txt's headline fidelity numbers (Order Management 0.815 vs.
0.685; Logistics 0.396 vs. 0.244 characterization) that justify demoting
GNNExplainer to a comparison-only role, and gives the "sparsity" metric named in
§2.2.5/dataset.txt's Explanation-quality-metrics section a figure for the first time.

Renders characterization + sparsity only (2026-07-27) -- fidelity+/fidelity- are
still computed in load_summary()/the saved CSV (cheap, already-available context),
just no longer plotted, since the thesis discussion centers on characterization/
sparsity and the raw fidelity+/- hour values were judged not worth a dedicated panel.

Fidelity+/-/Characterization come from each dataset's paired comparison CSV
(files/explainer_outputs/{db}/fidelity_validation/fidelity_validation_paired.csv),
which already runs LOO and GNNExplainer on the same orders. Sparsity is NOT in that
paired file, so it's read separately per method from aggregate_metrics.csv (LOO;
keyed by a positional 'trace' index, not order_id) and
aggregate_gnnprimary_metrics.csv (GNNExplainer; keyed by order_id) -- both are
simple per-method means, not an order-level join, since the LOO file has no usable
order_id key to join against.

Usage: python3 fidelity_comparison_summary.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd

OUT_DIR = "thesis_parts/figures_tables"

DATASETS = [
    ("Order Management", "order_management"),
    ("Logistics", "logistics"),
]

METHOD_COLORS = {"LOO": "#4e79a7", "GNNExplainer": "#e15759"}


def load_summary():
    rows = []
    for label, database in DATASETS:
        paired = pd.read_csv(
            f"files/explainer_outputs/{database}/fidelity_validation/fidelity_validation_paired.csv")
        loo_agg = pd.read_csv(f"files/explainer_outputs/{database}/aggregate/aggregate_metrics.csv")
        # aggregate_metrics.csv appends a "mean"/"std" summary row after the per-trace
        # rows (explainer.py's explain_aggregate() CSV writer) -- exclude them here so
        # they aren't silently averaged in as if they were additional trace observations.
        loo_agg = loo_agg[pd.to_numeric(loo_agg['trace'], errors='coerce').notna()]
        gnn_agg = pd.read_csv(
            f"files/explainer_outputs/{database}/aggregate_gnnprimary/aggregate_gnnprimary_metrics.csv")

        rows.append({
            'dataset': label, 'method': 'LOO', 'n_paired': len(paired),
            'fidelity_plus': paired['loo_fidelity_plus'].mean(),
            'fidelity_minus': paired['loo_fidelity_minus'].mean(),
            'characterization': paired['loo_characterization'].mean(),
            'node_sparsity': loo_agg['node_sparsity'].mean(),
            'n_sparsity_source': len(loo_agg),
        })
        rows.append({
            'dataset': label, 'method': 'GNNExplainer', 'n_paired': len(paired),
            'fidelity_plus': paired['gnn_fidelity_plus'].mean(),
            'fidelity_minus': paired['gnn_fidelity_minus'].mean(),
            'characterization': paired['gnn_characterization'].mean(),
            'node_sparsity': gnn_agg['node_sparsity'].mean(),
            'n_sparsity_source': len(gnn_agg),
        })
    return pd.DataFrame(rows)


def render(df):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for col, (label, _) in enumerate(DATASETS):
        sub = df[df['dataset'] == label].set_index('method')

        ax = axes[col]
        x = range(2)
        width = 0.35
        ax.bar([i - width / 2 for i in x], sub['characterization'], width,
               label='Characterization', color='#59a14f')
        ax.bar([i + width / 2 for i in x], sub['node_sparsity'], width,
               label='Node Sparsity', color='#b07aa1')
        ax.set_xticks(x)
        ax.set_xticklabels(sub.index)
        ax.set_ylim(0, 1)
        ax.set_ylabel("Score (0-1)")
        ax.set_title(f"{label} — Characterization / Sparsity", fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, axis='y', alpha=0.3)

    fig.suptitle("LOO vs. GNNExplainer — characterization and sparsity, both datasets",
                 fontsize=11)
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    png_path = os.path.join(OUT_DIR, "fidelity_comparison_summary.png")
    plt.savefig(png_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {png_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = load_summary()
    csv_path = os.path.join(OUT_DIR, "fidelity_comparison_summary.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path} ({len(df)} rows)")
    print(df.to_string(index=False))

    render(df)


if __name__ == '__main__':
    main()
