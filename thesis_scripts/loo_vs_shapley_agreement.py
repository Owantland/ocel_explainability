"""LOO vs. Shapley agreement, both datasets, including a pooled before/after view of
the BringToLoadingBay redundancy correction that motivated switching from LOO to
Shapley for quantification (thesis_structure.txt's headline Results finding).

Reads all 10 per-order comparison CSVs already produced by explainer.py's
compare_loo_vs_shapley() (5 orders/dataset), each with node_label/loo_signed_shift/
shapley_value/loo_rank/shapley_rank rows for that order's top candidate nodes. No
new computation -- pure aggregation of already-saved per-order tables.

Usage: python3 loo_vs_shapley_agreement.py
"""
import glob
import os

import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import spearmanr

OUT_DIR = "thesis_parts/figures_tables"

DATASETS = [
    ("Order Management", "order_management"),
    ("Logistics", "logistics"),
]

REDUNDANCY_LABEL = "Events=BringToLoadingBay"


def load_all():
    frames = []
    for label, database in DATASETS:
        paths = sorted(glob.glob(f"files/explainer_outputs/{database}/shapley/loo_vs_shapley_*.csv"))
        for path in paths:
            order_id = os.path.basename(path).replace("loo_vs_shapley_", "").replace(".csv", "")
            df = pd.read_csv(path)
            df.insert(0, 'order_id', order_id)
            df.insert(0, 'dataset', label)
            frames.append(df)
    return pd.concat(frames, ignore_index=True)


def render(df):
    has_redundancy = df['node_label'].eq(REDUNDANCY_LABEL).any()
    ncols = 3 if has_redundancy else 2
    fig, axes = plt.subplots(1, ncols, figsize=(6 * ncols, 5.5))

    for i, (label, _) in enumerate(DATASETS):
        ax = axes[i]
        sub = df[df['dataset'] == label]
        rho, p = spearmanr(sub['loo_signed_shift'], sub['shapley_value'])
        ax.scatter(sub['loo_signed_shift'], sub['shapley_value'], alpha=0.7,
                   edgecolor='black', linewidth=0.4, color='#4e79a7')
        ax.axhline(0, color='gray', linewidth=0.6)
        ax.axvline(0, color='gray', linewidth=0.6)
        ax.set_xlabel("LOO signed shift (hours)")
        ax.set_ylabel("Shapley value (hours)")
        ax.set_title(f"{label}\nSpearman's ρ={rho:.2f} (p={p:.1e}), n={len(sub)}", fontsize=10)
        ax.grid(True, alpha=0.3)

    if has_redundancy:
        ax = axes[2]
        red = df[df['node_label'] == REDUNDANCY_LABEL]
        pooled = red.groupby('dataset')[['loo_signed_shift', 'shapley_value']].mean()
        counts = red.groupby('dataset').size()
        x = range(len(pooled))
        width = 0.35
        ax.bar([i - width / 2 for i in x], pooled['loo_signed_shift'], width,
               label='LOO (pooled mean)', color='#4e79a7')
        ax.bar([i + width / 2 for i in x], pooled['shapley_value'], width,
               label='Shapley (pooled mean)', color='#e15759')
        ax.axhline(0, color='gray', linewidth=0.6)
        ax.set_xticks(x)
        ax.set_xticklabels([f"{d}\n(n={counts[d]} instances)" for d in pooled.index])
        ax.set_ylabel("Pooled mean shift (hours)")
        ax.set_title(f"'{REDUNDANCY_LABEL}'\nredundancy correction", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, axis='y', alpha=0.3)

    fig.suptitle("LOO vs. Shapley agreement across sampled orders, both datasets", fontsize=12)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    png_path = os.path.join(OUT_DIR, "loo_vs_shapley_agreement.png")
    plt.savefig(png_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {png_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = load_all()
    csv_path = os.path.join(OUT_DIR, "loo_vs_shapley_agreement.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path} ({len(df)} rows)")

    for label, _ in DATASETS:
        sub = df[df['dataset'] == label]
        rho, p = spearmanr(sub['loo_signed_shift'], sub['shapley_value'])
        print(f"{label}: Spearman's rho={rho:.3f} (p={p:.2e}), n={len(sub)}")

    if df['node_label'].eq(REDUNDANCY_LABEL).any():
        pooled = df[df['node_label'] == REDUNDANCY_LABEL].groupby('dataset')[
            ['loo_signed_shift', 'shapley_value']].agg(['mean', 'count'])
        print(f"\n'{REDUNDANCY_LABEL}' pooled comparison:\n{pooled}")

    render(df)


if __name__ == '__main__':
    main()
