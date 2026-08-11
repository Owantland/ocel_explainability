"""Model comparison summary (HGT vs. HomoGNN vs. Mean predictor vs. GBT), both
datasets, with 95% bootstrap CIs and paired significance tests -- the figure behind
thesis_structure.txt's Results/Conclusion MAE claims (e.g. "97.99h vs. 103.8h, ~5.6%
relative" on Order Management; Logistics numbers superseded, see below).

Order Management reads the plain-named model_comparison.csv / baseline_significance.csv
in files/explainer_outputs/order_management/validation_2000/ -- both written together by
explainer.py's compare_to_baselines() whenever config.yml's order_management kpi_event is
PayOrder (this repo's resting default), so the generic filename safely represents PayOrder.

Logistics does NOT use the generic filename -- FIXED 2026-08-07 after discovering it was
silently wrong: logistics has two live KPIs (Depart/LoadToVehicle) sharing config.yml's
kpi_event, so compare_to_baselines() overwrites the SAME generic model_comparison.csv
regardless of which KPI last ran (confirmed directly: it held LoadToVehicle's numbers
after refreshing both KPIs' figures the same session, not Depart's, purely because
LoadToVehicle happened to run last). Reads the explicit KPI-suffixed
model_comparison_Depart.csv/baseline_significance_Depart.csv instead -- same convention
already used by combined_baseline_comparison.py -- so this script can't silently flip
between KPIs again depending on run order.

Usage: python3 model_comparison_summary.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd

OUT_DIR = "thesis_parts/figures_tables"

# (dataset label, database, validation subdir, filename suffix or None for the generic file)
DATASETS = [
    ("Order Management", "order_management", "validation_2000", None),
    ("Logistics", "logistics", "validation_1000", "Depart"),
]

MODEL_ORDER = ["HGT (ours)", "HomoGNN (GCN)", "k-dim GNN (HOEG)", "Mean predictor", "GBT"]
MODEL_COLORS = {
    "HGT (ours)": "#4e79a7",
    "HomoGNN (GCN)": "#59a14f",
    "k-dim GNN (HOEG)": "#f28e2b",
    "Mean predictor": "#bab0ac",
    "GBT": "#e15759",
}


def _filename(base, suffix):
    return f"{base}_{suffix}.csv" if suffix else f"{base}.csv"


def load_comparisons():
    frames = []
    for label, database, subdir, suffix in DATASETS:
        path = f"files/explainer_outputs/{database}/{subdir}/{_filename('model_comparison', suffix)}"
        df = pd.read_csv(path)
        df.insert(0, 'dataset', label)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def load_significance():
    frames = []
    for label, database, subdir, suffix in DATASETS:
        path = f"files/explainer_outputs/{database}/{subdir}/{_filename('baseline_significance', suffix)}"
        df = pd.read_csv(path)
        df.insert(0, 'dataset', label)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def render(df):
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    for col, (label, _, _, _) in enumerate(DATASETS):
        sub = df[df['dataset'] == label].set_index('Model').reindex(MODEL_ORDER)
        for row, (metric, ci_low, ci_high, title) in enumerate([
            ('MAE_all', 'MAE_all_ci_low', 'MAE_all_ci_high', f"{label} — MAE (all prefixes)"),
            ('MAE_last', 'MAE_last_ci_low', 'MAE_last_ci_high', f"{label} — MAE (last event)"),
        ]):
            ax = axes[row, col]
            yerr_low = sub[metric] - sub[ci_low]
            yerr_high = sub[ci_high] - sub[metric]
            colors = [MODEL_COLORS[m] for m in sub.index]
            ax.bar(sub.index, sub[metric], yerr=[yerr_low, yerr_high],
                   color=colors, capsize=4, edgecolor='black', linewidth=0.5)
            ax.set_title(title, fontsize=11)
            ax.set_ylabel("MAE (hours)")
            ax.tick_params(axis='x', rotation=20)
            ax.grid(True, axis='y', alpha=0.3)

    fig.suptitle("Model comparison across baselines, both datasets\n"
                  "Error bars: 95% bootstrap CI. Source: explainer.py's compare_to_baselines(), "
                  "most recent run (2026-08-07).", fontsize=11)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    png_path = os.path.join(OUT_DIR, "model_comparison_summary.png")
    plt.savefig(png_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {png_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = load_comparisons()
    csv_path = os.path.join(OUT_DIR, "model_comparison_summary.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path} ({len(df)} rows)")

    sig_df = load_significance()
    sig_csv_path = os.path.join(OUT_DIR, "baseline_significance_summary.csv")
    sig_df.to_csv(sig_csv_path, index=False)
    print(f"Saved {sig_csv_path} ({len(sig_df)} rows)")

    render(df)


if __name__ == '__main__':
    main()
