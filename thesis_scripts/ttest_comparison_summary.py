"""HOEG Fig. 3 (a/c/e)-style violin + paired-significance plots, HGT vs. HomoGNN.

HOEG's own Fig. 3 has six panels: three violin plots of MAE distribution across
n=16 hyperparameter settings, split by Train/Val/Test, comparing EFG vs. HOEG
encodings and annotated with paired t-test results as text (a/c/e); and three
learning-curve panels (b/d/f, already reproduced separately by
training_curves_summary.py). This script reproduces the violin/statistical half
only, for this project's own two datasets and its own two models (HGT vs.
HomoGNN).

Two deliberate deviations from HOEG's original, both forced by what's actually
available on disk:
  1. Axis substitution -- HOEG's violin distributes MAE *across hyperparameter
     settings*. This project only has a persisted Optuna sweep study for the
     Hetero (HGT) model (files/models/{db}/{cant}/Hetero/sweep_{task_id}.db);
     HomoGNN (training.py's Homo_Reg_Modelling) trains with a single fixed
     hyperparameter set, so there is no matching per-setting distribution to
     build for it. Instead, this violin distributes *per-test-example absolute
     error* -- the same substitution training_curves_summary.py already made
     for the learning-curve half.
  2. Single split, not three -- HOEG splits by Train/Val/Test. The paired
     significance test this project has (explainer.py's compare_to_baselines())
     only exists for the test set's last-event subset (the one slice where
     order_id is a safe unique join key -- the full "all prefixes" set has
     multiple rows per order_id). So each dataset gets one violin pair, not
     three.

Recomputes the paired per-example absolute errors and both test statistics
(paired t-test + Wilcoxon) directly via baselines.hgt_predictions/
homo_predictions -- the same functions and join logic (filter to last_event,
merge on order_id) explainer.py's compare_to_baselines() already uses --
because the persisted baseline_significance.csv only stores p-values, not the
raw paired arrays or the t-statistic itself. No training or sweeping here;
both are pure inference over already-trained checkpoints.

Usage: python3 ttest_comparison_summary.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd
from scipy import stats

import baselines as bl
import training as t

# (dataset display label, database, cant)
DATASETS = [
    ("Order Management", "order_management", 2000),
    ("Logistics", "logistics", 1000),
]

OUT_DIR = "thesis_parts/figures_tables"
CSV_PATH = os.path.join(OUT_DIR, "ttest_comparison_summary.csv")
PNG_PATH = os.path.join(OUT_DIR, "ttest_comparison_summary.png")

MODEL_COLORS = {"HGT (ours)": "#4e79a7", "HomoGNN (GCN)": "#e15759"}


def paired_last_event_ae(database, cant):
    """Returns a DataFrame with one row per last-event test order: order_id,
    hgt_ae, homo_ae -- mirroring explainer.py's compare_to_baselines()
    (lines ~3089-3093) exactly: filter each model's predictions to last_event,
    then merge on order_id (the only slice where it's a safe unique key)."""
    m = t.Modelling(database, cant)

    hgt_df, _ = bl.hgt_predictions(m)
    hgt_last = hgt_df[hgt_df['last_event']][['order_id', 'true_h', 'hgt_pred_h']].copy()
    hgt_last['hgt_ae'] = (hgt_last['true_h'] - hgt_last['hgt_pred_h']).abs()

    homo_df, _ = bl.homo_predictions(m)
    homo_last = homo_df[homo_df['last_event']][['order_id', 'true_h', 'homo_pred_h']].copy()
    homo_last['homo_ae'] = (homo_last['true_h'] - homo_last['homo_pred_h']).abs()

    paired = hgt_last[['order_id', 'hgt_ae']].merge(
        homo_last[['order_id', 'homo_ae']], on='order_id', how='inner')
    return paired


def render(df):
    fig, axes = plt.subplots(1, len(DATASETS), figsize=(6.5 * len(DATASETS), 5.5))
    if len(DATASETS) == 1:
        axes = [axes]

    for ax, (label, _, _) in zip(axes, DATASETS):
        sub = df[df['dataset'] == label]
        hgt_ae, homo_ae = sub['hgt_ae'].values, sub['homo_ae'].values

        parts = ax.violinplot([hgt_ae, homo_ae], showmeans=True, showextrema=True)
        for body, model_name in zip(parts['bodies'], ["HGT (ours)", "HomoGNN (GCN)"]):
            body.set_facecolor(MODEL_COLORS[model_name])
            body.set_alpha(0.7)
        ax.set_xticks([1, 2])
        ax.set_xticklabels(["HGT (ours)", "HomoGNN (GCN)"])
        ax.set_ylabel("Absolute Error (hours, last-event)")

        t_stat, t_p = stats.ttest_rel(hgt_ae, homo_ae)
        w_stat, w_p = stats.wilcoxon(hgt_ae, homo_ae)
        p_fmt = lambda p: "p<.001" if p < 0.001 else f"p={p:.3f}"
        annotation = (f"paired t-test: t={t_stat:.2f}, {p_fmt(t_p)}\n"
                      f"Wilcoxon: W={w_stat:.1f}, {p_fmt(w_p)}\n"
                      f"n={len(sub)}")
        ax.text(0.5, 0.98, annotation, transform=ax.transAxes, fontsize=9,
                 va='top', ha='center',
                 bbox=dict(boxstyle='round', facecolor='white', alpha=0.85))

        ax.set_title(label, fontsize=11)
        ax.grid(True, axis='y', alpha=0.3)

    fig.suptitle(
        "Paired absolute-error distribution HGT vs. HomoGNN\n"
        "Note: distributes per-test-example error (last-event subset), not per-hyperparameter-\n"
        "setting MAE -- no persisted HomoGNN sweep study exists to build the latter.",
        fontsize=10)
    plt.tight_layout(rect=[0, 0, 1, 0.88])
    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    frames = []

    for label, database, cant in DATASETS:
        paired = paired_last_event_ae(database, cant)
        paired.insert(0, 'dataset', label)
        frames.append(paired)

        t_stat, t_p = stats.ttest_rel(paired['hgt_ae'], paired['homo_ae'])
        w_stat, w_p = stats.wilcoxon(paired['hgt_ae'], paired['homo_ae'])
        print(f"{label}: n={len(paired)}  t={t_stat:.3f} (p={t_p:.4g})  "
              f"W={w_stat:.1f} (p={w_p:.4g})")

    df = pd.concat(frames, ignore_index=True)
    df.to_csv(CSV_PATH, index=False)
    print(f"\nSaved paired per-order data to {CSV_PATH} ({len(df)} rows)")

    render(df)
    print(f"Saved {PNG_PATH}")


if __name__ == '__main__':
    main()
