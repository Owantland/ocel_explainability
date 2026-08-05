"""Combines the 5 currently-relevant models' baseline comparisons (HGT vs. HomoGNN vs.
k-dim GNN vs. Mean predictor vs. GBT) into one CSV + one presentation-ready table image.

k-dim GNN (HOEG's own architecture, Morris et al. 2019, to_hetero()-wrapped) added
2026-08-01 -- see model_classes/KDIM_GNN.py and training.KDim_Reg_Modelling. All 4 source
CSVs were regenerated via compare_to_baselines() to add this row; the other 4 models'
numbers are unchanged from the prior run (compare_to_baselines() always recomputes every
row fresh, so this wasn't a partial/merged update).

Pure combine-and-render over already-computed, already-verified results -- no model
is re-run here. Source files were cross-referenced against checkpoint modification
times to make sure each is the CURRENT comparison for its checkpoint, not a stale one
superseded by a later retrain (see combined_baseline_comparison.csv's Note column and
the plan this script implements for the full reasoning):

- order_management/PayOrder   : validation_2000/model_comparison.csv (generic/current,
                                 the PayOrder-suffixed CSV predates an Jul-11 retrain)
- order_management/PackageDelivered : validation_2000/model_comparison_PackageDelivered.csv
                                 (no later retrain, still current)
- logistics/CustomerOrder->Depart : validation_1000/model_comparison_Depart.csv (KPI-suffixed,
                                 permanent -- see below for why this is no longer the generic file)
- logistics/CustomerOrder->LoadToVehicle : validation_1000/model_comparison_LoadToVehicle.csv
                                 (KPI-suffixed, permanent -- new second logistics KPI, 2026-07-26)

logistics/TransportDocument->Depart was dropped from this table (2026-07-26) -- it's the
pre-fix/superseded logistics viewpoint, superseded by CustomerOrder->Depart, and multiple
attempts this session to fix its negative last-event R2 (a narrow hidden_channels resweep, a
last-event-aware selection_metric retrain) were both rejected as regressions (see
project_capacity_vs_lastevent_mae_tradeoff memory). Its source CSV
(validation_1000/model_comparison_TransportDocument.csv) and checkpoint are left on disk for
reference, just no longer rendered here. Re-add it to SOURCES below if it's ever needed again.

2026-07-26 incident, why logistics no longer uses the generic filename: both logistics KPIs'
`compare` pipeline stage write to the SAME non-KPI-suffixed `model_comparison.csv` (only
`compare_to_baselines()`'s caller decides the filename, and it doesn't vary by kpi_event).
With config.yml pointed at LoadToVehicle, that stage silently overwrote the file this script
was reading for the Depart row, so the combined table briefly mislabeled LoadToVehicle's
numbers as Depart's. Recovered by restoring the `_customerorder_depart_backup` graph/hetero
caches, temporarily pointing config.yml back to Depart, and rerunning just the `compare`
stage -- confirmed matching the previously-established citable numbers (MAE_last=7.47h,
R2_last=-0.746) before saving permanently. Both logistics CustomerOrder-based entries now
read from permanent, KPI-suffixed files instead of the generic one, so this can't recur for
logistics; order_management/PayOrder still relies on the generic file (unchanged, out of
scope here) since it wasn't part of this incident.

Usage: python3 combined_baseline_comparison.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd

SOURCES = [
    ("Order Management / Orders → PayOrder",
     "files/explainer_outputs/order_management/validation_2000/model_comparison.csv", ""),
    ("Order Management / Orders → PackageDelivered",
     "files/explainer_outputs/order_management/validation_2000/model_comparison_PackageDelivered.csv", ""),
    ("Logistics / CustomerOrder → Depart",
     "files/explainer_outputs/logistics/validation_1000/model_comparison_Depart.csv", ""),
    ("Logistics / CustomerOrder → LoadToVehicle",
     "files/explainer_outputs/logistics/validation_1000/model_comparison_LoadToVehicle.csv", ""),
]

OUT_DIR = "files/explainer_outputs"
CSV_PATH = os.path.join(OUT_DIR, "combined_baseline_comparison.csv")
PNG_PATH = os.path.join(OUT_DIR, "combined_baseline_comparison_table.png")

MODEL_ORDER = ["HGT", "HomoGNN (GCN)", "k-dim GNN (HOEG)", "Mean predictor", "GBT"]


def load_combined():
    frames = []
    for kpi_label, path, note in SOURCES:
        df = pd.read_csv(path)
        # Source CSVs (written by explainer.py's compare_to_baselines()) label the
        # model "HGT (ours)" -- shortened to "HGT" here rather than in every source
        # file, so this stays correct even if those CSVs are regenerated later.
        df["Model"] = df["Model"].replace("HGT (ours)", "HGT")
        df.insert(0, "Dataset_KPI", kpi_label)
        df["Note"] = note
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    # Stable, presentation-friendly ordering: group by KPI (source order), then a
    # fixed model order within each group (matches compare_to_baselines()'s own
    # row order -- HGT first). Two explicit rank columns instead of a sort key=,
    # since we need two DIFFERENT custom orders (KPI source order, model order),
    # not the same key applied to both sort columns.
    kpi_rank = {kpi: i for i, (kpi, _, _) in enumerate(SOURCES)}
    combined["_kpi_rank"] = combined["Dataset_KPI"].map(kpi_rank)
    combined["_model_rank"] = combined["Model"].apply(MODEL_ORDER.index)
    combined = (combined.sort_values(by=["_kpi_rank", "_model_rank"])
                .drop(columns=["_kpi_rank", "_model_rank"])
                .reset_index(drop=True))
    return combined


def render_table(combined):
    # MAE cells fold their 95% bootstrap CI in as a second line (mae_bootstrap_ci() in
    # baselines.py -- percentile CI over resampled per-sample absolute errors, not a
    # symmetric +/-sigma), rather than adding two more low/high columns -- keeps the
    # table at 8 columns instead of 10 while still surfacing both requested figures
    # (CI and RMSE).
    col_labels = ["Dataset / KPI", "Model", "MAE (all) [h]\n(95% CI)", "RMSE (all) [h]",
                  "R² (all)", "MAE (last) [h]\n(95% CI)", "RMSE (last) [h]", "R² (last)"]
    cell_rows = []
    row_kpi = []
    # Only print the Dataset/KPI label on the first row of each group (grouped-row
    # style) instead of repeating it on all 4 rows -- both less cluttered and frees
    # up column width for the long KPI names, which were clipped when repeated on
    # every row (matplotlib's auto column width sizes each column once, based on
    # every cell, so 16 copies of a long string was forcing an unreadably-narrow
    # column when combined with the other 5 columns' available width).
    prev_kpi = None
    for _, r in combined.iterrows():
        is_first_in_group = r["Dataset_KPI"] != prev_kpi
        label = (r["Dataset_KPI"] + ("*" if r["Note"] else "")) if is_first_in_group else ""
        mae_all_cell = f"{r['MAE_all']:.1f}\n[{r['MAE_all_ci_low']:.1f}, {r['MAE_all_ci_high']:.1f}]"
        mae_last_cell = f"{r['MAE_last']:.1f}\n[{r['MAE_last_ci_low']:.1f}, {r['MAE_last_ci_high']:.1f}]"
        cell_rows.append([
            label, r["Model"],
            mae_all_cell, f"{r['RMSE_all']:.1f}", f"{r['R2_all']:.3f}",
            mae_last_cell, f"{r['RMSE_last']:.1f}", f"{r['R2_last']:.3f}",
        ])
        row_kpi.append(r["Dataset_KPI"])
        prev_kpi = r["Dataset_KPI"]

    # Same figsize formula and loc='center' compare_to_baselines()'s own (working,
    # established) single-KPI table uses -- a separate bottom-anchored footnote
    # (fig.text at an absolute y) left a large dead gap under a 'center'/'upper
    # center'-positioned table earlier; simplest fix is the asterisk marker in the
    # KPI label plus the Note column already in the CSV and printed summary,
    # without a second floating text element to position. Width bumped 13->16 and
    # per-row height 0.4->0.5 to fit the two-line MAE-with-CI cells and 2 new
    # RMSE columns without crowding.
    fig, ax = plt.subplots(figsize=(16, 0.5 * len(cell_rows) + 1.0))
    ax.axis('off')
    ax.set_title("Baseline comparison — all 5 currently-relevant models", fontsize=12)
    table = ax.table(cellText=cell_rows, colLabels=col_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2.2)
    table.auto_set_column_width(col=list(range(len(col_labels))))

    for col in range(len(col_labels)):
        table[0, col].set_facecolor("#EAEAEA")
        table[0, col].set_text_props(weight='bold')

    # Alternate a light shade per Dataset_KPI group so the 4 groups of 4 rows are
    # visually distinguishable, and bold-highlight each group's HGT row -- same
    # convention compare_to_baselines() uses for its single-KPI table.
    group_colors = ["#FFFFFF", "#F5F5F5"]
    kpi_order = [k for k, _, _ in SOURCES]
    for r_idx, (r, kpi) in enumerate(zip(combined.itertuples(), row_kpi), start=1):
        shade = group_colors[kpi_order.index(kpi) % 2]
        for col in range(len(col_labels)):
            table[r_idx, col].set_facecolor(shade)
        if r.Model == 'HGT':
            for col in range(len(col_labels)):
                table[r_idx, col].set_facecolor("#DCE6F1")
                table[r_idx, col].set_text_props(weight='bold')

    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()


def print_summary(combined):
    print("\nSummary (HGT vs. best baseline, last-event MAE):")
    for kpi_label, _, note in SOURCES:
        sub = combined[combined["Dataset_KPI"] == kpi_label]
        hgt = sub[sub["Model"] == "HGT"].iloc[0]
        best_baseline = sub[sub["Model"] != "HGT"].sort_values("MAE_last").iloc[0]
        flag = f"  [{note}]" if note else ""
        print(f"  {kpi_label}{flag}")
        print(f"    HGT                : MAE_last={hgt['MAE_last']:.1f}h  R2_last={hgt['R2_last']:.3f}")
        print(f"    Best baseline ({best_baseline['Model']}): "
              f"MAE_last={best_baseline['MAE_last']:.1f}h  R2_last={best_baseline['R2_last']:.3f}")


def main():
    combined = load_combined()
    os.makedirs(OUT_DIR, exist_ok=True)
    combined.to_csv(CSV_PATH, index=False)
    print(f"Saved combined CSV to {CSV_PATH} ({len(combined)} rows)")

    render_table(combined)
    print(f"Saved presentation table image to {PNG_PATH}")

    print_summary(combined)


if __name__ == '__main__':
    main()
