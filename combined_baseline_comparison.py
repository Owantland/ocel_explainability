"""Combines the 4 recently trained models' baseline comparisons (HGT vs. HomoGNN vs.
Mean predictor vs. GBT) into one CSV + one presentation-ready table image.

Pure combine-and-render over already-computed, already-verified results -- no model
is re-run here. Source files were cross-referenced against checkpoint modification
times to make sure each is the CURRENT comparison for its checkpoint, not a stale one
superseded by a later retrain (see combined_baseline_comparison.csv's Note column and
the plan this script implements for the full reasoning):

- order_management/PayOrder   : validation_2000/model_comparison.csv (generic/current,
                                 the PayOrder-suffixed CSV predates an Jul-11 retrain)
- order_management/PackageDelivered : validation_2000/model_comparison_PackageDelivered.csv
                                 (no later retrain, still current)
- logistics/CustomerOrder->Depart : validation_1000/model_comparison.csv (generic/current,
                                 the CustomerOrder-suffixed CSV predates a Jul-10 retrain)
- logistics/TransportDocument->Depart : validation_1000/model_comparison_TransportDocument.csv
                                 (pre-fix/superseded logistics viewpoint -- kept for
                                 completeness, not a current citable checkpoint)

Usage: python3 combined_baseline_comparison.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd

SOURCES = [
    ("order_management / PayOrder",
     "files/explainer_outputs/order_management/validation_2000/model_comparison.csv", ""),
    ("order_management / PackageDelivered",
     "files/explainer_outputs/order_management/validation_2000/model_comparison_PackageDelivered.csv", ""),
    ("logistics / CustomerOrder→Depart",
     "files/explainer_outputs/logistics/validation_1000/model_comparison.csv", ""),
    ("logistics / TransportDocument→Depart",
     "files/explainer_outputs/logistics/validation_1000/model_comparison_TransportDocument.csv",
     "pre-fix / superseded viewpoint"),
]

OUT_DIR = "files/explainer_outputs"
CSV_PATH = os.path.join(OUT_DIR, "combined_baseline_comparison.csv")
PNG_PATH = os.path.join(OUT_DIR, "combined_baseline_comparison_table.png")

MODEL_ORDER = ["HGT (ours)", "HomoGNN (GCN)", "Mean predictor", "GBT"]


def load_combined():
    frames = []
    for kpi_label, path, note in SOURCES:
        df = pd.read_csv(path)
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
    col_labels = ["Dataset / KPI", "Model", "MAE (last) [h]", "R² (last)",
                  "MAE (all) [h]", "R² (all)"]
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
        cell_rows.append([
            label, r["Model"],
            f"{r['MAE_last']:.1f}", f"{r['R2_last']:.3f}",
            f"{r['MAE_all']:.1f}", f"{r['R2_all']:.3f}",
        ])
        row_kpi.append(r["Dataset_KPI"])
        prev_kpi = r["Dataset_KPI"]

    # Same figsize formula and loc='center' compare_to_baselines()'s own (working,
    # established) single-KPI table uses -- a separate bottom-anchored footnote
    # (fig.text at an absolute y) left a large dead gap under a 'center'/'upper
    # center'-positioned table earlier; simplest fix is the asterisk marker in the
    # KPI label plus the Note column already in the CSV and printed summary,
    # without a second floating text element to position.
    fig, ax = plt.subplots(figsize=(13, 0.4 * len(cell_rows) + 0.9))
    ax.axis('off')
    ax.set_title("Baseline comparison — all 4 recently trained models\n"
                "(* = pre-fix / superseded logistics viewpoint, shown for completeness)",
                fontsize=12)
    table = ax.table(cellText=cell_rows, colLabels=col_labels, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.6)
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
        if r.Model == 'HGT (ours)':
            for col in range(len(col_labels)):
                table[r_idx, col].set_facecolor("#DCE6F1")
                table[r_idx, col].set_text_props(weight='bold')

    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()


def print_summary(combined):
    print("\nSummary (HGT vs. best baseline, last-event MAE):")
    for kpi_label, _, note in SOURCES:
        sub = combined[combined["Dataset_KPI"] == kpi_label]
        hgt = sub[sub["Model"] == "HGT (ours)"].iloc[0]
        best_baseline = sub[sub["Model"] != "HGT (ours)"].sort_values("MAE_last").iloc[0]
        flag = f"  [{note}]" if note else ""
        print(f"  {kpi_label}{flag}")
        print(f"    HGT (ours)         : MAE_last={hgt['MAE_last']:.1f}h  R2_last={hgt['R2_last']:.3f}")
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
