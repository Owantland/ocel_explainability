"""MAE vs. prefix-depth, all baselines, all 5 currently-relevant KPIs -- addresses
thesis_structure.txt's explicit ask to "report MAE against prefix depth, not just
an aggregate figure."

Extended 2026-10-13 to add the 5th KPI, CargoPickup_IoT's Pickupplan->EvaluateTruckExit --
the depth-binned CSV was generated via baselines.py --database cargo_pickup_iot --cant 2000
(no config.yml switching needed, unlike the 2026-07-27 extension below -- this database
only has one KPI configured so far).

Extended 2026-07-27 to cover all 4 KPIs featured in hyperparameter_sweep_summary.png /
combined_baseline_comparison_table.png (previously only PayOrder/Depart). The two new
depth-binned CSVs (PackageDelivered, CustomerOrder->LoadToVehicle) didn't exist yet --
generated via baselines.py's run(database, cant), temporarily switching config.yml's
order_management kpi_event to PackageDelivered (backing up and restoring the PayOrder-era
graph/hetero caches around it, since they're kpi_event-specific); logistics/LoadToVehicle
needed no switching since config.yml already pointed there. Both new CSVs' aggregate
MAE/R2 were cross-checked against the already-verified combined_baseline_comparison_table.png
numbers and match exactly.

Regenerated 2026-07-23 against the current citable checkpoints (order_management/
PayOrder, logistics/CustomerOrder->Depart) via baselines.py's run(database, cant)
-- previously baselines.py only ever ran hardcoded for order_management/2000 and
Logistics' only depth-binned file on disk was keyed to the superseded
TransportDocument viewpoint; both gaps are closed (see baselines.py and
docs/archive/OPEN_ISSUES_FEASIBILITY.md item 7). Source CSVs are now named by task_id rather
than by event/viewpoint name, so they can't silently go stale the same way again
if the viewpoint or KPI event changes.

Usage: python3 mae_by_depth_summary.py
"""
import os
import shutil

import matplotlib.pyplot as plt
import pandas as pd

OUT_DIR = "thesis_parts/figures_tables"

# Self-promotion target, added 2026-10-13 to match combined_baseline_comparison.py's
# STAGING_DIR/BUILD_DIR pattern -- previously this script only wrote to OUT_DIR above,
# so the actual LaTeX build figure (results.tex's \includegraphics{figures/
# mae_by_depth_summary.png}) silently went stale every time this script was re-run,
# since nothing ever copied the regenerated PNG into the build directory. Caught when
# CargoPickup_IoT's 5th-KPI row was added here but never showed up in the built PDF.
BUILD_DIR = "thesis_parts/latex_template/figures"

# (dataset label, path) -- label style matches hyperparameter_sweep_summary.png /
# combined_baseline_comparison_table.png: "[Dataset] / [Object] -> [KPI Event]"
SOURCES = [
    ("Order Management / Orders → PayOrder",
     "files/explainer_outputs/order_management/validation_2000/mae_by_depth_TimeFrom_Orders_to_PayOrder.csv"),
    ("Order Management / Orders → PackageDelivered",
     "files/explainer_outputs/order_management/validation_2000/mae_by_depth_TimeFrom_Orders_to_PackageDelivered.csv"),
    ("Logistics / CustomerOrder → Depart",
     "files/explainer_outputs/logistics/validation_1000/mae_by_depth_TimeFrom_CustomerOrder_to_Depart.csv"),
    ("Logistics / CustomerOrder → LoadToVehicle",
     "files/explainer_outputs/logistics/validation_1000/mae_by_depth_TimeFrom_CustomerOrder_to_LoadToVehicle.csv"),
    ("CargoPickup_IoT / Pickup Plan → EvaluateTruckExit",
     "files/explainer_outputs/cargo_pickup_iot/validation_2000/mae_by_depth_TimeFrom_Pickupplan_to_EvaluateTruckExit.csv"),
]

DEPTH_ORDER = ["1-3", "4-6", "7-9", "10+"]
MODEL_COLORS = {
    "HGT": "#4e79a7",
    "HomoGNN": "#59a14f",
    "Mean": "#bab0ac",
    "GBT": "#e15759",
}


def load_all():
    frames = []
    for label, path in SOURCES:
        df = pd.read_csv(path)
        df['depth_bin'] = pd.Categorical(df['depth_bin'], categories=DEPTH_ORDER, ordered=True)
        df = df.sort_values('depth_bin')
        df.insert(0, 'dataset', label)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def render(df):
    ncols = 2
    nrows = (len(SOURCES) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(13, 5 * nrows))
    axes = axes.flatten()
    for ax, (label, _) in zip(axes, SOURCES):
        sub = df[df['dataset'] == label]
        for model, color in MODEL_COLORS.items():
            ax.plot(sub['depth_bin'], sub[model], marker='o', color=color, label=model)
        ax.set_title(label, fontsize=10)
        ax.set_xlabel("Prefix depth (events)")
        ax.set_ylabel("MAE (hours)")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    for ax in axes[len(SOURCES):]:
        ax.axis('off')

    fig.suptitle("MAE vs. prefix depth, all 5 currently-relevant KPIs",
                 fontsize=11)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    png_path = os.path.join(OUT_DIR, "mae_by_depth_summary.png")
    plt.savefig(png_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {png_path}")

    os.makedirs(BUILD_DIR, exist_ok=True)
    build_path = os.path.join(BUILD_DIR, "mae_by_depth_summary.png")
    shutil.copy(png_path, build_path)
    print(f"Promoted to {build_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = load_all()
    csv_path = os.path.join(OUT_DIR, "mae_by_depth_summary.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path} ({len(df)} rows)")

    render(df)


if __name__ == '__main__':
    main()
