"""Refreshes every thesis figure that depends on the Depart/LoadToVehicle checkpoints
promoted 2026-08-05/07 (AdamW retrain, see project_hgt_dead_features_finding memory).
Implements the plan at .claude/plans/propose-a-plan-to-whimsical-cerf.md, steps 1-7.

For each of the two logistics KPIs (Depart, LoadToVehicle), in turn:
  1. Swap in that KPI's graph cache + config.yml kpi_event (reusing
     retune_depart_full_search.py's already-validated _swap_graphs/_set_kpi_event).
  2. run_pipeline.run('logistics', 1000, {'validate','compare','explain'}) -- refreshes
     model_comparison_*.csv, mae_by_depth CSVs, residuals.png, aggregate_metrics.csv (LOO),
     feature attribution, two single-trace explanations.
  3. Explainer.explain_gnn_primary_aggregate(n_traces=50) -- aggregate_gnnprimary_metrics.csv.
  4. Explainer.validate_fidelity_comparison(n_traces=50) -- fidelity_validation_paired.csv.
  5. If Depart: Explainer.compare_loo_vs_shapley(order_id) for the 5 existing logistics
     orders (863/864/867/870/874 -- confirmed Depart-era, since LoadToVehicle didn't exist
     yet on 2026-07-20 when these were first generated).
  6. Re-collect the 4 live-inference showcases via subprocess (each script's own --collect
     CLI), chaining order_id/n_events between them exactly as their own docstrings specify.

After both KPIs: restore the graph cache to LoadToVehicle (this repo's resting state),
then render every affected image (the 4 showcases' --render, the subgraph composer,
mae_by_depth_summary.py, training_curves_summary.py, combined_baseline_comparison.py [+
copy into thesis_parts/figures_tables/, the one script that doesn't write there directly],
loo_vs_shapley_agreement.py, fidelity_comparison_summary.py).

Does NOT touch the production checkpoints -- read-only inference against them throughout.

Usage: python3 refresh_logistics_thesis_figures.py
"""
import json
import os
import shutil
import subprocess

import retune_depart_full_search as swap  # reuse its validated _swap_graphs/_set_kpi_event
import run_pipeline
import explainer as exp

DATABASE = "logistics"
CANT = 1000
THESIS_FIGURES_DIR = "thesis_parts/figures_tables"

KPIS = [
    ("Depart", "Logistics / CustomerOrder -> Depart", swap.DEPART_BACKUP),
    ("LoadToVehicle", "Logistics / CustomerOrder -> LoadToVehicle", swap.LOADTOVEHICLE_BACKUP),
]

LOO_VS_SHAPLEY_DEPART_ORDERS = [863, 864, 867, 870, 874]

TRACE_SCRIPT = "loo_shapley_trace_showcase.py"
EDGE_SCRIPT = "loo_shapley_edge_trace_showcase.py"
PREFIX_SCRIPT = "loo_shapley_prefix_evolution_showcase.py"
AGGREGATE_SCRIPT = "loo_shapley_aggregate_showcase.py"
TRACE_DATA_PATH = os.path.join(THESIS_FIGURES_DIR, "loo_shapley_trace_showcase_data.json")


def run_showcase(script, *args):
    cmd = ["python3", script, *args]
    print(f"\n$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def process_kpi(kpi_event, kpi_label, backup_dir):
    print(f"\n{'='*70}\nPROCESSING {kpi_label}\n{'='*70}")
    print(f"Swapping in {kpi_event}'s graph cache and setting kpi_event={kpi_event!r}...")
    swap._swap_graphs(backup_dir)
    swap._set_kpi_event(kpi_event)

    print(f"\n--- run_pipeline: validate,compare,explain for {kpi_event} ---")
    run_pipeline.run(DATABASE, CANT, {'validate', 'compare', 'explain'})

    print(f"\n--- explain_gnn_primary_aggregate (n_traces=50) for {kpi_event} ---")
    e = exp.Explainer(DATABASE, CANT)
    e.explain_gnn_primary_aggregate(n_traces=50, top_k=5)

    print(f"\n--- validate_fidelity_comparison (n_traces=50) for {kpi_event} ---")
    e2 = exp.Explainer(DATABASE, CANT)
    e2.validate_fidelity_comparison(n_traces=50, top_k=5)

    if kpi_event == "Depart":
        print(f"\n--- compare_loo_vs_shapley for {len(LOO_VS_SHAPLEY_DEPART_ORDERS)} Depart orders ---")
        e3 = exp.Explainer(DATABASE, CANT)
        for order_id in LOO_VS_SHAPLEY_DEPART_ORDERS:
            print(f"  order_id={order_id}")
            e3.compare_loo_vs_shapley(order_id)

    print(f"\n--- re-collecting 4 live-inference showcases for {kpi_label!r} ---")
    run_showcase(TRACE_SCRIPT, "--collect", kpi_label, "--database", DATABASE, "--cant", str(CANT))

    with open(TRACE_DATA_PATH) as f:
        trace_data = json.load(f)
    last_n_events = trace_data[kpi_label]['n_events']
    early_n_events = max(1, last_n_events // 3)
    print(f"  order_id={trace_data[kpi_label]['order_id']}  last_n_events={last_n_events}  "
          f"early_n_events(for prefix evolution)={early_n_events}")

    run_showcase(EDGE_SCRIPT, "--collect", kpi_label, "--database", DATABASE, "--cant", str(CANT))
    run_showcase(PREFIX_SCRIPT, "--collect", kpi_label, "--database", DATABASE, "--cant", str(CANT),
                 "--n-events", str(early_n_events))
    run_showcase(AGGREGATE_SCRIPT, "--collect", kpi_label, "--database", DATABASE, "--cant", str(CANT))


def render_all():
    print(f"\n{'='*70}\nRENDERING ALL AFFECTED IMAGES\n{'='*70}")
    run_showcase(TRACE_SCRIPT, "--render")
    run_showcase(EDGE_SCRIPT, "--render")
    run_showcase(PREFIX_SCRIPT, "--render")
    run_showcase(AGGREGATE_SCRIPT, "--render")
    run_showcase("loo_shapley_subgraph_showcase.py")
    run_showcase("mae_by_depth_summary.py")
    run_showcase("training_curves_summary.py")
    run_showcase("loo_vs_shapley_agreement.py")
    run_showcase("fidelity_comparison_summary.py")

    print("\n--- combined_baseline_comparison.py (writes to files/explainer_outputs/ only) ---")
    run_showcase("combined_baseline_comparison.py")
    src = "files/explainer_outputs/combined_baseline_comparison_table.png"
    dst = os.path.join(THESIS_FIGURES_DIR, "combined_baseline_comparison_table.png")
    shutil.copy(src, dst)
    print(f"Copied {src} -> {dst}")
    src_csv = "files/explainer_outputs/combined_baseline_comparison.csv"
    dst_csv = os.path.join(THESIS_FIGURES_DIR, "combined_baseline_comparison.csv")
    if os.path.exists(src_csv):
        shutil.copy(src_csv, dst_csv)
        print(f"Copied {src_csv} -> {dst_csv}")


def main():
    try:
        for kpi_event, kpi_label, backup_dir in KPIS:
            process_kpi(kpi_event, kpi_label, backup_dir)
    finally:
        print(f"\n{'='*70}\nRestoring LoadToVehicle graph cache and kpi_event (resting state)...\n{'='*70}")
        swap._swap_graphs(swap.LOADTOVEHICLE_BACKUP)
        swap._set_kpi_event("LoadToVehicle")
        print("Restored.")

    render_all()
    print(f"\n{'='*70}\nDONE -- all affected thesis figures refreshed.\n{'='*70}")


if __name__ == '__main__':
    main()
