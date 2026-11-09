"""Re-runs explain_gnn_primary_aggregate() + validate_fidelity_comparison() specifically
for Depart (not LoadToVehicle) against the promoted AdamW checkpoint, so
fidelity_comparison_summary.png's single "Logistics" row represents Depart -- the more
established/primary logistics KPI cited elsewhere in the thesis -- rather than
LoadToVehicle, which is what refresh_logistics_thesis_figures.py's batch run left behind
(LoadToVehicle ran last in that script's loop, silently overwriting Depart's fidelity data
in the same generic per-database files: aggregate_gnnprimary_metrics.csv,
fidelity_validation_paired.csv -- confirmed neither is KPI-suffixed, unlike
model_comparison.csv which at least has KPI-suffixed siblings to rescue into).

User decision (2026-08-07): re-run for Depart rather than keep LoadToVehicle's numbers,
matching Depart's role as the primary/first-cited logistics KPI throughout the thesis.

Does NOT touch the production checkpoint -- read-only inference only.

Usage: python3 refresh_depart_fidelity.py
"""
import retune_depart_full_search as swap
import explainer as exp

DATABASE = "logistics"
CANT = 1000


def main():
    print("Swapping in Depart's graph cache and setting kpi_event='Depart'...")
    swap._swap_graphs(swap.DEPART_BACKUP)
    swap._set_kpi_event("Depart")

    try:
        print("\n--- explain_gnn_primary_aggregate (n_traces=50) for Depart ---")
        e = exp.Explainer(DATABASE, CANT)
        e.explain_gnn_primary_aggregate(n_traces=50, top_k=5)

        print("\n--- validate_fidelity_comparison (n_traces=50) for Depart ---")
        e2 = exp.Explainer(DATABASE, CANT)
        e2.validate_fidelity_comparison(n_traces=50, top_k=5)
    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event (resting state)...")
        swap._swap_graphs(swap.LOADTOVEHICLE_BACKUP)
        swap._set_kpi_event("LoadToVehicle")
        print("Restored.")

    print("\nDONE -- Depart's fidelity data refreshed. Run fidelity_comparison_summary.py next.")


if __name__ == '__main__':
    main()
