"""Promotes the 4 per-KPI "global" LOO+Shapley aggregate bar charts to thesis figures.

Each chart is a single, full-size, ranked list (not the compact 4-panel-per-dataset
style of loo_shapley_aggregate_showcase.py) of every node identity whose presence
shifted the model's prediction, pooled across n_traces last-event traces for one KPI.
This is the "holistic aggregate view" / "helicopter view" figure set added in response
to deleonis_comment.txt's comment A.

The underlying computation already exists and was already run: explain_aggregate_
shapley() (the thesis's PRIMARY LOO+Shapley method, NOT aggregate_explanation_bars.py's
explain_gnn_primary_aggregate(), which is GNNExplainer-driven and comparison-only --
see loo_shapley_aggregate_showcase.py's docstring for the same warning) already writes
an "aggregate_explanation_bars.png"/".csv" pair per KPI, via loo_shapley_aggregate_
showcase.py's own --collect step, to:
    files/explainer_outputs/<database>/aggregate_shapley_<task_id>/

This script does NOT re-run that (expensive, ~81s/KPI) computation. It only:
  1. Reloads each KPI's already-computed per-label means from its CSV (mean_signed_
     shift column -- std/count aren't used by the plot itself, see
     plot_aggregate_explanation_bars()'s docstring, so a single-value list per label
     reproduces an identical render without needing the raw per-trace values).
  2. Re-renders via plot_aggregate_explanation_bars() -- picking up whatever layout
     fixes are current in explainer.py (e.g. the wider negative-side margin fix that
     resolved a value-label/category-label collision on the widest bar, found when
     these figures were first promoted).
  3. Copies the result into thesis_parts/figures_tables/ (staging) and thesis_parts/
     latex_template/figures/ (what the actual PDF build reads -- these two are NOT
     auto-synced in this repo, confirmed by their pre-existing divergence for other
     figures; both need the file).

Constructing an Explainer per dataset uses whatever KPI files/config.yml currently
points to for that dataset's target_std (only affects the ">1 std = large shift" bold
edge on the plot, not any bar's position/text -- a cosmetic-only approximation for
whichever 2 of the 4 KPIs aren't config.yml's live default at run time).

Usage: python3 promote_aggregate_shapley_global_figures.py
"""
import os
import shutil

import pandas as pd
import torch

import explainer as exp

JOBS = [
    ("order_management", 2000,
     "files/explainer_outputs/order_management/aggregate_shapley_TimeFrom_Orders_to_PayOrder",
     "aggregate_shapley_global_om_payorder.png"),
    ("order_management", 2000,
     "files/explainer_outputs/order_management/aggregate_shapley_TimeFrom_Orders_to_PackageDelivered",
     "aggregate_shapley_global_om_packagedelivered.png"),
    ("logistics", 1000,
     "files/explainer_outputs/logistics/aggregate_shapley_TimeFrom_CustomerOrder_to_Depart",
     "aggregate_shapley_global_logistics_depart.png"),
    ("logistics", 1000,
     "files/explainer_outputs/logistics/aggregate_shapley_TimeFrom_CustomerOrder_to_LoadToVehicle",
     "aggregate_shapley_global_logistics_loadtovehicle.png"),
]

STAGING_DIR = "thesis_parts/figures_tables"
BUILD_DIR = "thesis_parts/latex_template/figures"


def main():
    os.makedirs(STAGING_DIR, exist_ok=True)
    _cache = {}
    for database, cant, save_dir, out_name in JOBS:
        if database not in _cache:
            e = exp.Explainer(database, cant)
            e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
            e.model.eval()
            _cache[database] = e
        e = _cache[database]

        csv_path = os.path.join(save_dir, "aggregate_explanation_bars.csv")
        df = pd.read_csv(csv_path)
        shifts = {row["label"]: [row["mean_signed_shift"]] for _, row in df.iterrows()}

        tmp_png = os.path.join(save_dir, "aggregate_explanation_bars.png")
        e.plot_aggregate_explanation_bars(
            shifts, tmp_png,
            "Global Shapley explanations for remaining time prediction",
            dataset_label=database, n_traces=50, top_n=20,
        )

        staging_path = os.path.join(STAGING_DIR, out_name)
        build_path = os.path.join(BUILD_DIR, out_name)
        shutil.copy(tmp_png, staging_path)
        shutil.copy(tmp_png, build_path)
        print(f"Regenerated and promoted {out_name}")
        print(f"  -> {staging_path}")
        print(f"  -> {build_path}")


if __name__ == "__main__":
    main()
