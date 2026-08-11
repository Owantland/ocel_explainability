"""Galanti et al. 2023b Fig. 1-style global explanation bar charts, for both datasets.

Runs explain_gnn_primary_aggregate() (the pipeline's own headline aggregate
explainability method) for order_management and logistics, then copies its new
per-decoded-identity 'attr=value' explanation bar chart + CSV (added directly to
that method) into thesis_parts/figures_tables/ as citable, presentation-ready
thesis figures -- the method's own default save_dir (files/explainer_outputs/...)
is left untouched as the normal pipeline output location.

Usage: python3 aggregate_explanation_bars.py
"""
import os
import shutil

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables"

# (dataset display label, database, cant)
DATASETS = [
    ("order_management", "order_management", 2000),
    ("logistics", "logistics", 1000),
]


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    for label, database, cant in DATASETS:
        print(f"\n{'='*70}\n{label}: running explain_gnn_primary_aggregate()...\n{'='*70}")
        e = exp.Explainer(database, cant)
        result = e.explain_gnn_primary_aggregate(n_traces=50)

        src_png = os.path.join(result['save_dir'], "aggregate_explanation_bars.png")
        src_csv = os.path.join(result['save_dir'], "aggregate_explanation_bars.csv")
        dst_png = os.path.join(OUT_DIR, f"aggregate_explanation_bars_{database}.png")
        dst_csv = os.path.join(OUT_DIR, f"aggregate_explanation_bars_{database}.csv")
        shutil.copy(src_png, dst_png)
        shutil.copy(src_csv, dst_csv)
        print(f"Copied {src_png} -> {dst_png}")
        print(f"Copied {src_csv} -> {dst_csv}")


if __name__ == '__main__':
    main()
