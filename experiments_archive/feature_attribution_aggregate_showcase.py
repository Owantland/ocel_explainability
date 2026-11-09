"""Aggregate feature-attribution showcase, one worked example per KPI (4 total) --
the aggregate analog of gradient_shapley_comparison_showcase.py, standing on its own
(not paired against LOO-Shapley this time).

Uses explainer.py's explain_feature_attribution(n_traces, methods, depth_stratify) --
mean-abs/mean-signed attribution per node type per feature, pooled across n_traces
last-event test graphs. Runs BOTH InputXGradient and IntegratedGradients in one call,
so only 4 collection runs are needed total (one per KPI) to produce both methods'
final images.

explain_feature_attribution() has NO save_dir override (unlike explain_aggregate_shapley)
-- its out_dir is hardcoded to `{self.path_dict['explainer_path']}/attribution/`, which
is per-DATABASE, not per-KPI (the same generic-filename collision class already fixed
twice this session for model_comparison.csv and explain_aggregate_shapley's old
default). Worked around here by temporarily reassigning the freshly-built Explainer
instance's OWN path_dict['explainer_path'] before calling -- safe, since it's a fresh
instance's own dict each run, not shared global state.

Out of scope for this showcase (see plan): the function's built-in top-K/bottom-K
perturbation fidelity check (printed-only, no artifact) and the depth-stratified
breakdown (richer, its own figure) -- called with depth_stratify=False here.

Usage:
    python3 feature_attribution_aggregate_showcase.py --collect "Order Management / Orders -> PayOrder" --database order_management --cant 2000
    python3 feature_attribution_aggregate_showcase.py --collect "Logistics / CustomerOrder -> LoadToVehicle" --database logistics --cant 1000
    # (switch config.yml, regenerate graphs for PackageDelivered/Depart as needed, then:)
    python3 feature_attribution_aggregate_showcase.py --collect "Order Management / Orders -> PackageDelivered" --database order_management --cant 2000
    python3 feature_attribution_aggregate_showcase.py --collect "Logistics / CustomerOrder -> Depart" --database logistics --cant 1000
    python3 feature_attribution_aggregate_showcase.py --render
"""
import argparse
import json
import os

import matplotlib.pyplot as plt
import pandas as pd
import torch

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables"
DATA_PATH = os.path.join(OUT_DIR, "feature_attribution_aggregate_showcase_data.json")
PNG_PATHS = {
    'InputXGradient': os.path.join(OUT_DIR, "feature_attribution_aggregate_showcase_inputxgradient.png"),
    'IntegratedGradients': os.path.join(OUT_DIR, "feature_attribution_aggregate_showcase_integratedgradients.png"),
}

KPI_ORDER = [
    "Order Management / Orders -> PayOrder",
    "Order Management / Orders -> PackageDelivered",
    "Logistics / CustomerOrder -> Depart",
    "Logistics / CustomerOrder -> LoadToVehicle",
]

N_TRACES = 50
TOP_N = 8
METHODS = ('InputXGradient', 'IntegratedGradients')


def collect(kpi_label, database, cant):
    e = exp.Explainer(database, cant)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    # Redirect explain_feature_attribution's hardcoded out_dir
    # ({explainer_path}/attribution/) to a KPI-suffixed location, since the function
    # itself has no save_dir override and would otherwise collide with the other KPI
    # sharing this database.
    kpi_base = f"files/explainer_outputs/{database}/kpi_{e.task_id}"
    e.path_dict['explainer_path'] = kpi_base
    attribution_dir = os.path.join(kpi_base, "attribution")

    e.explain_feature_attribution(n_traces=N_TRACES, methods=METHODS, depth_stratify=False)

    all_data = {}
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH) as f:
            all_data = json.load(f)

    entry = {'kpi_label': kpi_label, 'n_traces': N_TRACES, 'methods': {}}
    for method in METHODS:
        csv_path = os.path.join(attribution_dir, f"ig_attribution_{method.lower()}.csv")
        df = pd.read_csv(csv_path)
        df['abs_rank'] = df['mean_abs'].abs()
        top = df.sort_values('abs_rank', ascending=False).head(TOP_N)
        entry['methods'][method] = [
            {'label': f"{r.node_type}: {r.feature_name}", 'mean_signed': r.mean_signed,
             'mean_abs': r.mean_abs}
            for r in top.itertuples()
        ]
    all_data[kpi_label] = entry

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(DATA_PATH, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"Saved collected result for {kpi_label!r} to {DATA_PATH} (from {attribution_dir})")


def render():
    with open(DATA_PATH) as f:
        all_data = json.load(f)
    missing = [k for k in KPI_ORDER if k not in all_data]
    if missing:
        raise SystemExit(f"Missing collected data for: {missing}. Run --collect for each first.")

    for method, png_path in PNG_PATHS.items():
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()
        for ax, kpi_label in zip(axes, KPI_ORDER):
            entry = all_data[kpi_label]
            rows = sorted(entry['methods'][method], key=lambda r: r['mean_signed'])
            labels = [r['label'] for r in rows]
            values = [r['mean_signed'] for r in rows]
            colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]

            ax.barh(range(len(labels)), values, color=colors)
            ax.set_yticks(range(len(labels)))
            ax.set_yticklabels(labels, fontsize=8)
            ax.axvline(0, color='black', linewidth=0.8)
            ax.set_xlabel(f"Mean {method} attribution (raw, unitless)")
            ax.set_title(f"{kpi_label}\n(n_traces={entry['n_traces']})", fontsize=10)
            ax.grid(True, axis='x', alpha=0.3)

        fig.suptitle(f"Aggregate feature attribution ({method}) — one worked example per KPI\n"
                     f"(red = raises predicted remaining time, blue = lowers it; ranked by "
                     f"mean |attribution| across {N_TRACES} last-event traces)", fontsize=12)
        plt.tight_layout(rect=[0, 0, 1, 0.93])
        plt.savefig(png_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved {png_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect', metavar='KPI_LABEL')
    parser.add_argument('--database', choices=['order_management', 'logistics'])
    parser.add_argument('--cant', type=int)
    parser.add_argument('--render', action='store_true')
    args = parser.parse_args()

    if args.collect:
        if not args.database or not args.cant:
            raise SystemExit("--collect requires --database and --cant")
        collect(args.collect, args.database, args.cant)
    elif args.render:
        render()
    else:
        raise SystemExit("Pass --collect \"<KPI label>\" --database ... --cant ... or --render")


if __name__ == '__main__':
    main()
