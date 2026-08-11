"""Aggregate LOO+Shapley showcase, one worked example per KPI (4 total) -- the
aggregate analog of loo_shapley_trace_showcase.py. Same panel layout/convention
("[Dataset] / [Object] -> [KPI Event]", red = pushes prediction later, blue =
pushes prediction earlier), but pooled across n_traces last-event traces per KPI
instead of a single trace.

Uses explainer.py's explain_aggregate_shapley() -- NOT aggregate_explanation_bars.py's
explain_gnn_primary_aggregate() (that one identifies candidates via GNNExplainer, not
LOO, despite similar-looking output). explain_aggregate_shapley() mirrors
explain_trace_shapley()'s own split: exhaustive per-trace LOO across n_traces decides
the top ~20 winning labels, then only those get Shapley-requantified by revisiting a
few of the traces where each label occurred -- same "LOO identifies, Shapley
quantifies" pattern as the single-trace figure, just pooled. Empirically ~81s per KPI
at the defaults used here (n_traces=50, n_samples=30, revisit_n=3,
max_revisit_candidates=6), per explain_aggregate_shapley's own docstring.

save_dir is passed explicitly, KPI-suffixed (files/explainer_outputs/<db>/
aggregate_shapley_<task_id>/) rather than the function's collision-prone default
("aggregate/", shared and overwritten across whichever KPI was last run --
same class of bug already found and fixed once this session for
combined_baseline_comparison.py's generic model_comparison.csv).

Two modes, since PackageDelivered/Depart need config.yml pointed at a different
kpi_event than PayOrder/LoadToVehicle (the two KPIs whose graph caches are the live
default as of this writing):
  --collect <KPI label>   Runs explain_aggregate_shapley() against whatever config.yml
                           is CURRENTLY pointed at, appends the result to the shared JSON.
  --render                 Builds the final 2x2 PNG from all 4 collected results.

Usage:
    python3 loo_shapley_aggregate_showcase.py --collect "Order Management / Orders -> PayOrder" --database order_management --cant 2000
    python3 loo_shapley_aggregate_showcase.py --collect "Logistics / CustomerOrder -> LoadToVehicle" --database logistics --cant 1000
    # (switch config.yml, regenerate graphs for PackageDelivered/Depart as needed, then:)
    python3 loo_shapley_aggregate_showcase.py --collect "Order Management / Orders -> PackageDelivered" --database order_management --cant 2000
    python3 loo_shapley_aggregate_showcase.py --collect "Logistics / CustomerOrder -> Depart" --database logistics --cant 1000
    python3 loo_shapley_aggregate_showcase.py --render
"""
import argparse
import json
import os

import matplotlib.pyplot as plt
import pandas as pd
import torch

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables"
DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_aggregate_showcase_data.json")
PNG_PATH = os.path.join(OUT_DIR, "loo_shapley_aggregate_showcase.png")

# Fixed panel order regardless of collection order, matching every other 4-KPI figure
# this thesis uses.
KPI_ORDER = [
    "Order Management / Orders -> PayOrder",
    "Order Management / Orders -> PackageDelivered",
    "Logistics / CustomerOrder -> Depart",
    "Logistics / CustomerOrder -> LoadToVehicle",
]

N_TRACES = 50
TOP_N = 8


def collect(kpi_label, database, cant):
    e = exp.Explainer(database, cant)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    save_dir = f"files/explainer_outputs/{database}/aggregate_shapley_{e.task_id}"
    e.explain_aggregate_shapley(n_traces=N_TRACES, top_k=5, save_dir=save_dir)

    bars_csv = os.path.join(save_dir, "aggregate_explanation_bars.csv")
    df = pd.read_csv(bars_csv)
    # Already sorted by abs(mean_signed_shift) descending and capped at top_n=20 by
    # plot_aggregate_explanation_bars() -- just take the first TOP_N rows.
    top = df.head(TOP_N)

    entry = {
        'kpi_label': kpi_label,
        'n_traces': N_TRACES,
        'rows': [
            {'label': r.label, 'mean_signed_shift': r.mean_signed_shift, 'std_shift': r.std_shift,
             'count': int(r.count)}
            for r in top.itertuples()
        ],
    }

    all_data = {}
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH) as f:
            all_data = json.load(f)
    all_data[kpi_label] = entry
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(DATA_PATH, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"Saved collected result for {kpi_label!r} to {DATA_PATH}")


def render():
    with open(DATA_PATH) as f:
        all_data = json.load(f)
    missing = [k for k in KPI_ORDER if k not in all_data]
    if missing:
        raise SystemExit(f"Missing collected data for: {missing}. Run --collect for each first.")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()
    for ax, kpi_label in zip(axes, KPI_ORDER):
        entry = all_data[kpi_label]
        rows = sorted(entry['rows'], key=lambda r: r['mean_signed_shift'])
        labels = [r['label'] for r in rows]
        means = [r['mean_signed_shift'] for r in rows]
        stds = [r['std_shift'] for r in rows]
        colors = ['#e15759' if v > 0 else '#4e79a7' for v in means]

        ax.barh(range(len(labels)), means, xerr=stds, color=colors,
                error_kw={'ecolor': 'gray', 'elinewidth': 1, 'capsize': 3})
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=9)
        ax.axvline(0, color='black', linewidth=0.8)
        ax.set_xlabel("Mean signed Shapley shift (hours)")
        ax.set_title(f"{kpi_label}\n(n_traces={entry['n_traces']}, error bars = ±1 std)",
                     fontsize=10)
        ax.grid(True, axis='x', alpha=0.3)

    fig.suptitle("LOO-identified, Shapley-quantified aggregate explanation — one worked "
                 "example per KPI\n(red = pushes prediction later, blue = pushes prediction "
                 "earlier)", fontsize=12)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {PNG_PATH}")


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
