"""Prefix-evolution showcase: for the SAME 4 real process executions already used in
loo_shapley_trace_showcase.py, show how LOO-identified/Shapley-quantified node
importance changes between an EARLY prefix and the LAST-EVENT prefix.

The last-event side is already fully computed (loo_shapley_trace_showcase_data.json)
-- only the early-prefix side needs a fresh explain_trace_shapley() call, passing an
explicit n_events instead of None. Reuses the exact same order_id per KPI so this
reads as "same real example, earlier in its life" rather than a different trace.

Early n_events is chosen per trace as max_available_n_events // 3 (a reproducible
rule, not a fixed absolute depth, since trace lengths vary hugely across KPIs --
e.g. order 1801/PayOrder has 12 prefixes total, order 911/LoadToVehicle has 88).

Usage:
    python3 loo_shapley_prefix_evolution_showcase.py --collect "Order Management / Orders -> PayOrder" --database order_management --cant 2000 --n-events 4
    python3 loo_shapley_prefix_evolution_showcase.py --collect "Logistics / CustomerOrder -> LoadToVehicle" --database logistics --cant 1000 --n-events 29
    # (switch config.yml, regenerate graphs for PackageDelivered/Depart as needed, then:)
    python3 loo_shapley_prefix_evolution_showcase.py --collect "Order Management / Orders -> PackageDelivered" --database order_management --cant 2000 --n-events <N>
    python3 loo_shapley_prefix_evolution_showcase.py --collect "Logistics / CustomerOrder -> Depart" --database logistics --cant 1000 --n-events <N>
    python3 loo_shapley_prefix_evolution_showcase.py --render
"""
import argparse
import json
import os
import shutil

import matplotlib.pyplot as plt
import pandas as pd
import torch

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables"
BUILD_DIR = "thesis_parts/latex_template/figures"
LAST_EVENT_DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_trace_showcase_data.json")
DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_prefix_evolution_showcase_data.json")
PNG_PATH = os.path.join(OUT_DIR, "loo_shapley_prefix_evolution_showcase.png")
# Exact filename results.tex actually cites (figures/prefix_evolution.png) -- this
# used to be a manual one-time rename with no re-sync path; promoted automatically
# now, same pattern as promote_aggregate_shapley_global_figures.py.
BUILD_PNG_PATH = os.path.join(BUILD_DIR, "prefix_evolution.png")

KPI_ORDER = [
    "Order Management / Orders -> PayOrder",
    "Order Management / Orders -> PackageDelivered",
    "Logistics / CustomerOrder -> Depart",
    "Logistics / CustomerOrder -> LoadToVehicle",
]

TOP_N = 8


def collect(kpi_label, database, cant, n_events):
    with open(LAST_EVENT_DATA_PATH) as f:
        last_event_data = json.load(f)
    if kpi_label not in last_event_data:
        raise SystemExit(f"{kpi_label!r} not found in {LAST_EVENT_DATA_PATH} -- run the "
                          f"node showcase's --collect for this KPI first.")
    order_id = last_event_data[kpi_label]['order_id']
    last_n_events = last_event_data[kpi_label]['n_events']
    if n_events >= last_n_events:
        raise SystemExit(f"--n-events {n_events} is not earlier than the last-event depth "
                          f"{last_n_events} for order_id={order_id}")
    print(f"Reusing order_id={order_id} from the node showcase for {kpi_label!r}, "
          f"early prefix n_events={n_events} (last event was {last_n_events})")

    e = exp.Explainer(database, cant)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    result = e.explain_trace_shapley(order_id, top_k=TOP_N, n_samples=100, n_events=n_events)

    nodes_csv = os.path.join(result['save_dir'], 'top_nodes_per_type.csv')
    nodes_df = pd.read_csv(nodes_csv)
    nodes_df['abs_shift'] = nodes_df['shift_hours'].abs()
    top = nodes_df.sort_values('abs_shift', ascending=False).head(TOP_N)

    prefix_graph = e._locate_test_graph(order_id, n_events)
    vp = e.kpi_viewpoint
    true_h = (prefix_graph[vp].y[0].item() * e.target_std.item() + e.target_mean.item()) / 3600.0

    entry = {
        'kpi_label': kpi_label,
        'order_id': order_id,
        'n_events': n_events,
        'true_hours': true_h,
        'predicted_hours': result['predicted_hours'],
        'rows': [
            {'label': f"{r.node_type}: {r.identifier}", 'signed_shift_hours': r.signed_shift_hours}
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


def _plot_panel(ax, entry, title_extra, xlim):
    rows = sorted(entry['rows'], key=lambda r: r['signed_shift_hours'])
    labels = [r['label'] for r in rows]
    values = [r['signed_shift_hours'] for r in rows]
    colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]

    ax.barh(range(len(labels)), values, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel("Signed Shapley shift (hours)")
    ax.set_xlim(xlim)
    ax.set_title(
        f"{title_extra}\nn_events={entry['n_events']}  true={entry['true_hours']:.1f}h  "
        f"pred={entry['predicted_hours']:.1f}h",
        fontsize=9)
    ax.grid(True, axis='x', alpha=0.3)


def render():
    with open(DATA_PATH) as f:
        early_data = json.load(f)
    with open(LAST_EVENT_DATA_PATH) as f:
        last_event_data = json.load(f)
    missing = [k for k in KPI_ORDER if k not in early_data]
    if missing:
        raise SystemExit(f"Missing collected early-prefix data for: {missing}. Run --collect first.")

    fig, axes = plt.subplots(4, 2, figsize=(14, 20))
    for row, kpi_label in enumerate(KPI_ORDER):
        early_entry = early_data[kpi_label]
        late_entry = last_event_data[kpi_label]

        both_vals = ([r['signed_shift_hours'] for r in early_entry['rows']] +
                     [r['signed_shift_hours'] for r in late_entry['rows']])
        span = max(abs(min(both_vals)), abs(max(both_vals))) * 1.1
        xlim = (-span, span)

        _plot_panel(axes[row, 0], early_entry, f"{kpi_label}\n(early prefix)", xlim)
        _plot_panel(axes[row, 1], late_entry, f"{kpi_label}\n(last event)", xlim)

    fig.suptitle("LOO-identified, Shapley-quantified explanation — same trace, early prefix "
                 "vs. last event\n(red = pushes prediction later, blue = pushes prediction "
                 "earlier)", fontsize=12)
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {PNG_PATH}")

    os.makedirs(BUILD_DIR, exist_ok=True)
    shutil.copy(PNG_PATH, BUILD_PNG_PATH)
    print(f"Promoted -> {BUILD_PNG_PATH}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect', metavar='KPI_LABEL')
    parser.add_argument('--database', choices=['order_management', 'logistics'])
    parser.add_argument('--cant', type=int)
    parser.add_argument('--n-events', type=int, dest='n_events')
    parser.add_argument('--render', action='store_true')
    args = parser.parse_args()

    if args.collect:
        if not args.database or not args.cant or args.n_events is None:
            raise SystemExit("--collect requires --database, --cant, and --n-events")
        collect(args.collect, args.database, args.cant, args.n_events)
    elif args.render:
        render()
    else:
        raise SystemExit("Pass --collect \"<KPI label>\" --database ... --cant ... --n-events ... or --render")


if __name__ == '__main__':
    main()
