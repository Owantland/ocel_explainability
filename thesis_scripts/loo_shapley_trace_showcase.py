"""Single-trace LOO+Shapley showcase, one worked example per KPI (4 total):
Order Management / Orders -> PayOrder, Order Management / Orders -> PackageDelivered,
Logistics / CustomerOrder -> Depart, Logistics / CustomerOrder -> LoadToVehicle.

For each KPI, picks a representative "slow" last-event trace (same 80th-percentile-by-
true_h convention run_pipeline.py's own 'explain' stage uses for its fast/slow examples),
runs explainer.py's explain_trace_shapley() (LOO identifies candidates, Shapley
requantifies them -- the production pipeline per project memory), and renders a
horizontal tornado-style bar chart of the top contributing nodes' signed shift in hours
(positive = pushes the prediction later, negative = pushes it earlier).

Two modes, since PackageDelivered/LoadToVehicle need config.yml pointed at a different
kpi_event than PayOrder/Depart (the two KPIs whose graph caches are the live default):
  --collect <KPI label>   Runs explain_trace_shapley() against whatever config.yml is
                           CURRENTLY pointed at, appends the result to the shared JSON.
  --render                 Builds the final 2x2 PNG from all 4 collected results.

Usage:
    python3 loo_shapley_trace_showcase.py --collect "Order Management / Orders -> PayOrder"
    python3 loo_shapley_trace_showcase.py --collect "Logistics / CustomerOrder -> LoadToVehicle"
    # (switch config.yml, regenerate graphs for PackageDelivered/Depart as needed, then:)
    python3 loo_shapley_trace_showcase.py --collect "Order Management / Orders -> PackageDelivered"
    python3 loo_shapley_trace_showcase.py --collect "Logistics / CustomerOrder -> Depart"
    python3 loo_shapley_trace_showcase.py --render
"""
import argparse
import json
import os

import matplotlib.pyplot as plt
import pandas as pd
import torch

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables"
DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_trace_showcase_data.json")
PNG_PATH = os.path.join(OUT_DIR, "loo_shapley_trace_showcase.png")

# Fixed panel order regardless of collection order, matching every other 4-KPI figure
# this thesis uses ("[Dataset] / [Object] -> [KPI Event]" convention).
KPI_ORDER = [
    "Order Management / Orders -> PayOrder",
    "Order Management / Orders -> PackageDelivered",
    "Logistics / CustomerOrder -> Depart",
    "Logistics / CustomerOrder -> LoadToVehicle",
]

TOP_N = 8


def _predict_all(e):
    vp = e.kpi_viewpoint
    records = []
    with torch.no_grad():
        for g in e.test_data:
            pred_norm = e.model(g.x_dict, g.edge_index_dict)[0].item()
            pred_h = (pred_norm * e.target_std.item() + e.target_mean.item()) / 3600.0
            true_h = (g[vp].y[0].item() * e.target_std.item() + e.target_mean.item()) / 3600.0
            records.append({
                'order_id': int(g[vp].id[0].item()),
                'true_h': true_h,
                'pred_h': pred_h,
                'last_event': bool(g[vp].last_event[0].item()),
            })
    return pd.DataFrame(records)


def collect(kpi_label, database, cant):
    e = exp.Explainer(database, cant)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    df = _predict_all(e)
    last = df[df['last_event']].sort_values('true_h')
    slow_id = int(last.iloc[-len(last) // 5]['order_id'])
    true_h = float(last[last['order_id'] == slow_id]['true_h'].iloc[0])
    print(f"Selected slow trace: order_id={slow_id}, true_h={true_h:.1f}h")

    result = e.explain_trace_shapley(slow_id, top_k=5, n_samples=100)

    nodes_csv = os.path.join(result['save_dir'], 'top_nodes_per_type.csv')
    nodes_df = pd.read_csv(nodes_csv)
    nodes_df['abs_shift'] = nodes_df['shift_hours'].abs()
    top = nodes_df.sort_values('abs_shift', ascending=False).head(TOP_N)

    entry = {
        'kpi_label': kpi_label,
        'order_id': slow_id,
        'true_hours': true_h,
        'predicted_hours': result['predicted_hours'],
        'n_events': result['n_events'],
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
        rows = sorted(entry['rows'], key=lambda r: r['signed_shift_hours'])
        labels = [r['label'] for r in rows]
        values = [r['signed_shift_hours'] for r in rows]
        colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]

        ax.barh(range(len(labels)), values, color=colors)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=9)
        ax.axvline(0, color='black', linewidth=0.8)
        ax.set_xlabel("Signed Shapley shift (hours)")
        ax.set_title(
            f"{kpi_label}\norder_id={entry['order_id']}  "
            f"true={entry['true_hours']:.1f}h  pred={entry['predicted_hours']:.1f}h",
            fontsize=10)
        ax.grid(True, axis='x', alpha=0.3)

    fig.suptitle("LOO-identified, Shapley-quantified single-trace explanation — one worked "
                 "example per KPI\n(red = pushes prediction later, blue = pushes prediction earlier)",
                 fontsize=12)
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
