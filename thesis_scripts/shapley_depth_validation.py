"""Validates de Leoni's intuition that Shapley-quantified element impact on the
model's prediction shrinks the further along a process execution is, by tracking
the magnitude of Shapley-value-quantified node importance across several prefix
depths of 2 distinct, real process executions.

Reuses the thesis's existing production single-trace method, explain_trace_shapley()
(explainer.py), at multiple n_events depths per trace -- no new explanation machinery,
just a new way of iterating an existing one. The 2 traces are the thesis's own already-
established, citable running examples (not freshly picked), for dataset diversity and a
deliberate short-vs-long-trace contrast:

- order 1801, Order Management / Orders -> PayOrder (12 total events)
- order 911, Logistics / CustomerOrder -> LoadToVehicle (88 total events)

Metric per depth: mean(|shift_hours|) over the top-8 ranked node importances returned
by explain_trace_shapley() at that depth (fixed N across depths for comparability, since
the raw candidate-set size returned varies slightly by depth).

This is a validation pass only -- results stage to thesis_parts/figures_tables/ but are
NOT promoted to thesis_parts/latex_template/figures/. Whether/how to cite this in the
thesis is a separate, later decision (see docs/archive/DELEONI_SHAPLEY_MAGNITUDE_DEPTH_HYPOTHESIS.md).

Usage: PYTHONPATH=. python3 thesis_scripts/shapley_depth_validation.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd
import torch
from scipy.stats import spearmanr

import explainer as exp

STAGING_DIR = "thesis_parts/figures_tables"
CSV_PATH = os.path.join(STAGING_DIR, "shapley_depth_validation.csv")
PNG_PATH = os.path.join(STAGING_DIR, "shapley_depth_validation.png")

TOP_N = 8
N_DEPTHS = 5

TRACES = [
    ("order_management", 2000, 1801, "Order Management / Orders → PayOrder"),
    ("logistics", 1000, 911, "Logistics / CustomerOrder → LoadToVehicle"),
]


def available_depths(e, order_id):
    """Every distinct n_events a test graph exists for this order_id, ascending --
    same scan _locate_test_graph() does internally, exposed here since n_events must
    match an exact available prefix length, not an arbitrary number."""
    vp = e.kpi_viewpoint
    depths = set()
    for g in e.test_data:
        if g[vp]['id'][0].item() != order_id:
            continue
        n = g['Events'].x.size(0) if 'Events' in g.node_types else 0
        depths.add(n)
    return sorted(depths)


def pick_depths(depths, n=N_DEPTHS):
    """Evenly-spaced-by-INDEX subset of an available-depths list (not evenly-spaced-
    by-value -- available depths aren't necessarily contiguous), always including the
    first and last."""
    if len(depths) <= n:
        return depths
    idxs = sorted({round(i * (len(depths) - 1) / (n - 1)) for i in range(n)})
    return [depths[i] for i in idxs]


def run_trace(database, cant, order_id, trace_label):
    e = exp.Explainer(database, cant)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()

    depths = available_depths(e, order_id)
    chosen = pick_depths(depths)
    max_n = max(depths)
    print(f"\n{trace_label} (order_id={order_id}): {len(depths)} available prefixes, "
          f"sampling {len(chosen)}: {chosen}")

    rows = []
    for n_events in chosen:
        try:
            result = e.explain_trace_shapley(order_id, top_k=TOP_N, n_samples=100,
                                              n_events=n_events)
        except Exception as ex:
            print(f"  [skipped] n_events={n_events}: {type(ex).__name__}: {ex}")
            continue
        top = result["node_importances"][:TOP_N]
        shifts_h = [shift / 3600.0 for _, _, shift, _, _ in top]
        mean_abs = sum(shifts_h) / len(shifts_h) if shifts_h else 0.0
        max_abs = max(shifts_h) if shifts_h else 0.0
        rows.append({
            "trace_label": trace_label, "order_id": order_id,
            "n_events": n_events, "depth_fraction": n_events / max_n,
            "mean_abs_shift_hours": mean_abs, "max_abs_shift_hours": max_abs,
            "predicted_hours": result["predicted_hours"], "n_candidates": len(top),
        })
        print(f"  n_events={n_events:>3}  predicted={result['predicted_hours']:.1f}h  "
              f"mean|shift| over top-{len(top)}={mean_abs:.2f}h  max|shift|={max_abs:.2f}h")
    return rows


def main():
    os.makedirs(STAGING_DIR, exist_ok=True)
    all_rows = []
    for database, cant, order_id, trace_label in TRACES:
        all_rows.extend(run_trace(database, cant, order_id, trace_label))

    df = pd.DataFrame(all_rows)
    df.to_csv(CSV_PATH, index=False)
    print(f"\nSaved {CSV_PATH} ({len(df)} rows)")

    fig, ax = plt.subplots(figsize=(8, 5))
    print("\nTrend per trace (Spearman rho between depth_fraction and mean_abs_shift_hours):")
    for (database, cant, order_id, trace_label), marker in zip(TRACES, ['o-', 's-']):
        sub = df[df["trace_label"] == trace_label].sort_values("depth_fraction")
        ax.plot(sub["depth_fraction"], sub["mean_abs_shift_hours"], marker,
                label=trace_label, markersize=6)
        if len(sub) >= 3:
            rho, p = spearmanr(sub["depth_fraction"], sub["mean_abs_shift_hours"])
            print(f"  {trace_label}: rho={rho:.3f}  p={p:.3f}  "
                  f"({'decreasing' if rho < 0 else 'increasing'} trend)")
        else:
            print(f"  {trace_label}: not enough usable depths for a correlation")

    ax.set_xlabel("Prefix depth (fraction of trace length)")
    ax.set_ylabel(f"Mean |Shapley shift| over top-{TOP_N} nodes [h]")
    ax.set_title("Shapley-quantified node impact vs. prefix depth")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(PNG_PATH, dpi=150)
    plt.close()
    print(f"Saved {PNG_PATH}")


if __name__ == '__main__':
    main()
