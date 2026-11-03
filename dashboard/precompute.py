"""Warms dashboard/cache.py's on-disk cache with single-trace perturbation results
(explain_trace_shapley) for the first DEMO_N last-event test traces of each KPI. Run once
before a live demo so the dashboard never hits a cold-cache wait; the dashboard itself works
without it (anything uncached is computed on first view and cached from then on).

Usage (from repo root):
    PYTHONPATH=. python3 dashboard/precompute.py                 # all five KPIs
    PYTHONPATH=. python3 dashboard/precompute.py --kpi om_payorder
"""
import argparse
import os

from dashboard import cache as dc
from dashboard import kpis


def shapley_save_dir(explainer, order_id, n_events=None):
    suffix = f"_ev{n_events}" if n_events is not None else ""
    return os.path.join(kpis.dashboard_dir(explainer), f"order_{order_id}{suffix}_shapley")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--kpi', choices=list(kpis.KPIS), action='append',
                    help="KPI slug(s) to warm; default: all")
    args = ap.parse_args()

    for slug in args.kpi or list(kpis.KPIS):
        print(f"\n=== {kpis.KPIS[slug]['label']} ===")
        e = kpis.make_explainer(slug)
        scope = kpis.cache_scope(e)
        checkpoint_fingerprint = int(os.path.getmtime(e.model_path))
        order_ids = kpis.demo_order_ids(e)
        print(f"Demo traces: {order_ids}")
        for order_id in order_ids:
            print(f"  {order_id} ...", end=" ", flush=True)
            try:
                _, cached = dc.get_or_compute(
                    scope, 'perturbation', order_id,
                    lambda oid=order_id: e.explain_trace_shapley(
                        oid, save_dir=shapley_save_dir(e, oid)),
                    checkpoint_fingerprint=checkpoint_fingerprint,
                )
                print("cached" if cached else "computed")
            except Exception as ex:
                print(f"FAILED ({type(ex).__name__}: {ex})")

    print("\nDone. Cache root:", dc.CACHE_ROOT)


if __name__ == '__main__':
    main()
