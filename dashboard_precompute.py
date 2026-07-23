"""Warms dashboard_cache.py's on-disk cache for a small, deterministic set of demo
orders per dataset (the 'loo' mode -- Shapley-based despite the name, see
dashboard.py's compute_local()). Run this once before a live demo/defense so the
dashboard never hits a cold-cache wait in front of an audience.

The dashboard itself works fine without this -- any order not in the cache is just
computed live on first view and cached from then on. This script only pre-warms a
curated subset ahead of time.

Usage: python3 dashboard_precompute.py
"""
import os

import explainer as exp
import dashboard_cache as dc

DATASETS = [('order_management', 2000), ('logistics', 1000)]
DEMO_N = 5  # first N last-event orders per dataset, same deterministic ordering
            # explain_aggregate() itself uses (last_event_graphs[:n])


def demo_order_ids(explainer, n):
    vp = explainer.kpi_viewpoint
    last_event_graphs = [g for g in explainer.test_data if g[vp]['last_event'][0].item()]
    return [int(g[vp]['id'][0].item()) for g in last_event_graphs[:n]]


def main():
    for database, cant in DATASETS:
        print(f"\n=== {database} (cant={cant}) ===")
        e = exp.Explainer(database, cant)
        order_ids = demo_order_ids(e, DEMO_N)
        print(f"Demo orders: {order_ids}")
        # Ties every cache entry this script writes to the exact checkpoint that produced
        # it -- see dashboard_cache.py's own docstring for why (a retrain must not leave
        # stale predictions silently matched against a newer checkpoint).
        checkpoint_fingerprint = int(os.path.getmtime(e.model_path))

        for order_id in order_ids:
            # No save_dir override -- let explain_trace_shapley() use its normal
            # default (files/explainer_outputs/{database}/order_{id}_shapley/), the
            # same stable, project-relative location every other explain_* call
            # already uses, so the PNG paths cached in the JSON stay valid across
            # sessions/machines instead of pointing into ephemeral /tmp.
            print(f"  order {order_id} -- loo...", end=" ", flush=True)
            try:
                _, cached = dc.get_or_compute(
                    database, cant, 'loo', order_id,
                    lambda oid=order_id: e.explain_trace_shapley(oid),
                    checkpoint_fingerprint=checkpoint_fingerprint,
                )
                print("cached" if cached else "computed")
            except Exception as ex:
                print(f"FAILED ({type(ex).__name__}: {ex})")

    print("\nDone. Cache root:", dc.CACHE_ROOT)


if __name__ == '__main__':
    main()
