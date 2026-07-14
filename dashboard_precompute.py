"""Warms dashboard_cache.py's on-disk cache for a small, deterministic set of demo
orders per dataset, in both explanation modes (exhaustive LOO and GNNExplainer-
primary). Run this once before a live demo/defense so the dashboard never hits a
cold-cache GNNExplainer wait (~200-epoch optimization) in front of an audience.

The dashboard itself works fine without this -- any order not in the cache is just
computed live on first view (fast for LOO, slow for GNNExplainer-primary) and
cached from then on. This script only pre-warms a curated subset ahead of time.

Usage: python3 dashboard_precompute.py
"""
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

        for order_id in order_ids:
            # No save_dir override -- let both functions use their normal default
            # (files/explainer_outputs/{database}/order_{id}[_gnnprimary]/), the
            # same stable, project-relative location every other explain_* call
            # already uses, so the PNG paths cached in the JSON stay valid across
            # sessions/machines instead of pointing into ephemeral /tmp.
            print(f"  order {order_id} -- loo...", end=" ", flush=True)
            try:
                _, cached = dc.get_or_compute(
                    database, cant, 'loo', order_id,
                    lambda oid=order_id: e.explain_trace(oid),
                )
                print("cached" if cached else "computed")
            except Exception as ex:
                print(f"FAILED ({type(ex).__name__}: {ex})")

            print(f"  order {order_id} -- gnn_primary...", end=" ", flush=True)
            try:
                _, cached = dc.get_or_compute(
                    database, cant, 'gnn_primary', order_id,
                    lambda oid=order_id: e.explain_gnn_primary(oid),
                )
                print("cached" if cached else "computed")
            except ValueError as ex:
                # explain_gnn_primary()'s own guard against a legitimately-empty
                # edge type (PyG's GNNExplainer can't handle it) -- expected for
                # some orders, not every order in the test set needs to support
                # every mode. Skip this order for this mode only; its LOO cache
                # entry above is unaffected.
                print(f"skipped ({ex})")

    print("\nDone. Cache root:", dc.CACHE_ROOT)


if __name__ == '__main__':
    main()
