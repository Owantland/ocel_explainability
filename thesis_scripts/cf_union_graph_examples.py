"""Counterfactual union-graph examples, one per KPI, all with 4-5 event prefixes.

One cf_union_<kpi>.png per analysed KPI, all in thesis_parts/figures_tables/cf_union_examples/.
Both the query prefix and its counterfactual have 4 or 5 Events, so the five examples are
comparable in size and every node can be labelled.

Selection is deterministic: test-set ids in ascending order; for each id with a 4-event
prefix (then a 5-event one), find_counterfactuals(n_events=...) is run and its candidates
with 4-5 events (and a predicted-time gap of >= 10% of the query's prediction, at least
1 h) are scanned in dissimilarity order. The first pair whose union has nodes
both only in the query and only in the CF is the example (a structurally identical pair
would draw all-grey and illustrate nothing); if none turns up within MAX_QUERIES queries,
the first in-range pair is used instead.
Each pick is recorded in cf_union_examples/selection.csv (one row per KPI, replaced on rerun).

--kpi runs against whatever config.yml and the graph caches CURRENTLY point at (it checks
task_id and exits on a mismatch), so Depart and PackageDelivered need their caches swapped
in first, as for loo_shapley_trace_showcase.py.

Usage (from the repo root):
    PYTHONPATH=. python3 thesis_scripts/cf_union_graph_examples.py --kpi om_payorder
    PYTHONPATH=. python3 thesis_scripts/cf_union_graph_examples.py --kpi logistics_loadtovehicle --min-gap-share 0
"""
import argparse
import os

import pandas as pd
import torch

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables/cf_union_examples"
SELECTION_CSV = os.path.join(OUT_DIR, "selection.csv")
EVENT_RANGE = (4, 5)
MAX_QUERIES = 200
# A CF must move the prediction by at least this share of the query's own prediction
# (floor 1 h); otherwise the closest "lower" trace can sit a fraction of an hour below.
# --min-gap-share overrides the share: LoadToVehicle's predictions at 2-7 events span
# only ~319-324 h, so no pair there can clear 10% and it runs with the 1 h floor alone.
MIN_GAP_SHARE, MIN_GAP_FLOOR_H = 0.10, 1.0

# slug -> (database, cant, task_id)
KPIS = {
    "om_payorder": ("order_management", 2000, "TimeFrom_Orders_to_PayOrder"),
    "om_packagedelivered": ("order_management", 2000, "TimeFrom_Orders_to_PackageDelivered"),
    "logistics_depart": ("logistics", 1000, "TimeFrom_CustomerOrder_to_Depart"),
    "logistics_loadtovehicle": ("logistics", 1000, "TimeFrom_CustomerOrder_to_LoadToVehicle"),
    "iot_evaluatetruckexit": ("cargo_pickup_iot", 2000, "TimeFrom_Pickupplan_to_EvaluateTruckExit"),
}


def _n_events(g):
    return g['Events'].x.size(0) if 'Events' in g.node_types else 0


def _shows_both_sides(e, oid, n, r):
    query_graph = e._locate_test_graph(oid, n)
    id_map_q = e._decode_all_identifiers(query_graph, oid, n)
    id_map_cf = e._decode_all_identifiers(r['graph'], r['order_id'], r['n_events'])
    event_pairs, _ = e._align_cf_events(query_graph, r['graph'])
    corr = e._cf_correspondence(query_graph, r['graph'], id_map_q, id_map_cf,
                                event_pairs=event_pairs, merge_seeds=True, match_by_role=True)
    return bool(corr['query_only']) and bool(corr['cf_only'])


def _memoize_predictions(e):
    # find_counterfactuals() re-predicts its whole depth-matched pool for every query;
    # the test graphs never change, so each one only needs predicting once.
    predict, cache = e._predict_value_for_graph, {}

    def cached(graph, object_idx, perturbed_graph=None):
        if perturbed_graph is not None:
            return predict(graph, object_idx, perturbed_graph)
        key = (id(graph), object_idx)
        if key not in cache:
            cache[key] = predict(graph, object_idx)
        return cache[key]
    e._predict_value_for_graph = cached


def find_example(e, min_gap_share):
    """(query id, n_events, CF result) with 4-5 events on both sides; see the docstring."""
    _memoize_predictions(e)
    vp = e.kpi_viewpoint
    depths = {}
    for g in e.test_data:
        depths.setdefault(int(g[vp]['id'][0].item()), set()).add(_n_events(g))

    tried, fallback = 0, None
    for oid in sorted(depths):
        for n in EVENT_RANGE:
            if n not in depths[oid]:
                continue
            tried += 1
            print(f"  query {tried}: #{oid} ({n} events)", flush=True)
            query_h = e._predict_value_for_graph(e._locate_test_graph(oid, n), 0) / 3600.0
            min_gap = max(MIN_GAP_FLOOR_H, min_gap_share * abs(query_h))
            results = e.find_counterfactuals(oid, n_results=50, n_events=n,
                                             min_gap_hours=min_gap)
            in_range = [r for r in results if EVENT_RANGE[0] <= r['n_events'] <= EVENT_RANGE[1]]
            for r in in_range:
                if _shows_both_sides(e, oid, n, r):
                    return oid, n, r, min_gap
            if in_range and fallback is None:
                fallback = (oid, n, in_range[0], min_gap)
            if tried >= MAX_QUERIES:
                return fallback
    return fallback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kpi", required=True, choices=list(KPIS))
    parser.add_argument("--min-gap-share", type=float, default=MIN_GAP_SHARE,
                        help="minimum CF prediction gap as a share of the query's prediction "
                             f"(floor {MIN_GAP_FLOOR_H:g} h)")
    args = parser.parse_args()
    database, cant, task_id = KPIS[args.kpi]

    e = exp.Explainer(database, cant)
    if e.task_id != task_id:
        raise SystemExit(f"config.yml points at {e.task_id}, expected {task_id}")
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()

    found = find_example(e, args.min_gap_share)
    if found is None:
        raise SystemExit(f"{args.kpi}: no query/CF pair with {EVENT_RANGE} events found.")
    oid, n, best, min_gap = found

    query_graph = e._locate_test_graph(oid, n)
    query_hours = e._predict_value_for_graph(query_graph, 0) / 3600.0
    id_map_q = e._decode_all_identifiers(query_graph, oid, n)
    id_map_cf = e._decode_all_identifiers(best['graph'], best['order_id'], best['n_events'])

    os.makedirs(OUT_DIR, exist_ok=True)
    filename = f"cf_union_{args.kpi}.png"
    e._plot_cf_union_graph(query_graph, best['graph'], oid, best['order_id'], query_hours,
                           best['predicted_hours'], OUT_DIR, id_map_q=id_map_q,
                           id_map_cf=id_map_cf, filename=filename)

    row = {'kpi': args.kpi, 'task_id': task_id, 'query_id': oid, 'query_events': n,
           'query_pred_h': round(query_hours, 2), 'cf_id': best['order_id'],
           'cf_events': best['n_events'], 'cf_pred_h': round(best['predicted_hours'], 2),
           'dissimilarity': round(best['dissimilarity'], 4), 'min_gap_h': round(min_gap, 2),
           'figure': filename}
    print(row)
    sel = pd.read_csv(SELECTION_CSV) if os.path.exists(SELECTION_CSV) else pd.DataFrame()
    if not sel.empty:
        sel = sel[sel['kpi'] != args.kpi]
    sel = pd.concat([sel, pd.DataFrame([row])], ignore_index=True)
    sel['order'] = sel['kpi'].map(list(KPIS).index)
    sel.sort_values('order').drop(columns='order').to_csv(SELECTION_CSV, index=False)


if __name__ == '__main__':
    main()
