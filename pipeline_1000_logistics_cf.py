"""
Counterfactual explanation on a non-final-prefix trace, logistics / TransportDocument_to_Depart.
Exercises explain_counterfactual()'s n_events parameter -- an existing code path never
previously called by any script -- to explain an in-process trace (a mid-case prefix)
rather than only last-event traces.
"""
import explainer as exp

DATABASE = 'logistics'
CANT = 1000

e = exp.Explainer(DATABASE, CANT)
vp = e.viewpoint_object

# Group prefixes by case, tracking each case's recorded depths (Events node count).
depths_by_case = {}
for g in e.test_data:
    oid = int(g[vp]['id'][0].item())
    n_ev = g['Events'].x.size(0) if 'Events' in g.node_types else 0
    depths_by_case.setdefault(oid, []).append(n_ev)

# Pick a case whose full (last-event) trace length is close to the median across all
# test cases, so the example is representative rather than a length outlier; then
# explain its mid-trace prefix -- neither the trivial first event nor the final one.
max_depths = {oid: max(d) for oid, d in depths_by_case.items()}
median_depth = sorted(max_depths.values())[len(max_depths) // 2]
candidate_id = min(max_depths, key=lambda oid: abs(max_depths[oid] - median_depth))
full_depth = max_depths[candidate_id]
mid_depth = full_depth // 2

print(f"Selected TransportDocument #{candidate_id}: {full_depth} events at completion "
      f"(median case length across {len(max_depths)} test cases: {median_depth}).")
print(f"Explaining the mid-trace prefix at {mid_depth} events "
      f"(in-process, not the case's final {full_depth}-event prefix).\n")

e.explain_counterfactual(candidate_id, n_events=mid_depth, target_band='opposite', n_results=3)
