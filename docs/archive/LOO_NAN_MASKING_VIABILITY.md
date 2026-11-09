# Viability check: replace LOO's zero-masking with NaN-masking

## Question

Would setting a masked node's feature vector to `NaN` (instead of `0.0`) work as a drop-in change to
how LOO quantifies an element's impact? How would it affect the explainability layer, and how big a
change would it be?

## Verdict: not viable as a value substitution — confirmed empirically, not just theoretically

This was tested directly against a real trace (order_management #1781) rather than reasoned about
abstractly:

```
baseline pred:                       920364.5
zero-masked Items[0] pred:           920292.0625   (current behavior — a small, local shift)
NaN-masked  Items[0] pred:           nan            (the ENTIRE prediction, not just a shift)
```

Masking a single non-seed node's features to `NaN` doesn't produce "a very large shift" — it makes
the model's output for the seed node **undefined**, and this happens every single time, regardless of
which node is masked.

## Why this happens (root cause)

`model_classes/HGT.py`'s `forward()` is: per-node-type `Linear` projection → `PReLU` → stacked
`HGTConv` message-passing layers (each attention-weighted, aggregating neighbor representations into
each node's updated representation) → final `Linear` read out from the viewpoint node type.

- The `Linear` projection step only poisons the *masked node's own* row (a linear layer processes
  each row independently, so `NaN` in one row of `Items.x` doesn't leak into other `Items` rows at
  this step).
- But `HGTConv` is exactly the mechanism LOO relies on to detect "impact" — it aggregates the masked
  node's (now-`NaN`) hidden representation into its neighbors' updated representations via attention-
  weighted summation. PyTorch has no default NaN-skipping semantics: `weight * NaN = NaN`, and
  `NaN + anything = NaN`. Once the seed's own representation picks up a single `NaN` contribution
  from *any* neighbor, it stays `NaN` through every subsequent layer, and the final prediction for
  that node is `NaN` — not a large finite shift, an undefined one.

This is not a corner case: **any** masked non-seed node connected (even indirectly, within
`num_layers` hops) to the seed will poison the seed's prediction this way. Since almost every node in
a trace subgraph is within reach of the viewpoint node (that's the whole point of the graph
encoding), this would poison the LOO shift for nearly every node in every trace.

## Downstream impact on the explainability layer if this shipped anyway

The failure mode is **silent, not a crash** — which makes it worse than an error:
- `signed_shift = baseline_value - pred` → `NaN`
- `shift = abs(signed_shift)` → `NaN`
- `large_shift = shift > self.target_std.item()` → `False` (NaN comparisons are always `False` in
  Python/NumPy/PyTorch — the "large shift" flag would silently and incorrectly read as "not large"
  for every single masked element)
- `node_importances.sort(key=lambda t: t[2], reverse=True)` → undefined/unstable ordering (NaN has no
  consistent sort position), so the "top-k most important" ranking would become meaningless noise,
  not an error you'd notice from a traceback.

This exact zero-masking pattern is used at **five call sites**, all of which would break identically:
`_loo_shift_for_nodes()` (node-level LOO), `reg_feature_importance_for_node_in_graph()`
(feature-dimension-level LOO), `evaluate_explanation_quality()`'s two constructions (Fidelity+'s
"remove explanation" and Fidelity−'s "keep only explanation"), and `_plot_cf_event_type_diff()`'s
group-level activity-type ablation used in counterfactual output. `explain_gnn_primary()` would break
transitively (it calls `_loo_shift_for_nodes()`). The **edge**-deletion sweep in `reg_explanation()`
is unaffected — it removes edges from `edge_index` directly rather than substituting a value, so it
was never exposed to this problem in the first place.

## What a genuinely viable alternative would require

If the underlying motivation is "zero/mean-substitution isn't a true removal, it's just recentering
to the population average" — that's a fair critique, but the only architecturally sound way to
express real removal is **structural node deletion**: drop the node's row from `x` and re-index every
`edge_type`'s `edge_index` so no edge references a since-shifted position. This is exactly what the
existing edge-deletion sweep already does for edges, just not yet for nodes.

This would be a materially bigger change than the current one-line `x[idx] = 0.0`:
- A new graph-surgery primitive (remove a node, renumber all affected edges across every edge type)
  replacing the substitution at all five call sites above.
- Care needed to confirm `object_idx` (the seed's own index, always 0 in every subgraph observed —
  every trace subgraph has exactly one viewpoint-type node) stays stable, which it would, since the
  seed itself is never masked.

But it would also **not cleanly solve a problem** — it would trade one open question for a closely
related one already on record. `AUDIT.md`/`EXPLAINABILITY.md` §6 already flag, as a still-open
design critique, that node-feature LOO and edge LOO measure qualitatively different kinds of
intervention (feature recentering vs. topology change) but get reported on the same `|Δpred|` scale.
True node deletion would remove a node's features **and** its edges in the same perturbation,
conflating those two effects into one number for nodes too — working *against* the project's own
stated goal of eventually separating structural from feature-level importance, not toward it.

## Recommendation

**Don't make this change.** Zero/mean-substitution on z-normalized features is the standard
perturbation-explainability convention (also what GNNExplainer's own soft-masking and Captum's
attribution methods effectively assume) specifically because it's a well-defined point in feature
space that message-passing architectures can propagate cleanly — this isn't an oversight in the
current design, it's the reason it works at all. If node-level "true removal" is wanted later for a
different reason (e.g. a cleaner ablation semantics), it should be scoped as its own, separate
project — a real graph-surgery feature, not a one-line value swap — and it comes with the
structural/feature-conflation trade-off above, which should be weighed on its own merits rather than
treated as a strict improvement.
