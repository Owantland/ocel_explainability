# Viability check: replace LOO's hard zero-masking with GNNExplainer-style soft masking

## Question

Would replacing LOO's `x[idx] = 0.0` substitution with a continuous, GNNExplainer-style multiplicative
mask (`x[idx] = x[idx] * sigmoid(c)` for some scale `c`, rather than a hard 0/1 choice) be a viable,
worthwhile change to how LOO quantifies an element's impact?

**Scoping note, since "GNNExplainer's masking" has two separable parts**: GNNExplainer's mechanism is
(1) a continuous multiplicative gate, learned via (2) a 200-epoch gradient-descent optimization that
*jointly* fits one mask across every node in the graph at once. Part (2) can't be grafted onto LOO
without LOO stopping being LOO — a jointly-optimized, shared mask is a different *kind* of method
(collective attribution) from LOO's defining trait (independent, one-element-at-a-time, deterministic
perturbation). "LOO with a joint 200-epoch optimization" is, at that point, just GNNExplainer restated
— which is exactly why this project already treats the two as complementary
(`EXPLAINABILITY.md`: "GNNExplainer identifies, LOO quantifies") rather than one subsuming the other.
So what's actually testable, and tested below, is part (1) only: keep LOO's existing independent,
deterministic design, swap only the substitution value.

## Verdict: mechanically viable, confirmed empirically — and the current hard-zero design turns out to already be one exact point on this spectrum, not a different choice

Tested directly against the same real trace and node the NaN check used (order_management #1781,
`Items[0]`) rather than reasoned about abstractly. Note the baseline prediction differs from the
number on record in `LOO_NAN_MASKING_VIABILITY.md` (`920364.5` there vs. `1816691.5` here) — this
reflects legitimate checkpoint retraining that happened later in the same project (the weight_decay
fix, `num_layers` investigation, etc.), not an inconsistency; the checkpoint used for both checks is
whatever was current at the time, and the methodology comparison is what matters, not the absolute
prediction value.

```
baseline pred:                    1816691.5000
hard zero-masked Items[0] pred:   1813537.1250   (current behavior, shift = 3154.375)

       c   sigmoid(c)             pred  shift_from_baseline
   -20.0     0.000000     1813537.1250            3154.3750
    -5.0     0.006693     1813555.0000            3136.5000
    -2.0     0.119203     1813865.1250            2826.3750
    -1.0     0.268941     1814319.2500            2372.2500
     0.0     0.500000     1815028.0000            1663.5000
     1.0     0.731059     1815783.5000             908.0000
     2.0     0.880797     1816280.5000             411.0000
     5.0     0.993307     1816667.7500              23.7500
    20.0     1.000000     1816691.5000               0.0000

Endpoint check: hard zero pred = 1813537.1250, soft c=-20 pred = 1813537.1250, difference = 0.0
```

Two things confirmed directly, not assumed:

1. **No NaN-style catastrophic failure at any point on the curve.** Every attenuation level produces a
   finite, sensible prediction — unlike the NaN check, where a single masked node poisoned the *entire*
   prediction via `HGTConv`'s attention-weighted aggregation (`weight * NaN = NaN`, propagating
   unboundedly). Soft masking is ordinary multiplication by a value in `(0, 1)`, which has no such
   propagation hazard — `HGTConv` (or any linear/attention layer downstream) handles a scaled-down
   feature vector exactly like it handles any other real-valued input, because that's all it is.
2. **The endpoint is exact, not approximate.** `sigmoid(-20) ≈ 1.13e-9`, small enough that
   `x * sigmoid(-20)` is numerically indistinguishable from `x * 0` at float32 precision — the two
   predictions match to `0.0` difference. This directly confirms
   `LOO_NAN_MASKING_VIABILITY.md`'s own claim, made in passing at the time
   ("zero/mean-substitution... is also what GNNExplainer's own soft-masking... effectively assume"),
   which hadn't been checked empirically until now: **the current hard-zero design already *is* the
   fully-suppressed limit of a GNNExplainer-style soft mask.** There's no hidden difference in kind
   between "set to 0" and "GNNExplainer-style mask this out completely" — they're the same operation.

## What the intermediate points on the curve show

The curve from full-presence (`c=20`, shift≈0) to full-suppression (`c=-20`, shift≈3154) is smooth and
monotonic, but **mildly non-linear**, not a straight line: at `scale=0.5` (the curve's midpoint by
mask strength), the shift is `1663.5` — about 5–6% higher than the `1577.2` a straight-line
interpolation between the two endpoints would predict. The deviation is modest for this particular
(node, feature) pair, not dramatic, but it's real and consistent with the model's nonlinear
activations (`PReLU` inside `HGTConv`) rather than a purely additive/linear feature pathway. This is
the concrete answer to "does a soft-mask sweep reveal something a single before/after point can't":
yes, in principle (a linear response would make intermediate points redundant with the two endpoints;
this one isn't perfectly linear) — but the effect size seen here is small enough that it's a genuine,
minor addition to what's already known, not a discovery that changes which elements look important.

## Cost of adding this as a real capability, quantified against the same trace

`reg_explanation()` (`explainer.py:319`) already does one forward pass per node instance and one per
edge, exhaustively, every time `explain_trace()` runs. For order #1781 specifically: **21 node LOO
targets, 112 edge LOO targets** (edges dominate the count more than 5:1). Soft-mask sweeping only
applies to the node/feature-value substitution path — edge LOO already works by removing the edge
from `edge_index` directly (a structural deletion, not a value substitution), so it's untouched by
this change either way, same as it was unaffected by the NaN check. That means the realistic added
cost is smaller than the plan's own a priori estimate: a 5-point node sweep brings the trace's total
forward-pass count from 133 to 217 (**1.6x**), a 7-point sweep to 259 (**1.9x**) — not the "5-10x"
naively expected from "N points instead of 1" on nodes alone, because edges (which don't get swept)
make up the majority of the exhaustive sweep's cost already. Applied to `explain_aggregate()`'s
default `n_traces=50`, the same ~1.6–1.9x multiplier would apply uniformly — still comfortably cheap
in absolute terms (each forward pass is sub-second; nowhere near GNNExplainer's own per-trace 200-epoch
optimization cost, which remains the actually expensive method in this pipeline).

## Recommendation

**Mechanically viable, and cheap enough to build for real** — this is the opposite conclusion from the
NaN check, which was a hard no. But two things are worth weighing before doing so:

1. **The current design is already the correct limit**, not an arbitrary stand-in — a single-point
   "is it there or not" LOO measurement at `sigmoid(c)=0` is exactly what GNNExplainer-style full
   suppression already means. Switching the *default* single-point measurement to some other fixed
   `sigmoid(c)` value (say, 0.5) would trade a well-motivated, well-understood endpoint for an
   arbitrary intermediate one, with no clear benefit — this would be a strictly worse default, not an
   improvement, so a straight value swap is not recommended.
2. **A sweep/curve mode is the one genuinely useful addition**, but its value depends on whether the
   mild non-linearity seen here (a ~5-6% deviation from linear at the midpoint) is representative or an
   outlier — this was checked on exactly one (node, feature) pair. If it generalizes as a real, if
   modest, pattern across many nodes/traces, a sweep mode would let LOO additionally report *how
   suddenly* an element's influence appears as it's reintroduced (a shape/curvature question LOO's
   current single-point design structurally can't answer), complementing rather than replacing the
   existing hours-denominated shift. If it turns out most nodes respond near-linearly, the sweep
   mode's main value would just be a (cheap, ~1.6-1.9x) sanity check that nothing is behaving
   pathologically — worth having, but a minor addition, not a headline capability.

**Suggested scope if pursued**: an *additional*, opt-in sweep mode (e.g. `reg_feature_importance_for_node_in_graph(..., sweep=True)` returning a curve instead of/alongside the single shift), not a
replacement of the existing single-point measurement — the single point at `sigmoid(c)=0` stays the
right default precisely because it's the well-defined, already-validated "fully removed" endpoint this
whole check confirms it to be.
