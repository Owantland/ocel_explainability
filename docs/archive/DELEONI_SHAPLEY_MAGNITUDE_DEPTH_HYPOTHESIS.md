# Validation: does Shapley-quantified element impact shrink further along a process?

## Hypothesis

De Leoni's intuition: the magnitude of Shapley-quantified impact of individual elements on the
model's prediction decreases as a process execution progresses (more events seen → smaller
per-element impacts).

## Background signal already on disk (population-level, not a substitute for this check)

`explain_loo_by_depth()`'s existing output (`files/explainer_outputs/{order_management,
logistics}/aggregate/loo_depth_importance.csv`, LOO-shift not Shapley-quantified, pooled across
up to 200 test prefixes per dataset rather than tracking individual traces) shows:

| Dataset | 1-3 events | 4-6 events | 7-9 events | 10+ events |
|---|---|---|---|---|
| order_management | 3.69h | 2.20h | 2.32h | 1.87h |
| logistics | 0.012h | 0.006h | 0.004h | 0.482h |

order_management is mostly decreasing; logistics decreases then spikes sharply in the deepest
bin. Mixed at the population level — motivating a focused, real-trace check.

## Method

For 2 of the thesis's own already-established, citable running-example traces — order **1801**
(Order Management / Orders→PayOrder, 12 total events) and order **911** (Logistics /
CustomerOrder→LoadToVehicle, 88 total events) — `thesis_scripts/shapley_depth_validation.py`
called the production method `Explainer.explain_trace_shapley(order_id, top_k=8, n_samples=100,
n_events=<depth>)` at 5 evenly-spaced-by-index available prefix depths per trace, and recorded
`mean(|Shapley shift_hours|)` over the top-8 ranked node importances at each depth. Read-only
against the deployed checkpoints, no retraining. Full numbers: `thesis_parts/figures_tables/
shapley_depth_validation.csv`; plot: `shapley_depth_validation.png` (same directory, staging
only — not promoted to the LaTeX build, see "What's left" below).

## Results

**Order Management / PayOrder (order 1801):**

| depth fraction | n_events | mean \|shift\| (h) | max \|shift\| (h) | predicted (h) |
|---|---|---|---|---|
| 0.08 | 1 | 7.09 | 19.07 | 267.4 |
| 0.33 | 4 | 5.64 | 10.71 | 211.9 |
| 0.58 | 7 | 9.93 | 52.17 | 147.5 |
| 0.75 | 9 | 9.86 | 51.18 | 124.7 |
| 1.00 | 12 | 7.00 | 18.83 | 125.8 |

Not monotonic: dips early (7.09→5.64), **rises to a mid-trace peak** (9.93, 9.86 at 58-75%
depth), then falls back at the very end (7.00) — a hump shape, not a decline. Spearman
rho = **+0.100** (p = 0.873, n=5) — no detectable trend, and what little signal exists points
the wrong direction.

**Logistics / LoadToVehicle (order 911):**

| depth fraction | n_events | mean \|shift\| (h) | max \|shift\| (h) | predicted (h) | n candidates |
|---|---|---|---|---|---|
| 0.01 | 1 | 0.46 | 0.46 | 303.0 | 1 (caveat: only 1 node existed yet) |
| 0.26 | 23 | 0.20 | 0.80 | 319.9 | 8 |
| 0.51 | 45 | 0.20 | 0.78 | 319.9 | 8 |
| 0.75 | 66 | 0.21 | 0.82 | 320.0 | 8 |
| 1.00 | 88 | **1.11** | **4.31** | **-13.5** | 8 |

Flat and low through the middle (~0.20h, 26-75% depth), then a sharp **5x spike at the very last
event** (1.11h) — the opposite of a decline. Excluding the degenerate n=1 first point (only 1
node candidate existed, not comparable to the top-8 means elsewhere), the remaining 4 points are
essentially monotonically *increasing* (0.20 → 0.20 → 0.21 → 1.11). Spearman rho = **+0.400**
(p = 0.505, n=5) — also not significant, also the wrong direction. Note the last-event prediction
itself is anomalous (-13.5 predicted hours, a physically impossible negative remaining time),
which likely explains the spike — the model is visibly unstable right at this trace's last event,
inflating every element's apparent Shapley contribution there.

## Verdict: **not confirmed** on these 2 traces

Neither trace shows the hypothesized decrease. Both show a non-monotonic pattern with the
*largest* impacts appearing mid-to-late in the trace, not shrinking toward the end. Both
Spearman correlations are small, positive (the opposite sign from the hypothesis), and far from
significant at n=5 — so this isn't strong evidence *for* an increasing trend either, just clear
evidence *against* the hypothesized decrease on both traces tested. The Logistics trace's
last-event spike is entangled with an anomalous, out-of-distribution prediction for that specific
point, which is itself worth flagging separately (a reproducibility/stability concern, not a
property of "impact shrinking," though it is the opposite of de Leoni's intuition in that specific
data point too).

This is consistent with, not contradicted by, the mixed population-level background signal above
(order_management's population trend did decrease, but these 2 single-trace checks show that
isn't a reliable per-trace pattern — a population average decreasing doesn't imply every, or even
most, individual traces decrease monotonically).

## What's left

- **No thesis text changes made in this pass** — this was a validation-only task. Whether/how to
  report this finding (e.g., as a "tested and not confirmed" note, likely relevant context for
  the existing `prefix_evolution.png`/Section 5.2 discussion) is a separate, later decision.
- n=5 points per trace is enough to see these traces don't show a *clean* decrease, but too few
  for a confident correlation estimate either way — if this hypothesis needs a firmer statistical
  answer (rather than "not confirmed on 2 traces"), a larger multi-trace version of this same
  script (more traces, same method) would be the natural next step, distinct from
  `explain_loo_by_depth()`'s existing population-pooled (not per-trace, not Shapley) approach.

## Addendum: order 911's negative last-event prediction investigated — not an anomaly

Follow-up investigation into the -13.5h prediction flagged above. **Correction to the framing
above**: this is not an isolated, order-911-specific anomaly — it's one instance of a systematic,
dataset-wide miscalibration affecting nearly every LoadToVehicle last-event prediction.

**Ground truth at last-event is small but not zero.** `last_event=True` marks the deepest
recorded prefix of a trace (`hetero_graphs.py:168`); `process_generation.py`'s KPI construction
truncates each trace's event log at the KPI event's own timestamp, so remaining time is always
small there, but not exactly 0 (ties/other events can share the final slot). Measured directly
across all 140 LoadToVehicle last-event test cases (denormalizing correctly via
`y_raw * target_std + target_mean`, not dividing the already-z-normalized `.y` by 3600 — an
easy mistake, caught and fixed mid-investigation): true remaining time ranges 0.37h–11.0h,
mean 2.26h, std 1.76h — matching the scale already named in the (now-removed, see the last-event
table-removal commit) results.tex text.

**The model's predictions, by contrast, are not just noisy but systematically biased low**:
mean predicted = -4.98h, std = 6.54h, range -19.8h to +41.8h. Error (pred - true): mean = -7.24h,
std = 6.82h. **132 of 140 (94.3%) last-event predictions are negative**, despite the true value
never being negative. Order 911's error (-17.1h) is worse than average but not an outlier — about
1.45 std from the mean error, ranked 4th-most-negative prediction of 140 cases, not a unique
glitch.

**Root cause**: the model's z-normalization is fit on *all* prefixes of *all* depths
(`target_mean≈306h, target_std≈164h`, dominated by early/mid-trace prefixes with large remaining
times), while last-event's true scale (mean 2.26h) sits deep in that distribution's left tail
(z ≈ -1.85) and is a small minority of training examples (one last-event prefix per trace, vs.
many non-last-event prefixes per trace). A small, unremarkable error in the model's *normalized*
output near that tail region (order 911's own error is only ≈ -0.10 normalized std) gets
multiplied by the large global `target_std` on denormalization, turning a modest normalized-space
miss into tens of absolute hours — enough to frequently cross zero, since the true value's own
local scale (std 1.76h) is two orders of magnitude smaller than the global one it's denormalized
against. This is the same scale-mismatch mechanism the (removed) results.tex text already
identified as the cause of LoadToVehicle's catastrophic last-event R² (mean predictor
R²=-29796, HGT R²=-30.8) — this addendum extends it with the *systematic negative bias*
(not just high variance) and confirms via Depart's much milder R²_last (-0.17, vs.
LoadToVehicle's -30.8) that this is specific to KPIs whose last-event target sits unusually close
to zero, not a universal last-event artifact. Not investigated further: why the bias specifically
skews negative rather than symmetric (a plausible mechanism is the model extrapolating its
learned "more events seen → less time remaining" trend past the true near-zero floor, with
nothing constraining output to stay non-negative, but this wasn't directly tested).

**Conclusion**: order 911's -13.5h prediction is explained, and is representative rather than
anomalous. No code or data bug found. This also means the Shapley-magnitude spike at that trace's
last event (main finding above) is better attributed to this already-partially-known, KPI-wide
last-event calibration issue than to anything unique about order 911 or about "depth" causally.
