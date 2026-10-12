# MAE Improvement Options — Catalogue and Viability

**Status:** analysis only, no code/thesis changes made. This is a decision reference, mirroring
the style of `OPEN_ISSUES_FEASIBILITY.md` / `DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md`.

## Framing

No comment file (`checked_thesis_comments.md`, `checked_thesis_comments_round2.md`) actually
contains a supervisor remark stating "the MAE is too high." Every existing MAE-adjacent comment
(#16–#21) was about *mis-framing* of already-existing numbers — apparent overfitting that turned
out to be temporal-split variance artifacts, a "terrible R²" claim replaced with a neutral
explanation, an overstated "HGT wins" claim corrected to per-KPI scoping. Those are all resolved
and the underlying numbers were never in dispute. This means "high MAE" is a newer/informal
comment not yet captured anywhere, and — checked directly — **the thesis currently has no text
proposing any concrete remediation for it.** `conclusion.tex`'s only related future-work item is
switching to k-fold/rolling-origin temporal validation, which addresses the train/val-gap
artifact, not the absolute error magnitude.

What the numbers actually show (`results.tex`, `combined_baseline_comparison.csv`):

| Dataset/KPI | HGT MAE_all (h) | HGT MAE_last (h) | R²_last | vs. baselines (last-event) |
|---|---|---|---|---|
| OM: Orders→PayOrder | 167.8 | 88.7 | 0.267 | ties k-dim GNN (p=0.31) |
| OM: Orders→PackageDelivered | 196.8 | 158.4 | -1.06 | **worst of 5 models** |
| Logistics: CustomerOrder→Depart | 112.0 | 6.12 | -0.17 | wins significantly |
| Logistics: CustomerOrder→LoadToVehicle | 115.0 | 8.35 | -30.8 | wins significantly |

And the sweep's own conclusion (`results.tex:51`): even after a full hyperparameter search,
PayOrder "cannot be improved below a 0.45 [normalized] MAE" — the clearest "we hit a wall"
statement already in the text. So the real complaint has two distinct faces that any fix needs to
target explicitly, since they move independently and sometimes in opposite directions when
capacity changes:
- **MAE_all** (all-prefix, pooled): large in absolute terms, often not clearly beating a mean
  predictor — this is the harder, more embarrassing number.
- **MAE_last** (last-event only): HGT is competitive-to-strong on Logistics, weak-to-worst on
  Order Management's PackageDelivered specifically.

## Already tried — do not re-propose

- **Dead-`lin_dict` / weight-decay bug**: `weight_decay=0.001` combined with the sweep's LR
  collapsed specific node types' entire input projections to zero. Root-caused and fixed
  (`weight_decay=1e-5` corrected in `sweep()`; affected checkpoints retrained).
- **AdamW adoption**: switched from Adam to AdamW (decoupled weight decay) for Logistics'
  Depart/LoadToVehicle checkpoints as a further fix in the same bug family.
- **`num_layers=1` reachability bug**: previously the sweep could pick 1 layer, which provably
  makes any node type without a direct edge to the viewpoint unreachable. Fixed by pinning
  `num_layers=2` for Order Management and tuning `{3,4,5}` for Logistics.
- **PReLU between stacked HGTConv layers**: was previously missing entirely; added to match HOEG.
- **HOEG-informed sweep priors**: grid search seeded with HOEG's own published best
  hyperparameters as an informed starting trial.
- **PayOrder train/val gap investigated directly** (`PAYORDER_TRAIN_VAL_GAP_INVESTIGATION.md`):
  confirmed the val split's target distribution is genuinely ~38% less variable than train's
  (std 135.8h vs. 219.6h) — a temporal-split artifact. Both "fix z-normalization" and "train
  longer" were tested directly and ruled out as the mechanism. **Do not re-propose either for
  PayOrder specifically.**
- **Logistics/Depart curve divergence investigated**: the deployed checkpoint itself shows no
  real train/val/test gap (432.1h/435.1h/439.9h); the divergence in the plotted curve is from
  epochs past the actually-saved early-stopped checkpoint.

## Ranked options

### 1. Fix the Logistics `Forklift` dead-`lin_dict` recurrence — ALREADY DONE (pre-dates this analysis)
**Targets:** was MAE_all and MAE_last for Depart/LoadToVehicle. **Status:** resolved, nothing to
apply.

This was originally ranked #1 based on `OPEN_ISSUES_FEASIBILITY.md` item 8, which as written
(2026-07-23) described `Forklift`'s input projection as still collapsing to zero on the live
checkpoint. **2026-08-14 verification found this is stale — the fix already shipped roughly two
weeks before this analysis was written.** Commit `000ddb0` (Aug 5) added AdamW optimizer support
to `Het_Reg_Modelling` specifically to fix the underlying Adam + L2-weight_decay runaway-decay
pathology; both `TimeFrom_CustomerOrder_to_Depart` and `TimeFrom_CustomerOrder_to_LoadToVehicle`
already have `optimizer: adamw` in `model_params.json` and were retrained/promoted on that basis
(LoadToVehicle confirmed clean across 10/10 seeds before promotion). Loading both current
production checkpoints directly and checking `lin_dict.Forklift.weight.norm()` gives **2.41
(Depart)** and **4.79 (LoadToVehicle)** — healthy, non-zero, in line with every other node
type's projection, not the `0.0000` collapse item 8 described. `OPEN_ISSUES_FEASIBILITY.md`
item 8 has been updated to reflect this.

Since this fix is already baked into the checkpoints the "high MAE" comment is presumably
reacting to, it does not explain that comment and there is no further action to take here —
option 2 below is the actual next open item.

### 2. Adopt `selection_metric='last_event'` for checkpoint selection
**Targets:** MAE_last across all 4 KPIs. **Cost:** zero dev (already implemented in `training.py`);
the real cost is re-running eval and updating every thesis table/figure/citation that currently
points at a pooled-selection checkpoint. **Risk:** moderate — would invalidate currently-cited
numbers close to submission. **Confidence:** high that MAE_last improves (3 of 4 documented prior
experiments showed pooled-selection favoring MAE_all at MAE_last's expense); this is exactly the
selection-bias problem, already identified, just not yet adopted.

Now the top actionable item on this list (option 1 needed no action) — a retrain to adopt this
selection metric is the cheapest genuinely-open lever available.

### 3. Log1p-transform the regression target — TRIED, mixed result (see below)
**Targets:** MAE_all primarily (outlier-driven blowups in mid-prefix predictions). **Cost:**
moderate — requires transforming targets before the loss, inverse-transforming before computing
reported MAE/R², and re-verifying that Shapley/LOO/counterfactual explainability code (which
reads raw target values) still behaves correctly. **Risk:** low-moderate, confirmed real: see
below. No architecture change needed — `training.py`'s `Modelling.__init__` now accepts an
opt-in `target_transform='log1p'` param (default remains `'zscore'`, fully backward compatible)
that applies `log1p()` before the usual z-score fit/apply.

**2026-08-14 empirical test** (Order Management / PayOrder, same tuned hyperparameters,
epoch budget, and seed as the production checkpoint — only the target transform differs):

| Metric | Baseline (z-score) | Log1p | Change |
|---|---|---|---|
| MAE_all | 167.14h | 159.46h | **-4.6%** |
| R²_all | -0.317 | -0.186 | improved |
| MAE_last | 97.99h | 112.30h | **+14.6%** |
| R²_last | 0.051 | -0.056 | worse, flipped negative |

Log1p delivered exactly the effect it was hypothesized to have on MAE_all (compressing the
right tail did reduce the aggregate metric), but at a real cost to MAE_last — the log-space
loss stops weighting a 5h vs. 500h miss so differently, which happens to be the wrong trade for
the minority last-event class specifically. **Net verdict: not a clean win.** Worth keeping only
if MAE_all is the metric being optimized for and MAE_last is checked/reported alongside it, or
combined with something that protects the last-event case (e.g. a last-event-weighted loss, or
selection_metric='last_event' checkpoint selection — see option 2 — to at least select the best
available epoch for the metric that regressed). Not recommended as a standalone fix.

### 4. Alternative loss: Huber/SmoothL1 in place of L1
**Targets:** MAE_all (outlier robustness). **Cost:** low — one-line change in `training.py`.
**Risk:** low. **Confidence:** low — L1 already directly optimizes the eval metric (MAE), so the
benefit of switching is uncertain; worth a quick trial run rather than a committed change, and
cheaper to test than item 3.

### 5. Expand the hyperparameter grid
**Targets:** both, marginal. **Cost:** moderate — grid size scales multiplicatively; the current
sweep is coarse (`lr ∈ {1e-3, 1e-2}` — nothing in between; `num_heads` fixed at 2, never tuned;
`weight_decay` not tuned at all). **Risk:** low. **Confidence:** low-moderate — the sweep already
found a fairly flat/plateaued MAE floor (the "cannot be improved below 0.45" line), so a finer
grid over the same axes may just confirm the plateau rather than break it. Tuning a previously
untouched axis (`num_heads`, `weight_decay`) is more likely to move the needle than finer steps
on already-tuned axes.

### 6. Increase `cant` (more training traces)
**Targets:** both, especially MAE_all's data-hungry mid-prefix regime. **Cost:** high —
compute/wall-clock scales with trace count × prefixes-per-trace; both full sweep and training
would need rerunning. **Risk:** moderate — untested for Logistics beyond `cant=2000`; small-`cant`
pipeline fragility is a documented failure mode in the *opposite* direction, not relevant here.
**Confidence:** moderate — feasible in principle since both datasets are synthetic OCEL 2.0 logs
from the official source (not fixed-size real data), and `cant=4000` has precedent for Order
Management already. Flag as viable but expensive, and the riskiest item to start this close to
submission given full-pipeline reruns (sweep → train → eval → explainability) would be needed.

### 7. Independently tune the baselines (HomoGNN, k-dim GNN)
**Targets:** neither MAE directly — this doesn't reduce HGT's own error. **Cost:** low-moderate.
**Risk:** low. **Confidence:** high that it addresses a *different* but related problem: both
baselines currently reuse HGT's own tuned hyperparameters rather than being independently swept
(`models.tex:137`), which weakens the "HGT isn't clearly better than baselines" framing — an
underpowered baseline understates how close the models really are. Worth doing if the goal is
defensible comparison validity rather than a lower headline MAE number.

### 8. Multi-seed averaging / ensembling
**Targets:** both, as variance reduction rather than bias reduction. **Cost:** low — no code
changes, just N reruns of the existing pipeline with different seeds, then report mean±std.
**Risk:** low. **Confidence:** moderate — reduces MAE inflation attributable to early-stopping
landing on an unlucky epoch, but doesn't address the underlying plateau; a good complement to
other items, not a replacement.

## Future work — not recommended now (too expensive or speculative for remaining thesis time)

- **K-fold / rolling-origin temporal validation** — already named in `conclusion.tex`'s future
  work for the train/val-gap artifact; the same technique would also stabilize hyperparameter
  selection and reported MAE variance, but requires multiple full training runs per fold. Good
  overlap to note in the text, not to execute now.
- **Relative-time positional encodings in HGT's attention** — a genuine architecture-research
  direction given remaining-time prediction is fundamentally about temporal reasoning, but this
  is a novel-contribution-sized change, not a tuning pass.
- **Richer engineered graph-structural features** (degree, centrality, etc.) — plausible but
  requires reworking `hetero_graphs.py`'s feature layout and re-validating every downstream
  consumer (training, explainability, all cached `_norm.json` sidecars) — too much surface area
  to de-risk before submission.

## Bottom line recommendation

If only a few of these are pursued, in priority order:

1. ~~Fix the Logistics `Forklift` dead-`lin_dict` recurrence~~ — **verified 2026-08-14: already
   done**, pre-dates this analysis (see option 1 above). No action needed.
2. **Adopt `selection_metric='last_event'`** — already implemented in code, just unused; the
   top genuinely-open item on this list. High confidence of improving the more decision-relevant
   metric, at the cost of a retrain + updating every thesis table/figure that cites a
   pooled-selection checkpoint.
3. ~~Trial the log1p target transform~~ — **tried (see option 3 above): -4.6% MAE_all, but
   +14.6% MAE_last.** Not a standalone win; drop it from the shortlist unless paired with a
   last-event safeguard (e.g. item 2's selection metric).

Everything else in the ranked list is a reasonable secondary pass if time allows, but item 2 is
now the single best confidence-to-cost pick given how close to submission this is.

## Decision (2026-10-12)

- **Item 2 (`selection_metric='last_event'`) rejected, permanently** — the blast radius
  (retraining every cited checkpoint and updating every thesis table/figure that quotes a
  pooled-selection MAE) outweighs the gain for this project's remaining scope. Not revisiting
  this unless a future retraining pass happens for an unrelated reason anyway.
- **Item 3 (`log1p` target transform) kept as a committed, opt-in `target_transform` parameter**
  on `Modelling.__init__` (default remains `'zscore'`, unchanged behavior) — the mixed result
  above is citable as an investigated-and-rejected option even though it isn't the active
  default.
