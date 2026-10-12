# Open Issues — Feasibility Review

Consolidated feasibility assessment for the substantive open engineering items scattered across
`AUDIT.md`, `TRAINING_VS_HOEG.md`, `EXPLAINABILITY.md`, `EXPLAINABILITY_DEPTH.md`, and the thesis
narrative files. Every "current status" below was freshly re-verified against the live repo
(2026-07-23) rather than trusted from the source docs — two items (9, 10) turned out to already be
resolved despite the source docs still describing them as open, one item (1) had a stale call-site
count, and one item (8) had its root-cause diagnosis overturned by direct re-testing (a claimed
permanent structural limitation turned out to be a fixable, recurring training pathology instead).

The 5 items assessed as cheap and well-scoped ("worth doing now") were implemented directly in this
pass — see each item's section for what changed and where.

Housekeeping/cosmetic items (legacy file cleanup, SHAP doc reconciliation, `feature_names`
placeholders, `ReduceLROnPlateau`/`MedianPruner` tuning knobs) are intentionally out of scope — they're
self-evidently low-effort and don't need a dedicated writeup.

---

## 1. True structural node removal for LOO masking

**What it is.** `explainer.py` masks nodes via `x[idx] = 0.0` (zero-substitution), not real graph
surgery (deleting the node row + reindexing every `edge_index` that references it).
`LOO_NAN_MASKING_VIABILITY.md` flags true deletion as "materially bigger than the current one-line fix."

**Current status.** The doc's own "5 call sites" figure is stale. Grepping the current codebase for the
zero-substitution pattern finds **7 sites**: 4 true node-level masks (`explainer.py:216`, `235`, `302`,
and Shapley's `shapley_node_importance()` value_fn at `explainer.py:407`) and 3 single-feature-column
masks that structural removal doesn't apply to (`explainer.py:49`, `430`, `2550`). The 2 extra sites
(both Shapley) postdate the viability doc, added during the LOO→Shapley pivot. None of the 4 node-level
sites currently touch `edge_index` at all.

**Feasibility: moderate, not large.** The reindexing logic is naturally shareable as one graph-surgery
primitive (`_remove_node_structurally(graph, node_type, idx)`) callable from all 4 sites — not 4
bespoke implementations. The real cost is downstream: every caller currently assumes "before" and
"after" graphs share node indices when comparing predictions; removing a node breaks that for any
index-based bookkeeping past the 4 call sites, and that wasn't audited here. Blast radius:
`explainer.py` only, no checkpoints touched.

**Recommendation: not worth doing — flag as permanent design limitation.** The viability doc's own
conclusion is the strongest argument against it: true deletion conflates a node's feature effect with
its structural effect into one number, working against this project's goal of eventually separating
the two (see item 2). Zero-substitution also matches the convention other perturbation-explainability
tools (GNNExplainer soft-masking, Captum) already use. Cite as scoped-out future work in the thesis.

---

## 2. LOO edge-masking vs. feature-masking scale mismatch

**What it is.** Deleting an edge is a structurally stronger perturbation than zeroing a feature vector
(which just recenters to the training mean), so edge-shift and feature-shift numbers aren't on
comparable scales — this is why `Packages→Items` edges show 53-55h shifts despite near-zero feature
attribution for the same node types (`EXPLAINABILITY.md` §"Masking strategy", `AUDIT.md`).

**Current status.** Confirmed unchanged — both `_loo_shift_for_nodes()` and the edge-deletion sweep in
`reg_explanation()` still report raw `|Δpred|` in the same hours-units regardless of perturbation type.

**Feasibility.** Two distinct paths, very different cost:
- **Report on separate scales/panels** (don't force comparability) — **trivial**, a
  plotting/reporting-convention change, no new computation.
- **Normalize edge shifts against a random-edge-removal null distribution** (Monte Carlo: delete N
  random same-type edges from similarly-sized graphs, report each real edge's shift as a z-score
  against that baseline) — **moderate**: needs a new per-edge-type/per-graph-size sampling routine, a
  decision on sample count for a stable baseline, reworking every downstream threshold that assumes
  raw-hours units (e.g. `large_shift = shift > self.target_std.item()`), and re-validating every
  already-cited edge-shift number (including the `Packages→Items` figure already in the thesis).

**Recommendation: worth doing, the cheap version only.** Switch to separate-scale reporting; flag the
random-baseline normalization as a citable future-work option rather than building it — it adds real
engineering and interpretability risk for a benefit the project doesn't clearly need.

---

## 3. LOO/IG integration (IG pre-selects LOO candidates)

**What it is.** `EXPLAINABILITY.md` §6 notes LOO costs O(all nodes+edges) per trace even when most
nodes are irrelevant, and proposes using IG's top-K attribution to pre-select candidate nodes before
running LOO only on those.

**Current status — cheaper than the source doc implies.** `_loo_shift_for_nodes()`
(`explainer.py:292-308`) already accepts an arbitrary `node_keys` subset — this "restricted LOO"
pattern is already in production use for GNNExplainer's candidates (`explain_gnn_primary()`,
`explainer.py:3887-3956`). The ranking function that reduces a candidate mask to per-node scores,
`_gnn_node_instance_ranking()` (`explainer.py:3351-3366`), is generic — no GNNExplainer-specific logic.
IG's `_compute_attribution_for_graph()` (`explainer.py:2676-2686`) returns the exact same
`{node_type: [n_nodes, n_feats]}` shape the ranking function expects, unchanged.

**Feasibility: small (~1-2 hours).** A new `explain_ig_primary()` can copy-adapt
`explain_gnn_primary()` (~70 lines), swapping only the identification step (GNNExplainer → IG's
`_compute_attribution_for_graph`), then reusing `_gnn_node_instance_ranking()`,
`_loo_shift_for_nodes()`, `_induced_edges()`, and `evaluate_explanation_quality()` verbatim. Caveat:
this only restricts the **node** half of LOO — the edge-importance loop has no candidate-restriction
path and stays exhaustive (consistent with GNNExplainer's edge masks being unavailable on this
architecture at all).

**Recommendation: worth doing now** — cheap, reuses existing validated primitives, no new masking logic.

---

## 4. Counterfactual fidelity/quality metric

**What it is.** `EXPLAINABILITY.md`'s closing section notes the counterfactual search has no fidelity
metric of its own; proposed fix: run LOO on the delta features between a trace and its retrieved
counterfactual, verify the shift direction matches the prediction gap.

**Current status — the needed data does not already exist in usable form.** `_graph_dissimilarity()`
(`explainer.py:1776-1833`) computes a **scalar**, pooled feature-distance component — it discards the
per-feature-dimension diff vector a real "delta features" input would need. Query and counterfactual
graphs are also different traces with generally different node counts per type, so "the delta between
two graphs' features" isn't well-defined at the per-node level without solving alignment first. LOO's
only masking primitive is zero-substitution on a single graph, not value-substitution between two
graphs — there's no reusable scaffolding here.

**Feasibility: moderate.** Needs a genuinely new masking primitive (substitute query's per-type-mean
feature vector with the CF's per-type-mean, sidestepping node alignment by operating at the same
per-type granularity `d_feat` already uses) plus new orchestration to interpret whether resulting
shifts point toward the actual prediction gap. Comparable in scope to `_loo_shift_for_nodes()` itself
plus a new validation function — similar order of effort to the already-built
`validate_fidelity_comparison()` (`explainer.py:3738`).

**Recommendation: worth doing but defer** — well-scoped once the per-type-mean simplification is
accepted, but it's new design, not a patch, and lower priority than item 3.

---

## 5. `explain_feature_attribution()` crash risk on logistics

**What it is.** `EXPLAINABILITY_DEPTH.md` (2026-07-04) documents an unguarded
`g[viewpoint_object]['last_event'][0]` access crashing on graphs with zero viewpoint nodes, found under
the (then-active) `TransportDocument` viewpoint.

**Current status — latent, not currently triggered.** The identical unguarded pattern still exists at
`explainer.py:2701`. A direct, read-only check against the live logistics checkpoint
(`t.Modelling('logistics', 1000).test_data`) found **0 of 8651 graphs** with zero `CustomerOrder`
(current viewpoint) nodes — the scenario appears resolved as a side effect of the
`TransportDocument→CustomerOrder` viewpoint switch, not because the code was fixed. The same unguarded
pattern actually appears at **7 call sites**, not 1: `explainer.py:1229, 1257, 1420, 1439, 2701, 3555,
3569`, spanning `explain_aggregate()`, `explain_aggregate_shapley()`,
`explain_feature_attribution()`, and `compare_loo_gnn_importance_aggregate()`.

**Feasibility: small.** Not truly "one line" — the same guard (or a shared `_last_event_graphs()`
filter helper, the better fix shape to avoid drift) needs repeating across 7 sites. ~30-45 minutes
including the helper.

**Recommendation: worth doing but defer** — currently latent, not actively broken. Low urgency, but
cheap enough to bundle into any other `explainer.py` work.

---

## 6a. Pipeline bug: `regenerate` leaves `ocel.csv`/`edges.csv` stale

**What it is.** `AUDIT.md` flags that `regenerate` can leave `ocel.csv`/`edges.csv` stale relative to a
freshly-regenerated `ev_log.csv`, crashing `hetero_graphs.py`'s `ast.literal_eval` with "malformed
node or string."

**Current status — root cause fully confirmed.** `hetero_graphs.py:69-76` needs 3 files from 2
independent generators: `ocel.csv`/`edges.csv` from `ocel_generator.Generator.generate_ocel(nodes)`,
and `ev_log.csv` from `process_generation.get_ev_log(nodes)`. `run_pipeline.py`'s `regenerate` stage
(`run_pipeline.py:112-118`) only calls `get_ev_log(nodes)` — it never calls `generate_ocel(nodes)`, and
`ocel_generator` isn't even imported in `run_pipeline.py`. So `regenerate` silently refreshes
`ev_log.csv` while leaving `ocel.csv`/`edges.csv` at whatever they were before.

**Feasibility: trivial.** `ocel_generator.Generator(database, cant)` has the same constructor signature
as `process_generation.ProcessGeneration(database, cant)`, and `generate_ocel(nodes)` takes the same
`nodes` object already produced and already passed to `get_ev_log(nodes)`. Fix: import
`ocel_generator`, call `og.Generator(database, cant).generate_ocel(nodes)` alongside the existing call.
~3 lines, one file, no retraining implied.

**Recommendation: worth doing now** — trivial, fully scoped, closes a real correctness gap.

---

## 6b. Pipeline bug: zero validation traces at small `cant`

**What it is.** `AUDIT.md` flags that small `cant` (confirmed 100/200 for order_management) can produce
zero validation traces, crashing `DataLoader(..., batch_size=0)` in `hetero_graphs.py:311`.

**Current status.** `train_test_builder.py:72-105` (`timestamps_generator()`) does a **temporal** split:
traces sorted by end-timestamp, `index_train = round(0.70*cant)`, test traces restricted to
`start_time > last_timestamp` (deliberate non-overlap, avoids temporal leakage). At small `cant`,
`len(test_orders)` after the non-overlap filter can be small enough that `int(len(test_orders)*0.5)`
truncates to 0, collapsing the validation window to empty — triggering an existing
`try/except ValueError` fallback (`train_test_builder.py:90-94`) that still produces a degenerate split.

**Feasibility — two tiers:**
- **Fail loudly instead of crashing 3 files away** (small): raise a clear `ValueError` naming `cant` at
  the end of `timestamps_generator()` if `len(val_orders) == 0` or `len(test_orders) == 0`. A few
  lines, `train_test_builder.py` only, no semantic change to the split.
- **Actually prevent the empty split at small `cant`** (moderate): touches the temporal non-overlap
  invariant directly; can't be verified safe against leakage from this read alone — a genuine design
  change to core splitting logic.

**Recommendation: worth doing, the small version only.** A loud, actionable error beats a silent crash.
The full redesign is low-priority — both citable checkpoints use `cant=2000`/`1000`; small-`cant` runs
are an edge case, not part of the production pipeline.

---

## 7. `mae_by_depth_summary.py` staleness

**What it is.** No `mae_by_depth_CustomerOrder.csv` exists for logistics (only a stale
`mae_by_depth_TransportDocument.csv`); the underlying `baselines.py` hardcodes
`DATABASE='order_management'`, `CANT=2000` at module level.

**Current status.** More is hardcoded than just DATABASE/CANT: `load_raw_split()`
(`baselines.py:40-66`) defaults `viewpoint='Orders'` and the `__main__` block never overrides it — for
logistics this would silently default to the wrong node type. `FEAT_COLS` (`baselines.py:31-32`) is a
fixed 4-viewpoint-feature list that `compare_to_baselines()` already had to work around per-dataset —
`CustomerOrder`'s raw feature count is unverified here. The `__main__` script also never calls
`homo_predictions()` (only Mean/GBT/HGT), yet the CSV `mae_by_depth_summary.py` expects has a `HomoGNN`
column — the original 2026-07-07 CSVs must predate a since-diverged script version. And the `__main__`
block never calls `.to_csv()` at all, only saves a PNG. Regeneration is pure inference over already-
trained checkpoints (seconds for logistics' 138 test graphs) — no retraining involved.

**Feasibility: small.** Four bounded fixes to one script: (1) pass `viewpoint=m.kpi_viewpoint`
explicitly, (2) derive `feat_cols` dynamically instead of the fixed list, (3) add a `homo_predictions()`
call with the row-alignment care `compare_to_baselines()` already established, (4) add a `.to_csv()`
export in the shape the summary script expects. No training, no schema changes.

**Recommendation: worth doing now** — bounded, well-scoped, removes the last "STALE" caveat from an
otherwise-complete figure set.

---

## 8. Dead feature attribution re-investigation (Forklift/HandlingUnit/Vehicle) — RESOLVED 2026-08-07

**2026-08-14 update: this item is resolved, not just re-diagnosed.** The Mechanism B pathology
this item root-caused (below) was fixed by adding AdamW optimizer support to `Het_Reg_Modelling`
(commit `000ddb0`, Aug 5) — AdamW's decoupled weight decay avoids the Adam + L2-weight_decay
runaway-decay interaction that was collapsing `Forklift`'s input projection to zero. Both
affected checkpoints were retrained and promoted:
- `TimeFrom_CustomerOrder_to_Depart` (`model_params.json`: `optimizer: adamw`, `lr: 0.005`)
- `TimeFrom_CustomerOrder_to_LoadToVehicle` (`optimizer: adamw`, `lr: 0.003`, seed=3, confirmed
  clean across 10/10 seeds before promotion)

Re-verified directly against the current production checkpoints (not just the config): loading
each `.pth` and checking `lin_dict.Forklift.weight.norm()` gives **2.41 (Depart)** and **4.79
(LoadToVehicle)** — both healthy and comparable in magnitude to every other node type's
projection, not the exactly-`0.0000` collapse described below. No further action needed here.

The original re-diagnosis (2026-07-23, kept for context) follows.

**What it is.** These logistics node types showed near-zero attribution despite healthy weights/edges.
The existing root-cause doc (`EXPLAINABILITY_DEPTH.md`, 2026-07-09) diagnosed a schema-level
reachability gap under the *old* `TransportDocument` viewpoint at `num_layers=1` — zero edges of any
type touch `TransportDocument` from these node types.

**Current status — re-verified directly against the live checkpoint and data (2026-07-23); the
"structurally disconnected" claim does not hold anymore.** `thesis_structure.txt:135` currently states
the issue was re-checked and remains open as reachability: "the Logistics dataset had at least one node
type that remained structurally disconnected from the viewpoint under any tested depth." This was
tested directly rather than trusted:

- **Schema check** (aggregated edge/node counts across all 8651 logistics test graphs): every node type
  — `Forklift`, `HandlingUnit`, `Vehicle`, `Truck`, `Container` — has real, non-empty edges to `Events`
  (33489 / 202464 / 13547 / 113280 / 206864 edges respectively, present in 6173–8651 of 8651 graphs),
  and `Events`↔`CustomerOrder` has 17164 edges present in all 8651 graphs. **Every node type is reachable
  within 2 hops** (`NodeType → Events → CustomerOrder`), comfortably inside the current `num_layers=3`.
  The old diagnosis's "zero edges touching the viewpoint" finding does not reproduce under the current
  `CustomerOrder`-viewpoint schema — it was specific to the old `TransportDocument` viewpoint and is
  stale, and `thesis_structure.txt`'s claim that the disconnection persists is itself inaccurate/stale.
- **Attribution check** (`_compute_attribution_for_graph`, InputXGradient, on 3 test graphs with all 4
  rare node types populated, current checkpoint): `Forklift` and `HandlingUnit` show exactly
  `0.000000` mean-|attribution| in every sampled graph; `Vehicle`/`Truck` show small but non-zero
  attribution (~1e-6, an order of magnitude below `Container`/`CustomerOrder`'s ~1e-4–1e-3).
- **Weight-norm check** (`model.lin_dict[nt].weight.norm()`, current checkpoint):
  `Forklift` = **0.0000** (shape `(16,9)`), `HandlingUnit` = 0.1305, `Vehicle` = 0.2702, `Truck` = 0.3050,
  `Container` = 2.2832, `CustomerOrder` = 1.0098, `Events` = 13.0343.
- **Feature-signal check** (raw `Forklift.x` across 2000 test graphs, 3478 populated instances):
  all 9 feature dims are non-degenerate (each nonzero in 7.5–16.6% of instances, plausible one-hot-style
  encoding) — ruling out "zero weight is correct because the feature itself is degenerate/constant."

**Corrected diagnosis: this is a recurrence of the previously-fixed dead-`lin_dict` weight-decay
pathology (Mechanism B, `EXPLAINABILITY_DEPTH.md`'s original root cause for a *different* set of node
types), not a reachability/structural problem (Mechanism A).** `Forklift`'s input projection has
collapsed to exactly zero weight despite carrying real signal and being genuinely reachable — the same
observable symptom the `weight_decay=0.001→1e-5` fix previously resolved project-wide, but recurring
specifically for `Forklift` on the current checkpoint even though `sweep()` already uses the corrected
`weight_decay=1e-5` (confirmed in item 9). `HandlingUnit`'s weight is small-but-nonzero (0.13) yet still
showed ~0 attribution in this quick sample — inconclusive from 3 graphs alone, needs a larger-sample
rerun to confirm whether it's a second dead projection or just genuinely low-signal. `Vehicle`/`Truck`
look like ordinary low-signal (not dead) node types — small weight, small-but-present attribution,
consistent with being rare (≈1 node per graph) rather than broken.

**Feasibility.**
- **Re-verification itself: done** (this pass) — schema check, attribution check, and weight-norm check
  all read-only/inference-only, a few minutes of compute total, no training.
- **Fixing `Forklift`'s specific dead projection**: plausibly **small-to-moderate**, not large/infeasible
  as previously assessed — this is the *same class of bug* already root-caused and fixed once (dead
  `lin_dict` from a training-dynamics issue, not a schema limitation). Likely candidates worth checking
  next (not yet investigated): whether `Forklift`'s sparsity (present in only ~71% of graphs, and often
  with few instances) starves it of gradient signal early in the ~5-8 epoch training budget before
  `ReduceLROnPlateau`/early-stopping cuts training short — the weight-norm training diagnostic added in
  item 12a will make this directly visible on the next retrain without a special investigation.
- **A structural/schema fix is NOT needed** — reachability is already fine, so no `hetero_graphs.py`
  graph-construction changes are implicated.

**Recommendation.** The stale `thesis_structure.txt:135` claim ("structurally disconnected... under any
tested depth") **should be corrected** — it's no longer accurate and mischaracterizes the current
mechanism as a permanent limitation when it's actually the same fixable training pathology already
solved once elsewhere. **Worth investigating further** (not "not worth pursuing" as originally assessed)
now that item 12a's weight-norm logging is in place — the next retrain's training log will show directly
whether `Forklift`'s projection norm collapses during training or was already near-zero at init, which
would immediately point to either a training-dynamics fix (e.g. a small `weight_decay` floor/warmup
override for known-sparse node types) or rule that out.

---

## 9. Optuna search-space discipline (`TRAINING_VS_HOEG.md` 5b–5d)

**What it is.** Three recommendations previously reported as not-implemented: fix `num_heads=2` (stop
tuning it), cap `n_trials`/switch to `GridSampler`, dataset-conditional `hidden_channels` ceiling for
logistics.

**Current status — already implemented; the earlier "not done" read was stale.** In `training.py`'s
`sweep()` (`training.py:352-543`): `FIXED_NUM_HEADS = 2` (`:394`), never in the search space (5b) ✓.
`GridSampler(search_space)` with `n_trials = min(n_trials, n_combos)` (`:404, 456`) — exact grid
coverage (5c) ✓. `hidden_choices` conditional on `is_logistics`, excluding `{128,256}` (`:398-399`)
(5d) ✓. Bonus: 5e (no `ReduceLROnPlateau` inside sweep trials, `:422-424`) and 5f
(`MedianPruner(n_warmup_steps=5)`, `:457`) are also both done. `git log` shows this landed in commit
`d7e3da3` ("Audit, catching up on problems that still needed fixing"), consistent with the 2026-07-12
audit pass.

**Feasibility: N/A — nothing to do.**

**Recommendation: not worth doing — already done.** `TRAINING_VS_HOEG.md` itself is the stale artifact
here and should be updated to mark 5b–5f complete, but that's a doc edit, not an engineering task.

---

## 10. Logistics checkpoint re-sweep status

**What it is.** `thesis_structure.txt` implies the `num_layers∈{3,4,5}` re-sweep for
logistics/`CustomerOrder_to_Depart` already happened and picked `num_layers=3`;
`presentation_plan.txt` (written earlier) describes it as still pending.

**Current status — resolved, not a live contradiction.** Loaded
`sweep_TimeFrom_CustomerOrder_to_Depart.db` directly via `optuna.load_study`: **25 completed trials**,
`num_layers` values tried = `{3,4,5}`, best trial = `{hidden_channels:16, lr:0.01, num_layers:3}`.
`model_params.json` matches this exactly. The deployed checkpoint's actual `state_dict` has conv-layer
indices `[0,1,2]` — literally 3 layers. File timestamps corroborate the sequence (sweep DB written Jul
10 11:53, checkpoint retrained Jul 10 12:21, same session).

**Feasibility: N/A — already done.**

**Recommendation: no work needed.** `presentation_plan.txt`'s "pending" language is simply stale,
superseded by the same-day 2026-07-10 retrain — flag that doc for correction so it stops misleading
future sessions (a one-line doc edit, out of this review's engineering scope).

---

## 11. Model-selection criterion bias (aggregate val MAE vs. last-event MAE)

**What it is.** Capacity increases (num_layers/num_heads) have repeatedly improved aggregate validation
MAE while degrading last-event MAE/R² (3 of 4 documented experiments). Early-stopping and
checkpoint-save currently use only aggregate val MAE.

**Current status — confirmed.** In both `Het_Reg_Modelling` (`training.py:270-350`) and
`Homo_Reg_Modelling` (`training.py:597-682`), `val_mae` accumulates over the full validation loader
under the general validity `mask` (not `last_event`) — confirmed aggregate-only. The best-checkpoint
decision (`if val_mae < best_val_mae: ...`) uses exactly this value. The data needed for a fix already
exists: `last_event` is set per-graph in `hetero_graphs.py` and survives `DataLoader` batching, so
`batch[kpi_viewpoint].last_event` is already accessible with zero new data plumbing.

**Feasibility: code change is small (~10-15 lines across 2 functions × 2 model types)** — add a second
accumulator restricted to `mask & last_event`, computed alongside the existing one using the same
accumulate-then-divide pattern already in place, then swap which value feeds the best-checkpoint
comparison. **But the blast radius is large**: every future retrain (both models, both datasets) would
select a different epoch's weights → different `.pth` checkpoints → different downstream MAE/R² numbers
already cited throughout `thesis_structure.txt`'s Results/Conclusion sections (the 97.99h/7.5h headline
figures). Existing citable checkpoints can't be "converted" — the alternate best-epoch weights were
never saved, so all 4 checkpoints would need full retraining to benefit. Additional risk: logistics'
last-event rate is ~1.6% of test prefixes, so a last-event-only validation signal would be computed
over a much smaller, noisier sample per epoch — plausibly noisier as a stopping criterion, not
obviously an improvement without a sanity check first.

**Recommendation: worth doing but defer** — cheap to implement, but should only be undertaken alongside
a deliberate, dedicated retraining pass, not folded into unrelated work, precisely because it
invalidates every currently-cited checkpoint number.

---

## 12. Attention-weight + per-node-type weight-norm training diagnostics

**What it is.** `EXPLAINABILITY_DEPTH.md` proposes (a) promoting an ad hoc attention-weight-inspection
monkey-patch to a standing diagnostic method, and (b) logging per-node-type weight norms alongside
`train_loss`/`val_mae` in the training log.

**Current status.** (a) No reusable code exists anywhere in the repo — grepped `explainer.py`,
`training.py`, `model_classes/HGT.py`, and every `experiments_archive`/`explainer_tests` file for the
monkey-patch pattern; found none. The original investigation's patch was apparently run ad hoc and
never committed — "promoting existing code" is a misnomer; this needs fresh implementation from the
doc's description. (b) `model_classes/HGT.py:9-11` confirms `self.lin_dict` is a `ModuleDict` keyed by
node type, trivially iterable for `.weight.norm()`. The training loop's log-dict construction
(`training.py:313-314, 650-651`) is a plain dict appended per epoch — adding keys is purely additive.

**Feasibility.** Attention diagnostic: **small-to-moderate** — the technique (monkeypatch
`HGTConv.message()` to intercept post-softmax attention, average by relation type) is well-specified in
the doc and PyG's source is inspectable, but there's no code to lift, only a description to implement
and validate against both datasets from scratch. Weight-norm logging: **trivial** — 2-3 line addition
inside the existing epoch loop in both training functions, purely additive new CSV columns, no effect
on training behavior or checkpoint selection.

**Recommendation.** Weight-norm logging — **worth doing now**, essentially free, directly guards
against a repeat of the dead-`lin_dict` investigation this project already burned time on once.
Attention diagnostic — **worth doing but defer**, real but bounded effort, lower urgency since the
pathology it would detect (uniform attention) was already traced back to the dead-`lin_dict` mechanism
rather than attention itself — its value going forward is as a general early-warning tool, not fixing
an active problem.

---

## 13. k-dimensional GNN baseline (HOEG's `HigherOrderGNN`)

**What it is.** HOEG's own baseline (Morris et al. 2019's k-GNN: stacked `GraphConv` + PReLU, wrapped
via PyG's `to_hetero()`) has no partial implementation anywhere in this project. This project's
existing "HomoGNN" (`model_classes/REG_GNN.py`) is a different, hand-rolled homogeneous model, not the
same thing.

**Current status.** `HigherOrderGNN` itself is compact (~77 lines,
`OCPPM-master/models/definitions/geometric_models.py:224-264`, fully generic constructor args). This
project's existing `REG_GNN.py` (26 lines) is already architecturally near-identical to what a k-dim GNN
would look like on this project's own data — same pooling, same MLP head, same single-scalar-per-graph
shape; the only delta is the conv operator (`GCNConv` vs `GraphConv`) and activation (`ReLU` vs
`PReLU`). Critically, a newly-audited finding
(`thesis_parts/hoeg_codebase_audit.md`, Finding 4) shows HOEG's actual task formulation is whole-case,
multi-target-per-graph — fundamentally different from this project's per-prefix, single-target
formulation. A byte-for-byte port isn't just an architecture swap, it's a different data pipeline and
target scheme. HomoGNN fit times on this project's hardware are trivial (9-62s per dataset per model,
per existing `*_homo_meta.json` sidecars) — compute cost for a new baseline is a non-issue either way.

**Feasibility — two tiers:**
- **Tier 1, pragmatic** (reuse this project's own per-prefix formulation, swap in the k-GNN operator):
  **small-moderate.** New model class (~30 lines, copy `REG_GNN.py`, swap conv/activation), new
  `KDim_Reg_Modelling()` in `training.py` (near-total copy of `Homo_Reg_Modelling()`, ~90 lines, reuses
  the existing `_hetero_to_homo()` transform unchanged), new `baselines.py` prediction function
  mirroring `homo_predictions()` (~45 lines). A single fixed-hyperparameter run is defensible for a
  baseline comparison (HOEG's own sweep only varies lr/hidden_dim). Total: ~150-200 LOC, 2 cheap
  training runs, no changes to existing checkpoints/pipeline stages. Must be explicitly labeled as using
  this project's task formulation, not HOEG's exact one.
- **Tier 2, HOEG-faithful** (`to_hetero()`, whole-case multi-target, matching HOEG's methodology
  exactly): **large.** Needs a new data pipeline, new target/masking scheme, and reconciling whole-case
  multi-target output back into this project's last-event MAE headline metric. Untested compatibility
  risk: HOEG's own code never exercises `to_hetero()` against a schema with object-object edges (which
  this project's graphs have and HOEG's don't) — unverified whether that combination works cleanly.

**Recommendation.** Tier 1 — **worth doing if time permits before the deadline**; roughly a half-day-
scale task given compute is negligible and the work is largely copy-adapt-paste from existing
`Homo_Reg_Modelling()`/`homo_predictions()`. Tier 2 — **not worth doing before the thesis deadline**;
leave as the already-scoped future-work item in `thesis_structure.txt:136`, strengthened with the newly
-audited task-formulation caveat, which isn't in the thesis text yet.

---

## Summary table

| # | Item | Effort | Recommendation |
|---|------|--------|-----------------|
| 1 | LOO structural node removal | Moderate | Not worth doing — permanent limitation |
| 2 | LOO edge/feature scale mismatch | Trivial (separate scales) / Moderate (normalization) | Worth doing — cheap version only |
| 3 | LOO/IG integration | Small (~1-2h) | **Worth doing now** |
| 4 | Counterfactual fidelity metric | Moderate | Worth doing but defer |
| 5 | `explain_feature_attribution` crash risk | Small | Worth doing but defer (latent, not active) |
| 6a | `regenerate` staleness bug | Trivial (~3 lines) | **Worth doing now** |
| 6b | Zero-validation-trace crash | Small (loud failure) / Moderate (real fix) | Worth doing — loud-failure version only |
| 7 | `mae_by_depth_summary.py` staleness | Small | **Worth doing now** |
| 8 | Dead feature attribution re-investigation | N/A | **RESOLVED 2026-08-07** — AdamW fix (commit `000ddb0`) promoted to both affected checkpoints; re-verified 2026-08-14 with healthy non-zero `Forklift` weight norms on the live production checkpoints |
| 9 | Optuna search-space discipline | N/A | Already done — fix the stale doc |
| 10 | Logistics re-sweep status | N/A | Already done — fix the stale doc |
| 11 | Model-selection criterion bias | Small code / Large blast radius | Worth doing but defer to a dedicated retraining pass |
| 12a | Weight-norm training diagnostic | Trivial | **Worth doing now** |
| 12b | Attention-weight diagnostic | Small-moderate | Worth doing but defer |
| 13 | k-dim GNN baseline (Tier 1) | Small-moderate (~half day) | Worth doing if time permits |
| 13 | k-dim GNN baseline (Tier 2, HOEG-faithful) | Large | Not worth doing before deadline |

**"Worth doing now" items — all 5 implemented in this pass** (2026-07-23): #3 (LOO/IG integration, new
`explain_ig_primary()` in `explainer.py`), #6a (regenerate bug fix, `run_pipeline.py`), #7
(`mae_by_depth_summary.py` regeneration, `baselines.py` parameterized + rerun for both datasets), #8's
re-verification (schema/attribution/weight-norm checks against the live checkpoint — see corrected
finding above), #12a (weight-norm logging added to `Het_Reg_Modelling`'s training loop,
`training.py`). None touched existing checkpoints or required retraining.

**Three documentation corrections surfaced by this review, independent of code work:**
`TRAINING_VS_HOEG.md` should mark recommendations 5b–5f as implemented (item 9); `presentation_plan.txt`
should have its "logistics re-sweep pending" language corrected (item 10); `thesis_structure.txt:135`
should have its "structurally disconnected under any tested depth" claim for Logistics corrected to
reflect the re-verified finding (item 8) — all three are stale relative to the current code/data state,
not open engineering items.
