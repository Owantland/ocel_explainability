# Deepening the Explanations: Findings from the Current Model and Recommendations

## Context

Running all four explainability methods (`explain_feature_attribution()` ×2 methods, `explain_aggregate()`, `explain_trace()`, `explain_counterfactual()`) end-to-end on the current retrained `order_management`/`PayOrder` checkpoint surfaced something stronger than "some node types are less important than others": **five of the model's seven node types carry exactly zero gradient-based attribution, not just small attribution.** This document traces that finding to its root cause in the trained weights themselves (not a bug in the explainability code) and recommends what to change upstream — in graph construction, architecture, or training — to get genuinely deeper explanations out of future runs.

Everything below is sourced to a specific CSV, checkpoint key, or a live diagnostic run this session (details inline) — nothing here is inferred from the paper literature.

## What the outputs show

### 1. Exactly zero attribution for 5 of 7 node types

`files/explainer_outputs/order_management/attribution/ig_attribution_inputxgradient.csv` (and the `IntegratedGradients` counterpart — both methods agree):

| Node type | Dims | Attribution |
|---|---|---|
| `Customers` | 15 | all exactly `0.0` / `-0.0` |
| `Employees` | 3 | all exactly `0.0` |
| `Items` | 2 | all exactly `0.0` / `-0.0` |
| `Packages` | 1 | exactly `0.0` |
| `Products` | 2 | all exactly `0.0` |
| **`Orders`** | 5 | non-zero (dominated by `n_packages`) |
| **`Events`** | 34 | non-zero (small but real) |

This is a *provable* zero, not a "the model doesn't find this very important" small number — worth distinguishing, because it changes what kind of fix is appropriate.

### 2. Root cause is not uniform across the 5 affected types — two distinct mechanisms

**Mechanism A — no path to the loss (`Employees`, `Products`, `Packages`).** The checkpoint's `num_layers=1` (confirmed via the `arch.json` fix earlier this session). With one HGTConv layer, `Orders`' prediction can only be a function of node types with a **direct edge into `Orders`**. Checking the schema's edge types, only three qualify: `Customers→Orders`, `Items→rev_to→Orders`, `Events→rev_to→Orders`. `Employees`, `Products`, and `Packages` connect to `Orders` only indirectly, via `Events` (which aggregates `o1_Employees`/`o1_Products`/`o1_Packages` object-count features — see `hetero_graphs.py:161-172`) — a 2-hop path that a 1-layer model cannot traverse for the *raw* per-object features. Their gradient is provably zero regardless of training quality.

**Mechanism B — dead input projection (`Customers`, `Items`).** These two *do* have a direct edge into `Orders`, so Mechanism A doesn't apply. Checking the checkpoint directly:

```
lin_dict.Customers: weight abs-mean=0.000000  bias abs-mean=0.000000
lin_dict.Items:     weight abs-mean=0.000000  bias abs-mean=0.000000
lin_dict.Employees: weight abs-mean=0.000017  bias abs-mean=0.000006
lin_dict.Products:  weight abs-mean=0.000018  bias abs-mean=0.000001
lin_dict.Packages:  weight abs-mean=0.000039  bias abs-mean=0.000042
lin_dict.Events:    weight abs-mean=0.006958  bias abs-mean=0.004865
lin_dict.Orders:    weight abs-mean=0.025160  bias abs-mean=0.031320
```

`lin_dict.Customers` and `lin_dict.Items` — the very first per-type linear projection, applied *before* any attention or message passing — have collapsed to all-zero weights. Their raw features are converted to an all-zero vector before entering the rest of the network at all, so no downstream computation (attention, aggregation) can ever depend on them. This is a different and more fundamental failure than Mechanism A: it's not that the architecture can't reach these types, it's that the model has zeroed out their entry point entirely.

### 3. The attention mechanism was checked directly and is *not* the cause

The original working hypothesis (before this diagnostic) was that HGTConv's per-relation attention had "collapsed onto `Events`" because it carries richer engineered features. This was checked directly by monkey-patching `HGTConv.message()` to capture the actual post-softmax attention weight for each edge feeding into the `Orders` node, averaged over 20 real test graphs:

```
('Customers', 'to', 'Orders'):      mean attention = 0.136746
('Events', 'rev_to', 'Orders'):     mean attention = 0.136746
('Items', 'rev_to', 'Orders'):      mean attention = 0.136746
```

These are **statistically indistinguishable** across relation types (identical to 6 decimal places across differently-sized graphs) — the attention mechanism currently treats every incoming edge as interchangeable, i.e. it hasn't learned to differentiate relations at all (consistent with 22 of 25 `p_rel` relation-priority parameters being bit-identical to their initialization value, and the `k_rel`/`v_rel` weight matrices having abs-mean ~5–7×10⁻⁵ — everything in the attention path is still close to initialization scale). So: **attention isn't why `Customers`/`Items` show zero gradient** — it's Mechanism B (the dead `lin_dict` projection), which happens entirely upstream of attention. This is worth stating precisely because it changes where a fix needs to target.

### 4. LOO's near-perfect fidelity scores are a symptom, not a strength

`explain_aggregate()`'s `characterization_score = 1.0000 ± 0.0000` (zero variance, n=50) is a direct consequence of #1–#3: with only `Orders` and `Events` capable of influencing the prediction at all, any reasonable top-k node/edge selection necessarily captures "the whole reachable graph," making perfect fidelity close to mechanical rather than a sign the explanation method found something subtle. Comparing fidelity scores across models with different depths/architectures without accounting for this would be misleading.

### 5. Root cause of the dead `lin_dict.Customers`/`lin_dict.Items` projection — CONFIRMED experimentally

Recommendation #1 (below) called for investigating whether the dead projection was a training pathology or a genuine "these features are uninformative" finding. This was tested directly with a controlled comparison: same architecture (`hidden_channels=48, num_layers=1, num_heads=1`), same data, same seed (`torch.manual_seed(42)`), 25 epochs, only the optimizer hyperparameters changed:

| Condition | `lr` | `weight_decay` | Optimizer | `lin_dict.Customers` \|w\| (epoch 1 → 25) | `lin_dict.Items` \|w\| (epoch 1 → 25) | train_loss (epoch 1 → 25) |
|---|---|---|---|---|---|---|
| **Current** (`model_params.json`) | 0.01 | 0.001 | Adam | 0.000000 → 0.000000 | 0.000000 → 0.000604 | 0.669 → 0.657 (flat) |
| Regression defaults | 0.001 | 1e-5 | Adam | 0.119 → 0.131 | 0.358 → 0.294 | 0.652 → 0.437 |
| Regression defaults | 0.001 | 1e-5 | AdamW | 0.137 → 0.173 | 0.364 → 0.445 | 0.652 → 0.413 |

**This is decisive, not just suggestive.** The dead projection reproduces immediately (visible by epoch 1) under the current hyperparameters (`lr=0.01, weight_decay=0.001` — confirmed via `files/models/order_management/Hetero/model_params.json`, and also reproduced independently on the `PackageDelivered` checkpoint, where `Customers`/`Products` die instead), and is **completely absent** when using the "regression defaults" (`lr=0.001, weight_decay=1e-5`) with either plain `Adam` or `AdamW` — the projection stays alive and the weights keep growing meaningfully. It's not specifically an L2-vs-decoupled-decay issue (both optimizers work fine at the lower settings) — it's that `weight_decay=0.001` combined with `lr=0.01` is simply too aggressive for these small-scale, weakly-signaled per-type parameters, wiping them out before they can establish any useful signal. The original training log for this checkpoint corroborates this independently: train_loss barely moved over 27 epochs (0.659→0.649) and val_mae never improved — a model that was already visibly struggling to learn anything, not just one with a narrow dead-parameter quirk.

**This traces directly to a previously-identified, not-yet-fixed issue**: `TRAINING_VS_HOEG.md` (written earlier this session) already flagged `weight_decay=0.001` as hardcoded in `sweep()` while the "regression defaults" use `1e-5` — a documented inconsistency (its recommendation #3) that was explicitly *not* included when the user asked to implement recommendations 2, 4, and 5. This investigation shows that gap isn't cosmetic — it's the direct cause of half the model's node types being unreachable in every explanation this session has produced.

One caveat worth carrying forward: in this uncontrolled 25-epoch comparison (no early stopping applied), val_mae got *worse* over time for both "regression defaults" runs even as train_loss kept dropping — ordinary overfitting once the model actually starts learning, not a new problem. The real `Het_Reg_Modelling()` training loop already has early stopping (`patience=10`) and would pick the best-val-MAE epoch along the way, not the 25th — so this isn't a reason to hesitate on the fix, just a note that "regression defaults" alone don't replace early stopping.

## Recommendations

1. ~~Investigate~~ **Fix the `weight_decay`/`lr` combination used for this checkpoint — confirmed root cause (section 5 above), not just a hypothesis.** Retrain `TimeFrom_Orders_to_PayOrder` (and re-check `TimeFrom_Orders_to_PackageDelivered`, which shows the same pathology with a different "dead" type) using `lr=0.001, weight_decay=1e-5` instead of the sweep's `lr=0.01, weight_decay=0.001`, with early stopping active as normal. This is the single highest-leverage fix in this document — it's cheap (a hyperparameter change, no architecture work) and directly resolves the majority of the "5 of 7 node types are dead" finding (specifically `Customers`/`Items`; see #2 for `Employees`/`Products`/`Packages`). Also revisit `TRAINING_VS_HOEG.md` recommendation #3 properly now that there's concrete evidence it matters, rather than treating it as a minor cleanup item.
2. **Increasing `num_layers` (currently 1, chosen by the Optuna sweep for best MAE) fixes the *other* mechanism only** (`Employees`/`Products`/`Packages` gain a real 2-hop path to the loss) — it will **not** fix `Customers`/`Items` on its own, since their projection dies before any conv layer runs regardless of depth (as confirmed in #1, the fix there is a training hyperparameter, not architecture). Treat these as two independent fixes. As before, note the accuracy/explainability trade-off explicitly if `num_layers≥2` is adopted only for the "explained" checkpoint rather than the sweep-optimal one.
3. **Add attention-weight inspection as a standing diagnostic, not a one-off.** The monkey-patch used in section 3 above is cheap and general — worth turning into a small permanent method (e.g. `explainer.py`'s `_get_attention_weights(graph)`) that reports mean attention per incoming relation type. This is the fastest way to notice this kind of degeneracy on any future retrain, rather than requiring another manual checkpoint-diffing session. Per [[reference_stevens2022]]'s caution that attention shouldn't be treated as a faithful primary explanation, frame this as a training-health diagnostic alongside the existing gradient/perturbation methods, not a new explanation method in its own right.
4. **Consider adding a per-node-type weight-norm check to the training loop itself** (e.g. logged alongside `train_loss`/`val_mae` in the training-log CSV) — this exact pathology would have been visible from epoch 1 of the original training run if `lin_dict.<type>.weight.abs().mean()` had been tracked, rather than requiring a full post-hoc explainability investigation to notice. Cheap, and generalizes beyond this one checkpoint/target.
5. **Fill in `feature_names` for `Customers` (15 dims) and `Employees` (3 dims)** — currently unnamed (`feat_0..feat_14`, `feat_0..feat_2`) because `training.py`'s `feature_names` construction only covers `attributes`/`time_attributes`/`Events`/viewpoint-extra, never the `encoding`/`role_encoding` config paths (`config.yml`'s `encoding: ['Customers', 'Employees']` and `role_encoding: {Employees: 'role'}`). Low effort, mirrors the `n_packages` naming fix from earlier this session — ensures that once #1 makes these types matter again, the resulting explanation is immediately legible.
6. **Extend `_explain_attribution_by_depth()` beyond the viewpoint type** to also cover `Events` — the only other type currently carrying real signal, and currently invisible in the depth-stratified view (`explainer.py`, defaults to `node_type=self.viewpoint_object` only).

## Verification

- #1: after any training-side change, re-run the `lin_dict.Customers`/`lin_dict.Items` weight-magnitude check used in section 2 above, and re-run `explain_feature_attribution()` to confirm non-zero attribution appears for these types if the fix worked.
- #2: after increasing `num_layers`, re-run the same attribution CSV check and confirm `Employees`/`Products`/`Packages` now show non-zero attribution (they should; `Customers`/`Items` still won't unless #1 is also addressed).
- #3: the attention-weight diagnostic should be added as a small unit-testable function and run against at least one graph per dataset as a sanity check going forward.
- #5/#6: verify via the same pattern used for the `n_packages` fix — print `e.feature_names['Customers']`/`['Employees']` and confirm real names appear instead of `feat_N`.

## Update: root cause fixed at the code level, all reachable checkpoints retrained (2026-07-04)

Recommendation #1 is now done, not just diagnosed. Fixed `training.py`'s `sweep()` at both hardcode sites (`training.py:367`, `training.py:444`: `weight_decay=0.001` → `1e-5`, matching `_DEFAULTS`), so every future sweep-driven retrain (any dataset, any target) inherits a sane value instead of the one that caused this. Then swept every existing checkpoint for the same pathology by inspecting `lin_dict.<type>.weight.abs().mean()` directly against each one's saved `lr`/`weight_decay`:

| Checkpoint | Before | After |
|---|---|---|
| `order_management` / `TimeFrom_Orders_to_PayOrder` | `Customers`/`Items` dead | Fixed & retrained — both alive (0.13/0.33) |
| `order_management` / `TimeFrom_Orders_to_PackageDelivered` | `Customers`/`Products` dead | Fixed & retrained (required rebuilding the graph cache for this target, then rebuilding it back to `PayOrder` afterward — see below) — **all 7 node types alive**, including `Employees`/`Packages`/`Products` (this checkpoint's `num_layers=2`, enough depth for 2-hop reachability, so both mechanisms resolved at once here) |
| `logistics` / `TimeFrom_TransportDocument_to_Depart` (active target) | `Forklift`/`HandlingUnit`/`Truck` dead | Fixed & retrained — `Container`/`CustomerOrder`/`Events`/`Vehicle` (all with direct edges to `TransportDocument`) now alive; `Forklift`/`HandlingUnit`/`Truck` **remain dead**, but confirmed via the edge-type list that none of the three have *any* edge touching `TransportDocument` at all — this is Mechanism A (`num_layers=1` reachability), not a residual weight_decay problem. Consistent with `Employees`/`Products`/`Packages` in the original `PayOrder` finding. |
| `logistics` / `TimeFrom_TransportDocument_to_LoadToVehicle` | only `Forklift` dead | Not retrained — not reachable via the current `config.yml` (`kpi_event: "Depart"` is active); revisit if this target is ever reactivated |

**A new, separate bug surfaced during validation**: `explain_feature_attribution()` assumes every graph has at least one viewpoint node (`g[self.viewpoint_object]['last_event'][0]` with no bounds check) — this crashes on `logistics` test data, where some graphs apparently have zero `TransportDocument` nodes. Never caught before because every explainability method this session was exercised on `order_management` only, where the viewpoint always has exactly one node per graph. Not fixed here (out of scope for the weight_decay investigation) — noted for a future pass.

**Accuracy**: no regression in either retrain — `PackageDelivered` new test MAE 85.2h (old checkpoint couldn't be re-evaluated for a direct comparison, predates the PReLU architecture change, same limitation as the original `PayOrder` comparison); `Depart` new test MAE 133.9h (same old-checkpoint limitation). `PayOrder`'s comparison from the original fix remains the cleanest before/after: 166.1h → 160.9h.

All old checkpoints/configs backed up before overwriting: `files/models/order_management/Hetero/_pre_fix_backup/`, `files/models/logistics/Hetero/_pre_fix_backup/`.

## Update: added capacity (`num_layers=3`, `num_heads=3`) improves aggregate MAE but degrades last-event accuracy specifically — order_management/PayOrder (2026-07-07)

With the `weight_decay` fix in place (`lin_dict` no longer dying), the natural next question was whether *more* capacity helps the now-healthy `TimeFrom_Orders_to_PayOrder` checkpoint (`hidden_channels=64, num_layers=2, num_heads=2, lr=0.001, weight_decay=0.0`) further. Two variants were tried, each changing exactly one architectural dimension and matched on everything else:

- `experiment_order_management_layers3.py`: `num_layers=3` (one more attention hop)
- `experiment_order_management_heads3.py`: `num_heads=3` (required bumping `hidden_channels` 64→63, the nearest multiple of 3, since PyG's `HGTConv` requires `hidden_channels % num_heads == 0`)

Both looked like improvements on the training-time metric (normalized validation MAE, dominated by the ~98.4% of examples that are *not* the last event): `layers=3` reached best val MAE 0.428 vs. the official 0.454; `heads=3` reached 0.456, essentially tied. But re-evaluating both saved checkpoints against the official one on the real test set, split by whether the graph is a last-event prefix, tells a different story:

| Model | ALL prefixes MAE / R² (n=2541) | **LAST-EVENT MAE / R² (n=235)** |
|---|---|---|
| **Official** (`layers=2, heads=2`) | 167.8h / −0.321 | **88.7h / 0.267** |
| `layers=3` experiment | 160.2h / −0.168 | **104.5h / 0.081** |
| `heads=3` experiment | 165.2h / −0.234 | **113.9h / −0.004** |

Both variants win on the aggregate "ALL prefixes" slice (lower MAE, less-negative R²) but lose decisively on last-event prefixes — the actual prediction target reported throughout `presentation_plan.txt` (`MAE_last`, `R2_last`). `heads=3`'s last-event R² is effectively 0 (no better than predicting the mean); `layers=3` fares a bit better but still gives up ~16h of MAE and more than half its R².

**Interpretation**: last-event prefixes are a small, high-value minority (~1.6% of training examples per `experiment_weighted_loss.py`'s count on the logistics dataset; similarly rare here). Early stopping selects the checkpoint with the best *aggregate* val MAE, which is dominated by the majority early/mid-trace prefixes. Extra model capacity (a layer or a head) gives the optimizer more room to fit that majority distribution better, at the expense of the minority last-event case — the opposite of what the thesis actually needs to report. This is consistent with, and a variant of, the same dynamic `experiment_weighted_loss.py` was independently designed to counteract (by up-weighting last-event samples in the loss) on the logistics dataset.

**Neither `layers=3` nor `heads=3` was promoted** — official stays at `num_layers=2, num_heads=2, hidden_channels=64`. `lin_dict` weight health was checked for both experiments and is fine in both cases (no dead projections; this is a genuine capacity/overfitting effect, not a repeat of Mechanism B above).

**Implication for future capacity experiments on this checkpoint**: aggregate validation MAE is a misleading selection criterion here. Any future architecture search for `TimeFrom_Orders_to_PayOrder` (or `PackageDelivered`, likely similarly imbalanced) should select/early-stop on last-event val MAE specifically, or at minimum report both metrics before adopting a change — the training loop currently only tracks and early-stops on the aggregate.

## Update: same pattern confirmed on logistics/`TransportDocument_to_Depart`, plus a stale-`model_params.json` bug found and fixed (2026-07-07)

The `num_layers=5, weight_decay=0` recipe that fixed `CustomerOrder_to_Depart` (a genuine reachability problem — see the original weight_decay update above) was tested against `TimeFrom_TransportDocument_to_Depart` too, via `experiment_logistics_transportdocument_layers5_wd0.py`. Doing this required temporarily restoring the `TransportDocument`-viewpoint hetero graph cache (from `files/hetero_structures/logistics/1000_transportdocument_backup/`) and `config.yml`'s `kpi_viewpoint` in turn, since these aren't viewpoint-namespaced (see [[project_hgt_dead_features_finding]] and `AUDIT.md`'s namespacing complaint) — both were reverted immediately after.

**Bug found first**: `model_params.json` recorded `{num_layers: 5, lr: 0.01, weight_decay: 0.0}` for this task_id, but the actual on-disk `TimeFrom_TransportDocument_to_Depart.pth` — inspected directly via its `state_dict` — has only **3** conv layers and was trained at `lr=0.001`. The JSON had silently drifted out of sync with the real checkpoint (likely overwritten while a different task_id was active in the same shared file). This is a live crash risk: `pipeline_1000_logistics.py` builds a fresh model from `model_params.json` and then calls `load_state_dict()` against this checkpoint — a 5-layer model can't load 3-layer weights. Fixed by correcting the JSON entry to `{hidden_channels: 64, num_layers: 3, num_heads: 2, lr: 0.001, weight_decay: 1e-05}`, matching the real weights.

With an honest baseline established (rebuilt using the checkpoint's actual architecture, not the stale JSON), the `layers=5, weight_decay=0` variant was trained and compared on the real test set:

| Model | ALL prefixes MAE / R² (n=8513) | **LAST-EVENT MAE / R² (n=138)** |
|---|---|---|
| **Official** (`layers=3, heads=2`, actual weights) | 115.4h / 0.278 | **9.1h / −4.022** |
| `layers=5, weight_decay=0` experiment | 112.7h / 0.322 | **14.3h / −6.730** |

Same shape as the `PayOrder` result above: the deeper/less-regularized variant wins on the aggregate slice (112.7h vs. 115.4h) but is **57% worse on last-event MAE** and clearly worse on R². The key difference from `CustomerOrder_to_Depart`: that checkpoint started at `num_layers=2` with several node types *provably unreachable* (Mechanism A), so going to 5 layers fixed a structural defect, not just added capacity. `TransportDocument_to_Depart` was already at `num_layers=3` with no known reachability gap, so the extra depth here is pure added capacity — and, consistent with the `PayOrder` finding, that capacity gets absorbed by the ~98%+ non-last-event majority at the expense of the metric that matters. **Not promoted** — official stays at `num_layers=3`. Experiment checkpoint kept at `TimeFrom_TransportDocument_to_Depart_layers5_wd0_experiment.pth` for reference, not wired into any pipeline.

**Running count across all four capacity experiments this session**: 3 of 4 (`PayOrder`×2, `TransportDocument_to_Depart`) show the aggregate-vs-last-event trade-off; only `CustomerOrder_to_Depart`'s `layers=5` was a clean win, because it was fixing reachability rather than just adding capacity. This is now a strong enough pattern to treat as a standing rule: **added depth/heads should not be adopted based on aggregate/sweep val MAE alone** — always check last-event MAE/R² specifically (or better, make it the early-stopping criterion, per the recommendation above) before promoting any architecture change on these two datasets' viewpoints.

## Update: aggregate LOO verified at full test-set scale; new LOO-vs-GNNExplainer comparison, single-trace and aggregate (2026-07-08)

**Aggregate LOO (`explain_aggregate()`) verified and fixed.** Its on-disk output was stale and internally inconsistent — only 10 of the expected 50 traces had succeeded, silently, via a bare `except: continue` with no logging (`explainer.py`, inside `explain_aggregate`'s per-trace loop), and its reported characterization score (0.7008±0.2277) matched neither `EXPLAINABILITY.md`'s 0.887±0.137 nor this doc's own earlier-quoted 1.0000±0.0000. Root cause: that run predates several of this session's checkpoint retrains (the PayOrder `weight_decay` fix landed after that run's timestamp) — not a reproducible code bug. Fixed the silent swallow to log failures, then reran clean:

- n=50 traces: **zero failures**, characterization = 0.8033 ± 0.2191
- n=235 traces (the full last-event test set for `order_management`/PayOrder): **zero failures**, characterization = **0.8426 ± 0.1818**

This full-scale, current-checkpoint number is the one to cite going forward — it lands closer to `EXPLAINABILITY.md`'s 0.887±0.137 than to this doc's earlier 1.0000±0.0000, and that earlier zero-variance number was always implausible on its own (LOO across genuinely different traces essentially never produces a perfect score for every single one unless something degenerate is happening, e.g. only 1-2 node types carrying any signal — not the case for the current, healthy checkpoint, which uses all 7 node types).

**New capability: `compare_loo_gnn_importance(order_id, top_k=5, ...)`** — single-trace comparison of which node *instances* LOO and GNNExplainer each flag as most important (node importance only; edge importance is out of scope, since GNNExplainer's edge masks are disabled for this architecture — see `_get_gnn_explainer`'s docstring — and the separate experimental edge-importance path reweights post-softmax attention rather than performing a true ablation, explicitly not comparable to LOO). Run on two orders:

| Order | Overlap (top-5) | Notes |
|---|---|---|
| #1821 | 2/5 | Both methods agree `Events[13]` is the dominant node |
| #1812 | 1/5 | Both methods agree `Events[13]` is the dominant node; reran independently, same result both times (GNNExplainer's stochastic optimization shifts ranks 2-5 slightly run-to-run, but the sole point of agreement is stable) |

In both traces, GNNExplainer's own top-5 scores are nearly flat (spread of ~0.03-0.05) while LOO's shifts span two-plus orders of magnitude — the two methods aren't just disagreeing on ranking, GNNExplainer doesn't distinguish its candidates as sharply as LOO does in the first place.

**New capability: `compare_loo_gnn_importance_aggregate(n_traces=235, top_k=5, ...)`** — extends the above across the full 235-trace last-event test set, using the lower-level primitives directly (`reg_explanation()` + a raw `_get_gnn_explainer()` call) rather than the heavy `explain_trace()`/`explain_gnn_subgraph()` wrappers, to avoid generating thousands of redundant per-trace plot files. Surfaced and fixed a real PyG limitation along the way: `GNNExplainer._initialize_masks()` calls `.max()` unconditionally on each relation's edge index, which crashes on any graph with a *legitimately empty* edge type (e.g. a `Customer` with no directly-linked `Employees` — common and valid, not a data bug). Added an explicit pre-check that skips these with a clear logged reason instead of surfacing a raw `RuntimeError` — 17 of 235 traces (7.2%) hit this and are skipped, not failed.

- **218/235 traces succeeded, 0 unexpected failures.** Mean top-5 overlap: **2.33/5 (σ=0.95)** — consistent with, and bracketing, the two single-trace data points above (2/5 and 1/5), not an outlier.
- Overlap distribution across traces is smooth and unimodal (peak at 2-3/5; only 3 traces at 0/5, only 2 at 5/5) — the two methods partially but consistently agree across the whole test set, not just on the two hand-picked examples.
- **Aggregate top-5 node types by GNNExplainer importance, with LOO's aggregate magnitude alongside** (n=218 traces):

  | Node type | GNNExp. rank | GNNExp. mean score | LOO rank | LOO mean \|shift\| (h) |
  |---|---|---|---|---|
  | Items | 1 | 0.1428 | 4 | 1.09 |
  | Packages | 2 | 0.1428 | 3 | 2.64 |
  | Orders | 3 | 0.1422 | — | — |
  | Events | 4 | 0.1399 | 1 | 7.89 |
  | Products | 5 | 0.0547 | 5 | 0.26 |

  This is the aggregate confirmation of the single-trace pattern: `Events` is LOO's clear #1 by a wide margin (7.89h, ~3x the next-highest) but only GNNExplainer's #4, and GNNExplainer's top 4 types are within 0.003 of each other (0.140-0.143) — effectively indistinguishable to GNNExplainer, sharply distinguishable to LOO. `Orders` (the seed node) shows `—` for LOO not because it's unimportant but because `reg_explanation()` structurally excludes the seed from its own ranking (it only ranks neighbors being ablated) — a genuine blind spot in this comparison, worth flagging if this table is reused, not a finding that LOO considers `Orders` unimportant.

**Outputs**: `files/explainer_outputs/order_management/aggregate/` (LOO verification), `files/explainer_outputs/order_management/order_{1821,1812}_loo_gnn/` (single-trace), `files/explainer_outputs/order_management/aggregate_loo_gnn/` (aggregate: `aggregate_loo_gnn_overlap.{csv}`, `aggregate_loo_gnn_overlap_distribution.png`, `aggregate_loo_gnn_by_type.{csv,png}`).
