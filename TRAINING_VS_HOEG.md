# Training Methodology vs. HOEG (Smit et al. 2024)

`db_testing.py`'s `compare_to_hoeg()` covers the *graph structure* differences between this project and HOEG (node/edge typing, GNN layer choice, target granularity — see that function and `AUDIT.md`). This document covers the piece that comparison leaves out: how the regression model is actually **trained and evaluated** — `training.py`'s `Het_Reg_Modelling` (plus `sweep`, `Homo_Reg_Modelling`, `compare_models`) — against HOEG's Section 5/6 methodology (`files/papers/HOEG-New_Approach_4_OCPPM.pdf`).

## 1. Architecture

| | HOEG | This project (`model_classes/HGT.py`) |
|---|---|---|
| Pre-message-passing | 0 layers | 1 linear projection **per node type** + ReLU, before the conv stack |
| Message-passing | 2 layers, k-dimensional GNN (Morris et al. 2019), fixed | `num_layers` stacked `HGTConv` (attention-based), default 2, tunable 1–2 |
| Between-layer activation | PReLU | **none** — `HGTConv` layers are stacked with no activation in between |
| Post-message-passing | 1 layer | 1 final `Linear` read-out (matches in spirit) |
| Dropout | 0.0 | none (equivalent) |

Two things worth flagging:
- This project's per-node-type linear projection is a genuine pre-message-passing layer, which HOEG's own config table explicitly says it doesn't use (their k-dimensional GNN takes features more directly). This isn't wrong — PyG's standard HGT pattern needs a shared-dimension projection before `HGTConv` can run — but it means the two architectures aren't apples-to-apples at the input stage.
- HOEG's stack includes PReLU; this project's stacked `HGTConv` layers have no activation between them at all (see recommendation 4).

## 2. Training loop / hyperparameters (`Het_Reg_Modelling`, `training.py:252-328`)

| | HOEG (Table 5) | This project |
|---|---|---|
| Batch size | 16 (resource-constrained, not principled) | 16 (same constraint, `path_dict.get('batch_size', 16)`) |
| Epochs / early stopping | 30 / patience 4 — **used uniformly** for tuning and final results | Sweep: 30 epochs, patience 4. **Final training: also 30/4** as of 2026-07-09 (`self.max_epochs`/`self.early_stop_patience`, `training.py:38-39`) — resolved, see recommendation 1. |
| Optimizer | Adam | Adam |
| Learning rate | fixed choice from `{0.01, 0.001}`, no scheduler | same grid during sweep, **plus** `ReduceLROnPlateau` (factor 0.5, patience 10) during training |
| Weight decay | not mentioned | 1e-5 (regression defaults) vs. **hardcoded 0.001 during `sweep()`** — internal inconsistency |
| Gradient clipping | not mentioned | `clip_grad_norm_(max_norm=1.0)` |
| Target normalization | z-score on train split only (leakage prevention) | same, plus `_norm.json` sidecar caching so re-running doesn't silently shift normalization |

**Resolved 2026-07-09** (see recommendation 1): the epoch/patience mismatch was the most material finding here — HOEG deliberately uses **one** small budget everywhere so tuning and reported results are directly comparable, but `Het_Reg_Modelling`/`Homo_Reg_Modelling` previously trained for over 3x longer with 2.5x the patience versus `sweep()`, as an undocumented accident of two numbers living in different places rather than a stated methodological choice. Fixed by aligning `self.max_epochs`/`self.early_stop_patience` down to 30/4 to match `sweep()` and HOEG's Table 5/7 exactly. All existing checkpoints (order_management/PayOrder, logistics/CustomerOrder_to_Depart) need retraining under the new regime — see `project_presentation_checkpoint_churn` memory for retrain status.

## 3. Hyperparameter tuning strategy

**Updated 2026-07-04**, after `enqueue_trial`-based informed priors (recommendation 5, below) were implemented and after this session's explainability investigation surfaced a concrete reason to revisit `num_layers` specifically, not just as a style preference.

This project's `sweep()` (`training.py:335-448`; Optuna, `n_trials=30`, `MedianPruner(n_warmup_steps=10)`) searches:
- `hidden_channels ∈ {8,16,24,32,48,64,128,256}` (8 values) — **identical** to HOEG's Table 5 grid
- `lr ∈ {0.001, 0.01}` (2 values) — **identical** to HOEG's Table 5 grid
- `num_layers ∈ {1,2}` (2 values) — HOEG **fixes** message-passing depth at 2 and never tunes it
- `num_heads ∈ {1,2}` (2 values) — HOEG's k-dimensional GNN has **no attention-head concept at all**, so this dimension has no HOEG analogue to align with or diverge from — it's simply extra search cost this project's HGT-based architecture requires that HOEG's doesn't

Nominal total: 8 × 2 × 2 × 2 = **64** combinations, vs. HOEG's own **16** (8 × 2). The entire 4x gap is exactly the two dimensions HOEG doesn't tune.

**This is no longer just a style/parity question.** `EXPLAINABILITY_DEPTH.md` (this session) found that `num_layers=1` — which `sweep()` is free to select, and did, for both `TimeFrom_Orders_to_PayOrder` and `TimeFrom_TransportDocument_to_Depart` — makes any node type without a direct edge to the viewpoint *provably* unreachable, regardless of training quality (`Employees`/`Products`/`Packages` in `order_management`; `Forklift`/`HandlingUnit`/`Truck` in `logistics`). The sweep is currently picking the architecture that causes this, purely because it scores a marginally better validation MAE over a 30-epoch budget — with no accounting for the interpretability cost.

HOEG ran the equivalent search manually (Section 6.1, Figure 2) rather than with an automated optimizer, and **published their findings**: lower learning rate (0.001) generally scores better across all three datasets, `hidden_dims=256` performed best on their more-structured datasets (BPI17, OTC), while `hidden_dims=64` performed best on their messier real-world dataset (FI). This is already the basis for the `enqueue_trial` priors added under recommendation 5 — but priors only *seed* the search, they don't *constrain* it, so `sweep()` can and still does wander back to a worse choice given enough trials.

**Also now grounded in a real measurement, not just secondhand anecdote**: retraining `logistics`/`Depart` this session (`hidden_channels=128, num_layers=1`) took ~285s/epoch — direct evidence that `logistics`'s scale makes large `hidden_channels` choices expensive in wall-clock terms, not just (per HOEG's finding) worse for messy data.

## 4. Evaluation methodology

- **Metrics**: both report MAE and MSE/RMSE across train/val/test splits. This project's `compare_models()` and `baselines.py` go further — R², last-event-only slices, and prefix-depth-stratified MAE (1-3/4-6/7-9/10+ events seen) — none of which appear in HOEG's paper. This is a genuine strength, not a gap: the depth-stratified view in particular gives insight HOEG's aggregate-only reporting doesn't.
- **Baselines**: HOEG compares against Median, LightGBM, EFG, and EFG_ss (subgraph-sampling). `baselines.py` compares against Mean, GBT, and the HomoGNN (this project's EFG-equivalent, confirmed via the earlier HOEG summary work) — three of HOEG's four baseline categories, missing only an EFG_ss-style subgraph-sampling variant.
- **Scalability**: HOEG treats fitting time and prediction time as first-class reported metrics (their Table 7 reports both, in seconds, for every model/dataset combination). **Neither `training.py` nor `baselines.py` currently measure or report wall-clock training/inference time anywhere.** This is the clearest concrete gap versus the paper's own methodology — there's no way to currently make a scalability claim (or rebut one) the way HOEG's own results table does.

## 5. Recommendations

1. ✅ **[Implemented, 2026-07-09]** Reconciled the epoch/patience mismatch — `Het_Reg_Modelling`'s (and `Homo_Reg_Modelling`'s, which shares the same `self.max_epochs`/`self.early_stop_patience` attributes) final-training budget now matches `sweep()`'s (30/4), restoring direct comparability with HOEG's Table 5/7 single-budget methodology. Requires retraining all existing checkpoints.
2. ✅ **[Implemented]** Wall-clock timing added to `compare_models()` and `baselines.py` (fitting-time/prediction-time table, mirroring HOEG's Table 7).
3. ✅ **[Implemented, 2026-07-04]** `weight_decay` inconsistency fixed — `sweep()`'s hardcoded `0.001` now matches the regression defaults' `1e-5` (`training.py:367,444`). This turned out to matter more than "pick one deliberately": the old `0.001` value was directly responsible for several node types' per-type input projections collapsing to exactly zero during training — see `EXPLAINABILITY_DEPTH.md`.
4. ✅ **[Implemented]** PReLU activation added between stacked `HGTConv` layers in `model_classes/HGT.py`, matching HOEG's own choice.
5. **Use HOEG's Section 6.1 findings as an informed prior for the Optuna search — updated 2026-07-04, now split into concrete, curtailed-search-space steps**, since seeding priors alone (already implemented via `enqueue_trial`) doesn't stop the sweep from still wandering back to a worse, unreachability-inducing choice:
   - **5a. Fix `num_layers=2`, remove it from the tuned dimensions.** Matches HOEG's own fixed depth *and* independently resolves the `EXPLAINABILITY_DEPTH.md` reachability pathology — no longer just a style choice. Note honestly: this removes the *cheaper* option from `logistics`'s pool, so every remaining trial there is guaranteed to cost the pricier 2-layer pass; 5c is what offsets that.
   - **5b. Fix `num_heads=2`, remove it from the tuned dimensions.** No HOEG grounding either way — tuning it is pure added cost for a parameter HOEG's architecture doesn't have.
   - Net effect of 5a+5b: search space drops from 64 → 16 combinations, exactly matching HOEG's own grid size.
   - **5c. Cap `n_trials` to match the now-exhaustible 16-combo space** (e.g. `n_trials=16`), or switch to `optuna.samplers.GridSampler` for guaranteed non-duplicate coverage — with TPE and `n_trials=30` over only 16 possible combinations, roughly half the trials are likely re-evaluating something already tried.
   - **5d. Dataset-conditional `hidden_channels` ceiling for `logistics`**: exclude `{128, 256}` from `logistics`'s choices specifically (grid → `{8,16,24,32,48,64}`, 6 values, 12 combinations with `lr`), leaving `order_management`'s full 8-value range untouched. Grounded in both HOEG's literature finding (smaller `hidden_dims` suit messier data) and a direct measurement from this session (128-dim `logistics` epochs cost ~285s each) — this is what buys back 5a's per-trial cost increase specifically where it matters.
   - **5e. Minor, not a speed lever**: `ReduceLROnPlateau(patience=10)` inside a trial that early-stops at `patience=4` can never actually fire — worth a comment or removal for clarity, not performance.
   - **5f. Minor, small speed lever**: lower `MedianPruner`'s `n_warmup_steps` (currently 10) to ~5, since `patience=4` currently stops most bad trials before the pruner ever gets a chance to.
6. **Consider adding a k-dimensional GNN baseline** (Morris et al. 2019, available directly in PyG) alongside the existing HomoGNN/GCN baseline. This would let the thesis make a controlled, same-data comparison against HOEG's actual architecture, rather than relying only on their published numbers from different datasets (BPI17/OTC/FI vs. order_management/logistics) for positioning.
