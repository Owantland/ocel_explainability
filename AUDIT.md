# Codebase Audit — OCEL Explainability

This document is a snapshot audit of the repository: what the project is trying to do, how the pipeline actually works end to end, and a prioritized list of things worth cleaning up. It exists because the design knowledge currently lives mostly in inline dev-log comments (`db_testing.py`, `training.py`) and one XAI-scoped doc (`EXPLAINABILITY.md`) — there was no single map of the whole system.

## 1. End Goal

The thesis extracts **Object-Centric Event Logs (OCEL)** from relational process-mining databases (`order_management`, `logistics`), encodes each process trace as a growing sequence of **heterogeneous graphs** (one per event prefix), trains a **Heterogeneous Graph Transformer (HGT)** to predict a KPI — primarily **remaining time until a target event** (a regression task) — and applies **four post-hoc explainability methods** to the trained model:

1. **Leave-One-Out (LOO)** feature/node/edge importance
2. **Fidelity+/Fidelity−/Characterization/sparsity** metrics quantifying explanation quality
3. **InputXGradient** (Captum) gradient-based feature attribution
4. **Counterfactual retrieval** — nearest real test-set trace with an opposite outcome, ranked by a 4-part graph dissimilarity score

A secondary **binary classification** KPI task is designed for (config, model class, explainer support all exist) but its training path is currently an explicit stub (`training.py:738`, `BinaryModelling` raises `NotImplementedError`).

**Relation to prior work** (per project memory):
- Extends **Adams et al. 2022** — `db_testing.py`'s `verify_ocel_generator` prints an explicit "Adams et al. feature coverage audit" table (C1–C3, D1–D3, R1–R3, P1–P10, O1–O6 feature categories), and commit `c8353d2` implemented the remaining C3 (activity-frequency) and O1-ext (per-event object counts) features to complete that coverage.
- Competes with **HOEG (Smit et al. 2024)** — `db_testing.py`'s `compare_to_hoeg()` is an inline related-work comparison table.
- The **HGT architecture**, **InputXGradient attribution**, and the **4-part counterfactual dissimilarity metric** (feature L2+cosine, node-type Jaccard, edge-type Jaccard, structural difference) are adapted from **Zhai et al. 2025**, whose reference implementation is vendored at `GNN-land-use-main/` (`main_hetero.py`, `explainability_FA.py`, `counterfactual_explanations.ipynb`).
- The counterfactual explanation approach is conceptually inspired by **LORELEY (Huang et al. 2022)** — `explainer.py`'s `explain_counterfactual()` retrieves real contrasting test-set traces rather than LORELEY's synthetic GA-generated neighbours, but shares the same goal of contrasting a query trace against an opposite-outcome counterpart under process/control-flow constraints.
- Fidelity framing is loosely in the spirit of **Stevens et al. 2022**, but there is no literal citation in code/docs, and the implemented metric set (Fidelity+/−, Characterization, node/edge sparsity) is narrower than that paper's full parsimony/monotonicity/faithfulness framework — no monotonicity or parsimony metric exists in this codebase.
- **SHAP**, cited in prior research notes as related/planned work, is **not implemented and not installed** in either project venv. Either it was superseded by InputXGradient or the notes describe an aspiration that was dropped — worth reconciling in the thesis writeup.

## 2. Pipeline Architecture

```
sqlite OCEL DB (files/databases/*.sqlite)
        │  process_generation.py — ProcessGeneration
        ▼
ev_log.csv, all_kpis.csv          files/graph_structures/<db>/<cant>/
        │  ocel_generator.py — Generator
        ▼
ocel.csv, edges.csv, tensor_dict.json   (same dir)
        │  train_test_builder.py — TrainTestBuilder (splits by viewpoint id / timestamp)
        ▼
train / val / test viewpoint-id samples
        │  hetero_graphs.py — HeteroGraphsGenerator
        ▼
train_graphs_sg.pt / val_graphs_sg.pt / test_graphs_sg.pt   files/hetero_structures/<db>/
        │  training.py — Modelling
        ▼
HGT + HomoGNN(GCN) checkpoints, norm stats, per-epoch logs   files/models/<db>/Hetero/*.pth
        │  explainer.py — Explainer(Modelling)
        ▼
LOO / fidelity / InputXGradient / counterfactual outputs     files/explainer_outputs/<db>/
```

`cant` ("cantidad") is a **dataset-size** parameter — the number of viewpoint objects (traces) sampled — not a different experiment variant. Observed sizes: `order_management/{10,20,100,200,1000,2000,4000}`, `logistics/{1000,2000}`.

### Stage-by-stage

- **`process_generation.py` (`ProcessGeneration`)** — reads the sqlite OCEL DB and produces the flat event log (`ev_log.csv`: `ocel_id, type, timestamp, vwpnt_id, ob_id`) and KPI table (`all_kpis.csv`: `timestamp, kpi_val, viewpoint_id, kpi_event, ob_id, ob_idx`).
- **`ocel_generator.py` (`Generator`)** — produces `ocel.csv` (one row per event, with per-object-type `{Type}::ids` / `{Type}::attributes` / `{Type}::idx` columns, stringified for storage) and `edges.csv` (one column per edge type, e.g. `Orders_to_Items`, holding stringified `[[src...],[dst...]]` adjacency), plus `tensor_dict.json` (base per-node-type / per-edge-type feature dimensionality).
- **`train_test_builder.py` (`TrainTestBuilder`)** — builds temporally-non-overlapping train/val/test id splits. `get_active_orders` trims `ev_log.csv` per viewpoint object; ~~this used to rewrite the file in place once per viewpoint object inside the loop (O(n) full-file rewrites, a correctness risk if interrupted mid-run)~~ **RESOLVED** — it now performs a single `self.pd_df.to_csv(...)` after the loop completes (`train_test_builder.py:54-56`), so a failure partway through no longer leaves `ev_log.csv` half-trimmed on disk.
- **`hetero_graphs.py` (`HeteroGraphsGenerator`)** — the core prefix-graph builder (`get_learning_set`, `hetero_graphs.py:95`). For every event in a trace it emits one `HeteroData` snapshot containing everything observed so far: one-hot event type, 6 temporal features (elapsed/waiting hours, cyclical hour-of-day and day-of-week), **C3** (cumulative per-activity-type counts) and **O1-ext** (per-event object-type participation counts) appended to `Events`; object node features built from `::attributes` columns plus aggregate `Orders` features (`n_items`, `total_weight`, `n_products`, `n_packages`) when present. The regression target `y` is the KPI value where available, else `max(0, end_time − current_time)`; when the KPI-bearing object type has multiple instances per prefix (e.g. several `Packages` under one `Orders`), each gets its own `y` and a `mask` so untracked instances don't contribute to loss (`hetero_graphs.py:206–235`). Output is saved via `trace_kpi()` to `train/val/test_graphs_sg.pt`.
- **`training.py` (`Modelling`)** — loads the three `.pt` splits, builds `task_id`:
  - regression (`kpi_type: 0`): `TimeFrom_{kpi_viewpoint}_to_{kpi_event}` (e.g. `TimeFrom_Packages_to_PackageDelivered`)
  - classification (`kpi_type: 1`): `Classifier_{kpi_event}`
  z-normalizes targets and continuous node features (mean/std cached in a `_norm.json` sidecar so normalization survives graph regeneration), builds `model_classes.HGT` (regression) or `HGT_CLASS` (classification) via `_build_model`. `Modelling()` (`training.py:741`) dispatches to `Het_Reg_Modelling` for regression, or the **stub** `BinaryModelling` (`training.py:738`, always raises `NotImplementedError`) for classification. Also provides `sweep()` (Optuna hyperparameter search), `Homo_Reg_Modelling`/`_hetero_to_homo` (a homogeneous GCN baseline trained on the Events-only subgraph, for comparison), `compare_models()` and `plot_training_curves()`.
- **`explainer.py` (`Explainer(Modelling)`)** — inherits the trained model and test set; implements the four XAI methods described in §1 and documented in detail in `EXPLAINABILITY.md`. Entry points used by `pipeline_2000.py`/`validate.py`: `explain_feature_attribution()` (InputXGradient, aggregate only), `explain_aggregate(n_traces, top_k)` (LOO, aggregate), `explain_trace(order_id)` (LOO, single trace), `explain_counterfactual(order_id)` (single trace only — no aggregate mode exists yet).

### Config model (`files/config.yml`)

Per-dataset keys consumed by `sup_funcs.SupportFunctions.get_paths()`: `ocel_path`, `graph_output_path`/`pytorch_path`/`model_output_path`/`explainer_output_path`, `viewpoint`, `added_depth`, `unique_ids`, `kpi_type`, `kpi_event`, `kpi_viewpoint`, `filtered_tables`, `attributes`, `time_attributes`, `encoding`, and (order_management only) `role_encoding`.

Two terms look similar but mean different things and are a genuine maintenance trap:
- `viewpoint` — the object type that defines a *process trace* (`Orders` for order_management, `CustomerOrder` for logistics).
- `kpi_viewpoint` — the object type the **prediction target and model output node** are attached to (`Packages` for order_management, `TransportDocument` for logistics) — can differ from `viewpoint`.
- In `training.py`, `self.viewpoint_object` (`training.py:28`) is bound to `kpi_viewpoint`, not `viewpoint` — the attribute name doesn't match what it holds.

The `aoe` and `iot` blocks in `config.yml` are **legacy and non-runnable** through the current pipeline: they lack `kpi_type`, `attributes`, `model_output_path`, etc., and use a different path-key scheme (`ev_output_path`/`ob_output_path`/`ocel_output_path`) consistent with an earlier pipeline generation (`old_version/`).

### Explainability outputs (`files/explainer_outputs/`)

`order_management` has full coverage: `aggregate/` (LOO), `attribution/` (InputXGradient), plus training/validation plots (`validation_2000/`, `validation/`). `logistics` is sparse (only `validation_2000/homo_comparison.png` and a sweep plot) — consistent with `logistics` support being newer/partial (see §3). No canonical single-trace LOO output or counterfactual output (`cf_node_type_comparison.png`) exists under `files/explainer_outputs/` for either dataset — those artifacts currently only appear under the scratch `explainer_tests/` directory, meaning the worked examples quoted in `EXPLAINABILITY.md` aren't backed by a saved artifact in the canonical output tree.

### Model/data artifact map

- `files/graph_structures/<db>/<cant>/` — CSV/JSON intermediates (`ev_log.csv`, `all_kpis.csv`, `ocel.csv`, `edges.csv`, `tensor_dict.json`), namespaced by dataset size.
- `files/hetero_structures/<db>/` — the three `.pt` graph-list files. **Not** namespaced by `cant` (see §3).
- `files/models/<db>/Hetero/<task_id>.pth` plus sidecars: `_norm.json` (target mean/std), `_training_log.csv`, `_homo.pth` + `_homo_training_log.csv` (baseline counterpart), legacy `_arch.json`, and (for one task) `_pgexplainer.pt` evidencing a PGExplainer run. `model_params.json` is a single shared hyperparameter cache keyed by `task_id`.

## 3. Prioritized Recommendations

### High priority — correctness / reproducibility risk

- **`files/hetero_structures/<db>/` and `model_output_path` are not namespaced by `cant`.** Every rerun at a different dataset size silently overwrites the previous `train/val/test_graphs_sg.pt` and, for a given `task_id`, the previous `.pth`/`_norm.json`/training log — there's no `cant` in `task_id` (`training.py:42,66`). If results at different scales need to be compared later, they'll already be gone. Fix: include `cant` in the `pytorch_path`/`model_output_path` templates, or in `task_id`.
- **No dependency manifest** (`requirements.txt` / `pyproject.toml`) at the repo root, despite two divergent local venvs (`.venv` Python 3.11 with the active stack incl. torch 2.12/torch-geometric 2.8/captum 0.9, `.venv1` Python 3.13, much smaller). Reproducing the environment currently depends on tribal knowledge. Fix: `pip freeze` the working `.venv` into a checked-in manifest; decide whether `.venv1` should be deleted or documented.
- ~~**The `db_testing.py` validation suite is dataset-schema-specific but not dataset-parameterized in practice.**~~ **RESOLVED — and was already resolved when this line was originally written.** `verify_process_generation`, `verify_ocel_generator`, `verify_hetero_graphs`, and `compare_to_hoeg` all default to `database='logistics'` and are fully config-driven (read object types/attribute columns from `path_dict` instead of hardcoding `order_management`'s schema), per commits `5d6d920` and `33175c0` — both of which predate this audit document. The real, separate issue found on re-verification (2026-07-11): `db_testing.py`'s module-level block (training run on import, no dataset relation) had no `if __name__ == '__main__':` guard, so `import db_testing` would silently trigger a full model training run as a side effect. Fixed by adding the guard; the hand-toggled comment structure inside is otherwise untouched.

### Medium priority — clarity / consistency

- **`viewpoint` vs `kpi_viewpoint` vs `self.viewpoint_object`** naming overload (see §2) — rename `self.viewpoint_object` to `self.kpi_viewpoint` (or similar) in `training.py` and `explainer.py` to match what it actually holds.
- **`db_testing.py` is not a test file** — it's the pipeline's module-level entry point (hardcoded `database='logistics'; cant=2000`, executes the full pipeline on import) *plus* a data-validation function library *plus* a HOEG comparison writeup. Splitting these three responsibilities (e.g. `run_pipeline.py`, `validation.py`, keep `db_testing.py` name only if it's ever turned into real pytest tests) would make the codebase much easier to navigate for a grader or collaborator.
- **The classification path is half-migrated.** `HGT_CLASS` model class and `Explainer.class_evaluate_explanation`/`class_explanation` exist and presumably worked at some point (artifacts `Classifier_CreatePackage.pth`, older `BinaryClass_CreatePackage.pth`, `het_BinaryClassifier.pt` are on disk), but `training.Modelling.BinaryModelling` is a hard stub today, and neither active dataset config sets `kpi_type: 1`. Decide: finish `BinaryModelling` to match the regression training loop, or drop the classification scaffolding (model class + explainer branch + stale checkpoints) if it's out of scope for the thesis.
- **Unused `batch_size` config key.** `training.py` reads `self.path_dict.get('batch_size', 16)` in three places, but `batch_size` is never set in `config.yml` — it always silently uses the hardcoded default. Either add it to config (if it's meant to be tunable) or remove the `.get(...)` indirection.

### Low priority — housekeeping

- **`evaluation.py` appears to be dead code** — it imports `OrderPredictionHeteroGNN_2` from `explainer_tests/model_class.py` and loads `GAT_sg_{1..5}.pth` checkpoints, neither of which are used by any current pipeline script (`training.py`, `explainer.py`, `pipeline_2000.py`, `validate.py`, `db_testing.py` all use `model_classes.HGT`/`HGT_CLASS`). Confirm it's unused and delete, or document why it's kept.
- **Multiple generations of legacy artifacts coexist with current ones** with no cleanup: `old_version/` (full prior pipeline generation, including `old_version/*/heterogeneous_graphs.py`), `shelved_ideas/`, `explainer_tests/` (PyG/Captum API scratch scripts against toy datasets like DBLP/Reddit), orphaned model files (`BinaryClass_CreatePackage.pth`, `TimeUntil_Depart.pth`, per-KPI legacy directory trees under `files/models/order_management/{CreatePackage,PackageDelivered,PayOrder}/`), and stale pre-refactor intermediates (`all_graphs.json`/`all_idx.json`/`all_timestamps.json` in one `graph_structures/order_management/1000/` folder, `*_hom.pt` files in `hetero_structures/logistics/`). None of this is tracked in git (`files/`, `old_version/`, etc. are gitignored), so cleanup is low-risk — it's about not confusing future-you (or a thesis committee member poking around the repo) about which artifacts are current.
- **`EXPLAINABILITY.md`'s own "Proposed Improvements" section** already lists several concrete, author-acknowledged gaps worth revisiting before finalizing: no single-trace InputXGradient entry point, no aggregate counterfactual mode, LOO edge-masking perturbation strength not comparable to feature-zeroing perturbation strength, event indices not decoded to human-readable names in explanation output, fidelity metrics reported in seconds rather than hours (unit mismatch with the rest of the system).

## 4. Confirmed vs. Reported

The claims above were spot-checked directly against `files/config.yml`, `hetero_graphs.py`, and `training.py` (line numbers cited are current as of this audit). Findings about `explainer.py`, `EXPLAINABILITY.md`, `GNN-land-use-main/`, `files/explainer_outputs/`, and the legacy directories (`old_version/`, `shelved_ideas/`, `explainer_tests/`, `evaluation.py`) are drawn from a thorough codebase exploration but were not re-verified line-by-line in this pass — worth a light re-check before citing specific line numbers from that side in the thesis text.
