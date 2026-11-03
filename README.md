# OCEL Explainability

Predictive process monitoring on Object-Centric Event Logs (OCEL 2.0): a
Heterogeneous Graph Transformer (HGT) predicts each process instance's
remaining time to a target KPI event, and four post-hoc explainability
methods (LOO, Shapley, feature attribution, counterfactuals) explain why.
Two datasets are configured: `order_management` (KPIs: PayOrder,
PackageDelivered) and `logistics` (KPIs: Depart, LoadToVehicle).

## Pipeline: OCEL database → explanation

Everything below is one fixed stage order (`run_pipeline.py`'s
`STAGE_ORDER`), each stage mapping 1:1 onto a root module. Run any subset
with `PYTHONPATH=. python3 run_pipeline.py --database <order_management|
logistics> --cant <int> --stages <comma-separated subset>` (all three flags
required; stages always execute in the order below regardless of the order
listed on the CLI). `--selection-metric {pooled,last_event}` optionally
changes how `sweep`/`train` pick their best checkpoint (default `pooled`).

| # | Stage | What it does | Module / entry point |
|---|---|---|---|
| 1 | `regenerate` | Rebuilds `ev_log.csv`/`all_kpis.csv` and `ocel.csv`/`edges.csv`/`tensor_dict.json` from the raw OCEL SQLite DB | `process_generation.ProcessGeneration` → `ocel_generator.Generator` |
| 2 | `split` | Temporal (non-leaking) train/val/test split over trace ids | `train_test_builder.TrainTestBuilder.timestamps_generator()` |
| 3 | `graphs` | Builds cumulative PyG `HeteroData` prefix-graphs, one per prefix of each trace | `hetero_graphs.HeteroGraphsGenerator.trace_kpi()` |
| 4 | `sweep` | Optuna hyperparameter search | `training.Modelling.sweep()` |
| 5 | `train` | Trains the HGT regression model | `training.Modelling.Modelling()` |
| 6 | `homo` | Trains the homogeneous GCN baseline ("HomoGNN") | `training.Modelling.Homo_Reg_Modelling()` |
| 7 | `kdim` | Trains HOEG's own k-dimensional GNN baseline | `training.Modelling.KDim_Reg_Modelling()` |
| 8 | `validate` | Scores a trained checkpoint on the held-out test set (MAE/RMSE/R², by-depth breakdown) | `explainer.Explainer` (subclasses `Modelling`) |
| 9 | `compare` | Compares HGT against all baselines | `explainer.Explainer.compare_to_baselines()` / `baselines.py` |
| 10 | `explain` | Runs the post-hoc explainability layer (feature attribution, aggregate + single-trace explanations) | `explainer.Explainer.explain_*()` |

`model_classes/` holds the three `torch.nn.Module` definitions these stages
train: `HGT.py` (primary model), `REG_GNN.py` (HomoGNN baseline),
`KDIM_GNN.py` (HOEG's k-dim GNN baseline). `sup_funcs.py` is a shared utility
(`files/config.yml` path resolution) imported by every stage above.

The **explainability layer** (`explainer.py`, stage 10 and beyond) is the
thesis's actual contribution: Leave-One-Out (LOO) importance identifies
candidate important elements cheaply; Shapley values quantify their impact
in hours (`explain_trace_shapley`, the production single-trace method);
Captum-based feature attribution and counterfactual retrieval provide two
independent cross-checks. See `thesis_parts/latex_template/chapters/results.tex`
(§"Explainability framework") for the full writeup, and `thesis_scripts/`
for the scripts that regenerate every cited figure/table from these outputs.

## How to verify it works

Both commands below run against an **already-trained checkpoint** — no
retraining, no sweep, done in seconds. (The pipeline has no bundled toy
dataset or `--cant 10`-style fast path for a from-scratch run; reproducing
stages 1-7 from zero requires the real configured databases under
`files/databases/`.)

**Tier 1 — fast correctness check** (scores the test set, prints MAE/RMSE/R²):
```
PYTHONPATH=. python3 run_pipeline.py --database order_management --cant 2000 --stages validate
```

**Tier 2 — deeper sanity check** (6 PASS/FAIL groups: artifact existence, raw
data integrity, normalization correctness, forward pass/gradient flow,
checkpoint round-trip, sweep/train consistency):
```
PYTHONPATH=. python3 debug_scripts/sanity_check.py --database order_management --cant 2000
```
(`--database logistics --cant 1000` and `--database cargo_pickup_iot --cant 2000` work the same way for the other datasets.)

## Interactive demo

```
PYTHONPATH=. streamlit run dashboard/app.py
```
Explores the held-out test set of a trained model interactively — not a
live process-monitoring feed. Pick any of the five KPIs in the sidebar
(registry: `dashboard/kpis.py`; each KPI loads its own graph cache, so no
`config.yml` edit or cache swap is needed), then:

- **Perturbation → Single trace**: LOO identification + Shapley
  quantification for one trace at any prefix length, plus an
  attribution-vs-Shapley feature comparison.
- **Perturbation → Aggregate**: top-K nodes/features from the KPI's
  precomputed last-event aggregate (read-only; the all-prefix/normalized
  global Shapley analyses are not shown).
- **Counterfactual**: the most similar test-set trace with a contrasting
  prediction, drawn as a union graph; "Load curated example" opens the
  thesis example for the KPI.

Optionally pre-warm the single-trace cache before a live demo with
`PYTHONPATH=. python3 dashboard/precompute.py` (add `--kpi <slug>` to limit
it to one KPI). Dashboard outputs go to
`files/explainer_outputs/<db>/dashboard/<task_id>/`, never into the
thesis-cited folders.

## Repo map

- `thesis_parts/latex_template/` — the real, compiled LaTeX source (everything
  under `thesis_parts/*.txt` at the top level is a superseded early draft,
  archived in `thesis_parts/archive/`).
- `thesis_scripts/` — one script per thesis figure/table, writing to
  `thesis_parts/figures_tables/` (staging) and promoting into
  `thesis_parts/latex_template/figures/`. See its `README.md`.
- `docs/archive/` — dated investigation/decision logs from throughout the
  project (hyperparameter audits, viability checks, verification passes).
  Historical record, not living specs.
- `files/` — generated artifacts (databases, graph caches, model checkpoints,
  explainer outputs); gitignored, regenerated by the pipeline stages above.
- `debug_scripts/`, `experiments_archive/` — standalone verification scripts
  and retired one-off pipeline runs, each with its own `README.md`.
- `GNN-land-use-main/`, `OCPPM-master/` — vendored reference implementations
  for cited papers (Zhai et al. 2025; Smit et al. 2024/HOEG), not this
  project's own code.
