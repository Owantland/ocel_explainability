# OCEL Explainability

Predictive process monitoring on Object-Centric Event Logs (OCEL 2.0): a
Heterogeneous Graph Transformer (HGT) predicts each process instance's
remaining time to a target KPI event, and four post-hoc explainability
methods (LOO, Shapley, feature attribution, counterfactuals) explain why.
Three datasets and five KPIs are configured: `order_management` (PayOrder,
PackageDelivered), `logistics` (Depart, LoadToVehicle) and
`cargo_pickup_iot` (EvaluateTruckExit).

**Just want to see it?** Jump to [Run the dashboard](#run-the-dashboard):
after cloning, `python3 run_dashboard.py` sets everything up and opens it.

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

## Run the dashboard

An interactive explorer for the held-out **test set** of the five trained
models (already-completed traces, not a live process-monitoring feed).

### Quick start

You need **Python 3.11**, **git**, an internet connection for the first run,
and about **2 GB of free disk**: ≈1.2 GB for the Python environment
(macOS; Linux's default CUDA build of PyTorch is larger), an 81 MB download
and 0.63 GB of unpacked data.

```
git clone https://github.com/Owantland/ocel_explainability.git
cd ocel_explainability
python3 run_dashboard.py
```

(On Windows use `py -3.11 run_dashboard.py`. If `python3` on your machine
isn't 3.11, run it with `python3.11 run_dashboard.py`; the script also looks for
`python3.11` on your `PATH` by itself.)

The **first run** takes a few minutes. It:
1. creates a virtual environment in `.venv/` and installs
   `requirements-dashboard.txt` into it (PyTorch is the big part);
2. downloads the trained models and test-set data (one archive from this
   repo's [GitHub Releases](https://github.com/Owantland/ocel_explainability/releases/tag/dashboard-data-v1)),
   checks its checksum and unpacks it under `files/`;
3. starts the dashboard at **http://localhost:8501**. Open that address in
   your browser if it doesn't open by itself.

**Later runs** skip whatever is already in place and start in seconds. Stop
the dashboard with `Ctrl+C`.

To check the setup without starting the server, run
`python3 run_dashboard.py --check`. It installs and downloads whatever is
missing, then verifies every file's checksum and the imports.

### What gets downloaded

The archive is listed file by file, with sizes and SHA-256 checksums, in
`dashboard/data_manifest.json`. It contains:
- the trained HGT checkpoint and normalization stats for each KPI;
- each KPI's **test-split** graph cache;
- the three OCEL SQLite databases;
- the event-log CSVs;
- the precomputed aggregate explanations.

It doesn't contain the training/validation graph caches. Without those the
dashboard can explain the trained models but can't retrain them. The
training pipeline below needs the full data.

### Using it

Pick any of the five KPIs in the sidebar (registry: `dashboard/kpis.py`).
Each KPI loads its own graph cache, so no `config.yml` edit or cache swap is
needed. Then choose a view:

- **Perturbation → Single trace**: LOO identification + Shapley
  quantification for one trace at any prefix length, plus an
  attribution-vs-Shapley feature comparison.
- **Perturbation → Aggregate**: top-K nodes/features from the KPI's
  precomputed last-event aggregate (read-only; the all-prefix/normalized
  global Shapley analyses are not shown).
- **Counterfactual**: the most similar test-set trace with a contrasting
  prediction, drawn as a union graph; "Load curated example" opens the
  thesis example for the KPI.

Before a live demo, you can pre-warm the single-trace cache:

```
PYTHONPATH=. .venv/bin/python dashboard/precompute.py
```

Add `--kpi <slug>` to limit it to one KPI. This makes the first
"Explain this trace" click on the demo traces instant instead of taking
seconds to minutes.

Dashboard outputs go to `files/explainer_outputs/<db>/dashboard/<task_id>/`
and are never written into the thesis-cited folders.

### Manual setup (your own environment)

If you manage Python environments yourself (conda, an existing venv), these
steps do the same as the script:

```
python3.11 -m venv .venv && source .venv/bin/activate   # or your own env
pip install -r requirements-dashboard.txt
python run_dashboard.py --data-only                      # download + verify the data only
PYTHONPATH=. streamlit run dashboard/app.py
```

`python run_dashboard.py --skip-install` also uses the active interpreter
instead of `.venv`. For the training and research scripts, install the full
`requirements.txt` instead.

### Troubleshooting

- **"Python 3.11 is required"**: every pin was resolved on 3.11. Install it
  (python.org, `brew install python@3.11`, `apt install python3.11`, or
  `pyenv install 3.11`). Then delete `.venv/` if an earlier run created it
  with another version, and rerun.
- **Port 8501 already in use**: pass Streamlit options after `--`, e.g.
  `python3 run_dashboard.py -- --server.port 8502`.
- **Download interrupted or corrupt**: just rerun. Missing or changed files
  are detected and fetched again. If the download keeps failing, download the
  archive from the release page in your browser and unpack it at the repo
  root with `tar -xzf ocel_dashboard_data_v1.tar.gz`. Then run
  `python3 run_dashboard.py --check`.
- **No GPU**: fine. It runs on the CPU, or on the Apple-silicon GPU (MPS)
  when available. On a CPU, the first single-trace explanation of a trace
  takes longer.
- **Linux**: `pip` installs PyTorch's default build, which bundles CUDA and
  is a much larger download (several GB). For a CPU-only build, create the
  environment first:

  ```
  python3.11 -m venv .venv
  .venv/bin/pip install torch==2.12.0 --index-url https://download.pytorch.org/whl/cpu
  ```

  Then run `python3 run_dashboard.py`. It keeps that torch.

## Repo map

- `run_dashboard.py`, `requirements-dashboard.txt`, `dashboard/`: the
  one-command dashboard (see [Run the dashboard](#run-the-dashboard)).
  `scripts/build_dashboard_bundle.py` is the maintainer-only script that
  rebuilds the data archive and `dashboard/data_manifest.json`.
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
