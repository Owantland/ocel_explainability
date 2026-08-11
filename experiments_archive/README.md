# Archived experiment scripts

Concluded, one-off hyperparameter/verification scripts, moved here to declutter the repo root.
Each still describes its own hypothesis, method, and result in its header docstring — kept for
methodology/reproducibility reference, not for active re-running.

If a script ever needs to be re-run, invoke it from the repo root with the root on `PYTHONPATH`
(it isn't added automatically just by `cd`ing there, since Python puts the *script's own*
directory on `sys.path`, not the current working directory):

```
PYTHONPATH=. python3 experiments_archive/<script>.py
```

## Retired pipeline scripts (2026-07-13)

`pipeline_2000.py`, `pipeline_1000_logistics.py`, `pipeline_1000_logistics_customerorder.py`, and
`validate.py` are a different category from the hyperparameter-probe scripts above — not concluded
experiments, but the four near-identical "train → validate → compare → explain" orchestration
scripts consolidated into `run_pipeline.py` at the repo root (see its own docstring for the exact
`--stages` combination reproducing each one's behavior). Moved here after `run_pipeline.py`'s tail
logic was verified end-to-end against both datasets' real checkpoints, matching these originals'
previously-verified numbers exactly. Kept for reference/rollback, not for active re-running — use
`run_pipeline.py` going forward.

## Concluded diagnostics/retunes/showcases, code-audit batch (2026-08-11)

Moved here as part of a repo-wide code audit — each is a resolved diagnostic, a
superseded retune, or a showcase script whose output is not (or is no longer) cited
anywhere in the built thesis. Findings from the diagnostics are already baked into
`model_params.json`/thesis prose; nothing here is a live dependency of any current
figure or table.

- `diagnose_depart_collapse_mechanism.py`, `diagnose_depart_collapse_mechanism_adamw.py`,
  `diagnose_depart_collapse_phaseb_lowlr.py` — the 3-stage investigation chain that
  root-caused the Adam+weight_decay Depart collapse; conclusion (switch to AdamW)
  cited in `models.tex`, superseded in practice by `retune_depart_full_search.py`
  (kept active at the repo root).
- `retune_depart_lr_search.py` — an Adam-only (no AdamW) hyperparameter search,
  explicitly superseded by `retune_depart_full_search.py`'s later, AdamW-inclusive
  search, which is what the production checkpoint actually matches.
- `experiment_logistics_depart_seed_robustness.py` — characterized init-seed
  sensitivity of the (since-fixed) Adam collapse; superseded once AdamW eliminated
  the mechanism being probed.
- `experiment_logistics_transportdocument_hidden_sweep.py`,
  `experiment_logistics_transportdocument_lastevent_selection.py` — investigate the
  `TransportDocument` KPI viewpoint, which predates the switch to `CustomerOrder`
  (`files/config.yml`'s `kpi_viewpoint`); zero mentions anywhere in the current
  thesis.
- `aggregate_explanation_bars.py` — GNNExplainer-driven aggregate bars (comparison-
  only method); superseded by `promote_aggregate_shapley_global_figures.py`, which
  renders the thesis's primary LOO+Shapley method instead and is the one actually
  cited in `results.tex`.
- `feature_attribution_aggregate_showcase.py`,
  `gradient_shapley_comparison_neighbor_showcase.py` — standalone/neighbor-node
  variants whose rendered output was never copied into
  `thesis_parts/latex_template/figures/` or cited; the cited counterparts
  (`feature_attribution_aggregate_shapley_comparison_showcase.py`,
  `gradient_shapley_comparison_showcase.py`) remain active at the repo root.
- `model_comparison_summary.py` — covered only 2 of 4 KPIs without the k-dim GNN
  baseline; superseded by `combined_baseline_comparison.py`'s
  `combined_baseline_comparison_table.png` (5 models × 4 KPIs), which is what
  `results.tex` actually cites.
