# Active thesis scripts

One-off scripts that generate a figure/table currently cited in
`thesis_parts/latex_template/chapters/results.tex`, or that back a claim made in the
thesis prose without producing a file of their own (`diagnose_train_val_gap.py`,
`investigate_payorder_split_shift.py`), or that are still-relevant hyperparameter
retunes/utilities. Unlike `experiments_archive/`, everything here is still "live" --
rerunning a script here is expected to update a real thesis artifact.

All use flat top-level imports (`import explainer`, `import baselines`, etc.) that
resolve relative to the repository root, not to this directory, and some import each
other as modules (e.g. `retune_loadtovehicle_stage2_extra_seeds.py` imports
`retune_loadtovehicle_full_search.py`; `refresh_depart_fidelity.py` and
`refresh_logistics_thesis_figures.py` both import `retune_depart_full_search.py`).
Run any of them from the repo root with the root on `PYTHONPATH` (same convention as
`experiments_archive/`):

```
PYTHONPATH=. python3 thesis_scripts/<script>.py
```

Scripts that write figures/tables write to `thesis_parts/figures_tables/` (staging)
and, where noted in their own docstring, also promote the result directly to
`thesis_parts/latex_template/figures/` under the exact filename `results.tex` cites --
follow that pattern (see `promote_aggregate_shapley_global_figures.py`) for any new
script, rather than relying on a manual copy/rename step, which is what caused 3
figures to silently drift out of sync before this directory existed.
