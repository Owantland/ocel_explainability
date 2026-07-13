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
