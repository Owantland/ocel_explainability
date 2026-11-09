# Debug / verification scripts

Standalone, manually-run scripts that cross-check pipeline outputs against the raw
data (or against a reference implementation) rather than being part of the reusable
pipeline. Neither script is imported anywhere else in the repo.

- `db_testing.py` -- verifies `process_generation.py`/`ocel_generator.py`/
  `train_test_builder.py`/`hetero_graphs.py` output against the raw OCEL SQLite
  database directly, plus a comparison against HOEG's reference implementation.
- `sanity_check.py` -- a 6-group PASS/FAIL sanity checker (artefact existence,
  gradient flow, split leakage, etc.) for a trained model checkpoint. Runs its checks
  at module-execution time (no `if __name__=='__main__'` guard), hardcoded to
  `order_management`/2000 at file scope.

Both use flat top-level imports (`import explainer`, `import training`, etc.) that
resolve relative to the repository root, not to this directory. Run them from the
repo root with the root on `PYTHONPATH` (same convention as `experiments_archive/`):

```
PYTHONPATH=. python3 debug_scripts/db_testing.py
PYTHONPATH=. python3 debug_scripts/sanity_check.py
```
