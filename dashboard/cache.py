"""On-disk read-through cache for the Streamlit explainability dashboard.

explain_trace_shapley() takes seconds to minutes per trace, too slow to recompute on every
click. This module gives dashboard/app.py and dashboard/precompute.py a shared, persistent
(survives app restarts) cache keyed by (scope, mode, order_id[, n_events][, top_k]
[, checkpoint_fingerprint]), where scope is "<database>_<cant>_<task_id>" (see
dashboard/kpis.cache_scope) -- the task_id keeps two KPIs on the same database (e.g.
PayOrder and PackageDelivered) from sharing entries. Stores exactly what the explain_*
function returns, which is JSON-serializable as-is.
"""
import json
import os

CACHE_ROOT = "dashboard_cache"


def cache_path(scope, mode, order_id, n_events=None, top_k=5, checkpoint_fingerprint=None):
    ev_suffix = f"_ev{n_events}" if n_events is not None else ""
    k_suffix = f"_k{top_k}" if top_k != 5 else ""
    # Ties a cached result to the exact model checkpoint (mtime) that produced it, so a
    # retrain gets a new cache path instead of silently returning a stale prediction (a
    # stale cached predicted_hours once made per-feature shifts collapse to a constant).
    cp_suffix = f"_cp{checkpoint_fingerprint}" if checkpoint_fingerprint is not None else ""
    return os.path.join(CACHE_ROOT, scope, mode,
                        f"{order_id}{ev_suffix}{k_suffix}{cp_suffix}.json")


def load_cached(scope, mode, order_id, n_events=None, top_k=5, checkpoint_fingerprint=None):
    path = cache_path(scope, mode, order_id, n_events, top_k, checkpoint_fingerprint)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def save_cached(scope, mode, order_id, result, n_events=None, top_k=5, checkpoint_fingerprint=None):
    path = cache_path(scope, mode, order_id, n_events, top_k, checkpoint_fingerprint)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(result, f)


def get_or_compute(scope, mode, order_id, compute_fn, n_events=None, top_k=5,
                   checkpoint_fingerprint=None):
    """Return (result, was_cached): the cached result if present AND its PNG folder still
    exists, otherwise compute_fn()'s result, cached for next time."""
    cached = load_cached(scope, mode, order_id, n_events, top_k, checkpoint_fingerprint)
    if cached is not None and os.path.isdir(cached.get('save_dir', '')):
        return cached, True
    result = compute_fn()
    save_cached(scope, mode, order_id, result, n_events, top_k, checkpoint_fingerprint)
    return result, False
