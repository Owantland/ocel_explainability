"""On-disk read-through cache for the Streamlit explainability dashboard.

explain_trace()/explain_gnn_primary() are cheap-to-moderate for LOO but
explain_gnn_primary()'s GNNExplainer optimization (~200 epochs by default) is far
too slow for on-demand interactivity. This module gives dashboard.py and
dashboard_precompute.py a shared, persistent (survives app restarts) cache keyed
by (database, cant, mode, order_id[, n_events]), storing exactly what the
underlying explain_* function already returns -- verified JSON-serializable as-is
(node/edge/feature importances are plain (str, int, float, bool) tuples, no numpy/
torch types leak through), so no custom encoder is needed.
"""
import json
import os

CACHE_ROOT = "dashboard_cache"


def cache_path(database, cant, mode, order_id, n_events=None):
    suffix = f"_ev{n_events}" if n_events is not None else ""
    return os.path.join(CACHE_ROOT, f"{database}_{cant}", mode, f"{order_id}{suffix}.json")


def load_cached(database, cant, mode, order_id, n_events=None):
    path = cache_path(database, cant, mode, order_id, n_events)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def save_cached(database, cant, mode, order_id, result, n_events=None):
    path = cache_path(database, cant, mode, order_id, n_events)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(result, f)


def get_or_compute(database, cant, mode, order_id, compute_fn, n_events=None):
    """Read-through cache: return the cached result if present, otherwise call
    compute_fn() (expected to return explain_trace()'s or explain_gnn_primary()'s
    result dict), cache it, and return it. Returns (result, was_cached)."""
    cached = load_cached(database, cant, mode, order_id, n_events)
    if cached is not None:
        return cached, True
    result = compute_fn()
    save_cached(database, cant, mode, order_id, result, n_events)
    return result, False
