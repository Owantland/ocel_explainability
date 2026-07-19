"""On-disk read-through cache for the Streamlit explainability dashboard.

explain_trace()/explain_gnn_primary() are cheap-to-moderate for LOO but
explain_gnn_primary()'s GNNExplainer optimization (~200 epochs by default) is far
too slow for on-demand interactivity. This module gives dashboard.py and
dashboard_precompute.py a shared, persistent (survives app restarts) cache keyed
by (database, cant, mode, order_id[, n_events][, top_k][, checkpoint_fingerprint]),
storing exactly what the
underlying explain_* function already returns -- verified JSON-serializable as-is
(node/edge/feature importances are plain (str, int, float, bool) tuples, no numpy/
torch types leak through), so no custom encoder is needed.
"""
import json
import os

CACHE_ROOT = "dashboard_cache"


def cache_path(database, cant, mode, order_id, n_events=None, top_k=5, checkpoint_fingerprint=None):
    ev_suffix = f"_ev{n_events}" if n_events is not None else ""
    # Omitted at the default (5) deliberately -- keeps every pre-existing cache entry
    # (all implicitly top_k=5) valid and hit-able for the common case.
    k_suffix = f"_k{top_k}" if top_k != 5 else ""
    # Omitted when not given (back-compat for any caller that hasn't been updated), but
    # dashboard.py/dashboard_precompute.py always pass one -- ties a cached result to the
    # exact model checkpoint (mtime) that produced it, so a retrain naturally produces a
    # new, distinct cache path instead of silently returning a stale prediction against
    # the new checkpoint. Found via direct investigation: a stale cached predicted_hours
    # (from before a retrain) was the direct cause of a reported bug where per-feature LOO
    # shifts collapsed to a near-constant, dominated by the stale-vs-fresh baseline mismatch.
    cp_suffix = f"_cp{checkpoint_fingerprint}" if checkpoint_fingerprint is not None else ""
    return os.path.join(CACHE_ROOT, f"{database}_{cant}", mode,
                        f"{order_id}{ev_suffix}{k_suffix}{cp_suffix}.json")


def load_cached(database, cant, mode, order_id, n_events=None, top_k=5, checkpoint_fingerprint=None):
    path = cache_path(database, cant, mode, order_id, n_events, top_k, checkpoint_fingerprint)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def save_cached(database, cant, mode, order_id, result, n_events=None, top_k=5, checkpoint_fingerprint=None):
    path = cache_path(database, cant, mode, order_id, n_events, top_k, checkpoint_fingerprint)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(result, f)


def get_or_compute(database, cant, mode, order_id, compute_fn, n_events=None, top_k=5,
                   checkpoint_fingerprint=None):
    """Read-through cache: return the cached result if present, otherwise call
    compute_fn() (expected to return explain_trace()'s or explain_gnn_primary()'s
    result dict), cache it, and return it. Returns (result, was_cached)."""
    cached = load_cached(database, cant, mode, order_id, n_events, top_k, checkpoint_fingerprint)
    if cached is not None:
        return cached, True
    result = compute_fn()
    save_cached(database, cant, mode, order_id, result, n_events, top_k, checkpoint_fingerprint)
    return result, False
