"""
Baseline comparison for the HGT remaining-time regression model.

Two baselines are evaluated against the trained HGT on the test split:
  1. Mean predictor   — always predicts the training-set mean remaining time
  2. Gradient Boosted Trees (GBT) — sklearn GradientBoostingRegressor on
     handcrafted tabular features extracted from the raw graph objects

Usage: python baselines.py
Output: printed metrics table + baseline_comparison.png
"""
import warnings
warnings.filterwarnings("ignore")

import os
import json
import time
import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

import training as t

# ── constants ─────────────────────────────────────────────────────────────
DATABASE = 'order_management'
CANT     = 2000
OUT_DIR  = f"files/explainer_outputs/{DATABASE}/validation_2000"
FEAT_COLS = ['n_events', 'elapsed_h', 'waiting_h',
             'order_f0', 'n_items', 'total_weight', 'n_products']

# temporal feature indices in Events.x (after 11 one-hot event-type dims)
_ELAPSED_IDX = 11
_WAITING_IDX = 12

# ── helpers ────────────────────────────────────────────────────────────────

def load_raw_split(path: str) -> pd.DataFrame:
    """Load a raw (un-normalised) .pt graph file and return a feature DataFrame."""
    graphs = torch.load(path, weights_only=False)
    rows = []
    for g in graphs:
        n_ev = g['Events'].num_nodes
        if n_ev > 0:
            elapsed_h = g['Events'].x[-1, _ELAPSED_IDX].item()
            waiting_h = g['Events'].x[-1, _WAITING_IDX].item()
        else:
            elapsed_h = waiting_h = 0.0
        o = g['Orders'].x[0]          # shape [4]
        rows.append({
            'n_events':     n_ev,
            'elapsed_h':    elapsed_h,
            'waiting_h':    waiting_h,
            'order_f0':     o[0].item(),
            'n_items':      o[1].item(),
            'total_weight': o[2].item(),
            'n_products':   o[3].item(),
            'y_h':          g['Orders'].y[0].item() / 3600.0,
            'order_id':     int(g['Orders'].id[0].item()),
            'last_event':   bool(g['Orders'].last_event[0].item()),
        })
    return pd.DataFrame(rows)


def metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """MAE, RMSE, R² — same formulas as validate.py."""
    ae   = np.abs(y_true - y_pred)
    mae  = ae.mean()
    rmse = np.sqrt((ae ** 2).mean())
    ss_res = ((y_true - y_pred) ** 2).sum()
    ss_tot = ((y_true - y_true.mean()) ** 2).sum()
    r2   = 1.0 - ss_res / ss_tot if ss_tot > 0 else float('nan')
    return {'mae': mae, 'rmse': rmse, 'r2': r2}


def depth_mae(y_true: np.ndarray, y_pred: np.ndarray,
              n_events: np.ndarray) -> dict:
    """MAE broken down by prefix-depth bins (1-3, 4-6, 7-9, 10+)."""
    bins   = [(1, 3), (4, 6), (7, 9), (10, 9999)]
    labels = ['1-3', '4-6', '7-9', '10+']
    result = {}
    for (lo, hi), lbl in zip(bins, labels):
        mask = (n_events >= lo) & (n_events <= hi)
        if mask.sum() > 0:
            result[lbl] = np.abs(y_true[mask] - y_pred[mask]).mean()
        else:
            result[lbl] = float('nan')
    return result


# ── baseline models ────────────────────────────────────────────────────────

class MeanPredictor:
    def fit(self, y_train: np.ndarray):
        self.mean_ = y_train.mean()

    def predict(self, n: int) -> np.ndarray:
        return np.full(n, self.mean_)


class GBTPredictor:
    def __init__(self):
        self.scaler = StandardScaler()
        self.model  = GradientBoostingRegressor(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, random_state=42, verbose=0
        )

    def fit(self, X_train: np.ndarray, y_train: np.ndarray):
        self.scaler.fit(X_train)
        self.model.fit(self.scaler.transform(X_train), y_train)

    def predict(self, X_test: np.ndarray) -> np.ndarray:
        return self.model.predict(self.scaler.transform(X_test))

    @property
    def feature_importances_(self):
        return self.model.feature_importances_


# ── HGT inference ──────────────────────────────────────────────────────────

def hgt_predictions(m) -> tuple[pd.DataFrame, float]:
    """Run the trained HGT on m.test_data; return denormalised predictions plus the
    wall-clock prediction time in seconds (model loading excluded, matching how
    fitting/prediction time are measured separately elsewhere)."""
    m.model.load_state_dict(torch.load(m.model_path, weights_only=False))
    m.model.eval()
    vp = m.viewpoint_object
    records = []
    pred_time_s = 0.0
    with torch.no_grad():
        for g in m.test_data:
            _t0 = time.time()
            out = m.model(g.x_dict, g.edge_index_dict)
            pred_h = (out[0].item() * m.target_std.item() + m.target_mean.item()) / 3600.0
            pred_time_s += time.time() - _t0
            true_h = (g[vp].y[0].item() * m.target_std.item() + m.target_mean.item()) / 3600.0
            records.append({
                'order_id':   int(g[vp].id[0].item()),
                'last_event': bool(g[vp].last_event[0].item()),
                'n_events':   g['Events'].num_nodes,
                'true_h':     true_h,
                'hgt_pred_h': pred_h,
            })
    return pd.DataFrame(records), pred_time_s


def read_hgt_fit_time(m) -> float | None:
    """Read back the fitting time recorded by training.Modelling.Het_Reg_Modelling's
    _norm.json sidecar — baselines.py never retrains HGT, so its fit time has to come
    from the actual training run instead of being measured here."""
    norm_path = m.model_path.replace(".pth", "_norm.json")
    if os.path.exists(norm_path):
        with open(norm_path) as f:
            return json.load(f).get("fit_time_s")
    return None


# ── printing helpers ───────────────────────────────────────────────────────

def _fmt(m: dict) -> str:
    return f"{m['mae']:7.1f}  {m['rmse']:7.1f}  {m['r2']:6.3f}"


def print_table(results: dict):
    header = (f"{'Model':<18}  {'ALL PREFIXES':^30}  {'LAST-EVENT ONLY':^30}\n"
              f"{'':18}  {'MAE(h)':>7}  {'RMSE(h)':>7}  {'R²':>6}  "
              f"{'MAE(h)':>7}  {'RMSE(h)':>7}  {'R²':>6}")
    sep = '─' * 76
    print()
    print(header)
    print(sep)
    for name, (m_all, m_last) in results.items():
        print(f"{name:<18}  {_fmt(m_all)}  {_fmt(m_last)}")
    print()


# ── main ───────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # ── 1. Load data ──────────────────────────────────────────────────────
    import sup_funcs as sf
    path_dict = sf.SupportFunctions(DATABASE, CANT).get_paths()
    pt_path   = path_dict['pytorch_path']

    print("Loading raw graph splits...")
    train_df = load_raw_split(f"{pt_path}/train_graphs_sg.pt")
    test_df  = load_raw_split(f"{pt_path}/test_graphs_sg.pt")

    X_train = train_df[FEAT_COLS].values
    y_train = train_df['y_h'].values
    X_test  = test_df[FEAT_COLS].values
    y_test  = test_df['y_h'].values
    last_mask = test_df['last_event'].values

    # ── 2. Fit baselines (timed, HOEG Table 7-style) ────────────────────────
    print("Fitting Mean predictor...")
    mean_pred = MeanPredictor()
    _t0 = time.time()
    mean_pred.fit(y_train)
    mean_fit_time_s = time.time() - _t0
    _t0 = time.time()
    mean_preds = mean_pred.predict(len(test_df))
    mean_pred_time_s = time.time() - _t0

    print("Fitting GBT (n_estimators=300)...")
    gbt = GBTPredictor()
    _t0 = time.time()
    gbt.fit(X_train, y_train)
    gbt_fit_time_s = time.time() - _t0
    _t0 = time.time()
    gbt_preds = gbt.predict(X_test)
    gbt_pred_time_s = time.time() - _t0

    # ── 3. HGT predictions ────────────────────────────────────────────────
    print("Loading HGT model and running inference...")
    m = t.Modelling(DATABASE, CANT)
    hgt_fit_time_s = read_hgt_fit_time(m)  # from training.py's recorded fit time — never retrained here
    hgt_df, hgt_pred_time_s = hgt_predictions(m)
    hgt_preds = hgt_df['hgt_pred_h'].values

    # sanity: test split order matches (same number of graphs)
    assert len(hgt_df) == len(test_df), (
        f"HGT ({len(hgt_df)}) and raw test ({len(test_df)}) have different lengths"
    )

    # ── Scalability table (fitting/prediction time, seconds) ────────────────
    def _fmt_time(v):
        return f"{v:.4f}" if v is not None else "n/a"

    print(f"\n{'Model':<18}  {'Fitting Time (s)':>18}  {'Prediction Time (s)':>20}")
    print(f"{'Mean predictor':<18}  {_fmt_time(mean_fit_time_s):>18}  {mean_pred_time_s:>20.4f}")
    print(f"{'GBT':<18}  {_fmt_time(gbt_fit_time_s):>18}  {gbt_pred_time_s:>20.4f}")
    print(f"{'HGT (ours)':<18}  {_fmt_time(hgt_fit_time_s):>18}  {hgt_pred_time_s:>20.4f}")

    # ── 4. Compute metrics ────────────────────────────────────────────────
    results = {}
    for name, preds in [('Mean predictor', mean_preds),
                         ('GBT',            gbt_preds),
                         ('HGT (ours)',      hgt_preds)]:
        m_all  = metrics(y_test,             preds)
        m_last = metrics(y_test[last_mask],  preds[last_mask])
        results[name] = (m_all, m_last)

    print_table(results)

    print("GBT feature importances:")
    for feat, imp in sorted(zip(FEAT_COLS, gbt.feature_importances_),
                             key=lambda x: -x[1]):
        print(f"  {feat:<14}: {imp:.3f}")

    # ── 5. MAE by depth ───────────────────────────────────────────────────
    n_ev = test_df['n_events'].values
    BINS = ['1-3', '4-6', '7-9', '10+']
    depth_results = {
        name: depth_mae(y_test, preds, n_ev)
        for name, preds in [('Mean', mean_preds), ('GBT', gbt_preds), ('HGT', hgt_preds)]
    }

    print("\nMAE by prefix depth (all prefixes):")
    print(f"  {'Bin':<6}  {'Mean':>7}  {'GBT':>7}  {'HGT':>7}")
    for b in BINS:
        row = "  ".join(f"{depth_results[m][b]:7.1f}" for m in ['Mean', 'GBT', 'HGT'])
        print(f"  {b:<6}  {row}")

    # ── 6. Visualization ──────────────────────────────────────────────────
    os.makedirs(OUT_DIR, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # — Left: Predicted vs True scatter (last-event only) —
    ax = axes[0]
    y_true_last = y_test[last_mask]
    lim = max(y_true_last.max(), gbt_preds[last_mask].max(), hgt_preds[last_mask].max()) * 1.08

    ax.scatter(y_true_last, gbt_preds[last_mask],
               alpha=0.55, s=22, color='steelblue',
               label=f"GBT  (MAE={results['GBT'][1]['mae']:.1f}h)")
    ax.scatter(y_true_last, hgt_preds[last_mask],
               alpha=0.55, s=22, color='tomato',
               label=f"HGT  (MAE={results['HGT (ours)'][1]['mae']:.1f}h)")
    ax.plot([0, lim], [0, lim], 'k--', lw=1, label='Perfect prediction')
    ax.set_xlim(0, lim); ax.set_ylim(0, lim)
    ax.set_xlabel('True remaining time (h)')
    ax.set_ylabel('Predicted remaining time (h)')
    ax.set_title('Predicted vs True — last-event prefixes')
    ax.legend(fontsize=9)

    # — Right: MAE by depth (grouped bar chart, all prefixes) —
    ax = axes[1]
    x     = np.arange(len(BINS))
    width = 0.25
    colors = {'Mean': 'silver', 'GBT': 'steelblue', 'HGT': 'tomato'}
    for i, (name, clr) in enumerate(colors.items()):
        vals = [depth_results[name][b] for b in BINS]
        bars = ax.bar(x + (i - 1) * width, vals, width, label=name, color=clr,
                      edgecolor='white', linewidth=0.5)
        for bar, v in zip(bars, vals):
            if not np.isnan(v):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                        f'{v:.0f}', ha='center', va='bottom', fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(BINS)
    ax.set_xlabel('Prefix depth (n events seen)')
    ax.set_ylabel('MAE (h)')
    ax.set_title('MAE by prefix depth — all prefixes')
    ax.legend(fontsize=9)

    plt.suptitle(f'Baseline Comparison — {DATABASE} (cant={CANT})', fontsize=12, y=1.01)
    plt.tight_layout()
    out_path = f"{OUT_DIR}/baseline_comparison.png"
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nPlot saved to {out_path}")
