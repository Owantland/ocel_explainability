"""
Full validation + explainability pipeline for cant=1000, logistics.
Assumes OCEL (ocel.csv, edges.csv, ev_log.csv, all_kpis.csv), hetero graphs, and a
trained HGT checkpoint (TimeFrom_TransportDocument_to_Depart.pth) already exist.
Steps: load existing model → validate → model comparison → explainability.
"""
import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os

import training as t
import explainer as exp

DATABASE = 'logistics'
CANT = 1000

# ── 1. Load the existing trained HGT checkpoint (no retraining) ──────────────
print("Loading existing HGT checkpoint...")
m = t.Modelling(DATABASE, CANT)
print(f"  Loaded params: {m.params}")
m.model.load_state_dict(torch.load(m.model_path, weights_only=False))
m.model.eval()

# ── 2. Validation ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("VALIDATION")
print("=" * 60)

vp = m.viewpoint_object
records = []

with torch.no_grad():
    for g in m.test_data:
        pred_norm = m.model(g.x_dict, g.edge_index_dict)[0].item()
        pred_h    = (pred_norm * m.target_std.item() + m.target_mean.item()) / 3600.0
        true_h    = (g[vp].y[0].item() * m.target_std.item() + m.target_mean.item()) / 3600.0
        records.append({
            'order_id':   int(g[vp].id[0].item()),
            'n_events':   g['Events'].num_nodes,
            'true_h':     true_h,
            'pred_h':     pred_h,
            'abs_err_h':  abs(true_h - pred_h),
            'last_event': g[vp].last_event[0].item(),
        })

df   = pd.DataFrame(records)
last = df[df['last_event']]

def print_metrics(subset, label):
    mae_  = subset['abs_err_h'].mean()
    rmse_ = np.sqrt((subset['abs_err_h'] ** 2).mean())
    ss_res = ((subset['true_h'] - subset['pred_h']) ** 2).sum()
    ss_tot = ((subset['true_h'] - subset['true_h'].mean()) ** 2).sum()
    r2_   = 1 - ss_res / ss_tot
    print(f"\n{'='*60}")
    print(f"TEST METRICS — {label}  (n={len(subset)})")
    print(f"{'='*60}")
    print(f"  MAE  : {mae_:.1f} h")
    print(f"  RMSE : {rmse_:.1f} h")
    print(f"  R²   : {r2_:.3f}")
    print(f"  Mean true remaining : {subset['true_h'].mean():.1f} h  "
          f"(std={subset['true_h'].std():.1f} h -- spread of the TRUE remaining-time "
          f"values in this subset, not a model-error metric)")
    return mae_, rmse_, r2_

mae_all,  rmse_all,  r2_all  = print_metrics(df,   "ALL prefixes")
mae_last, rmse_last, r2_last = print_metrics(last, "LAST-EVENT prefixes only")

print("\nMAE by prefix depth:")
bins   = [0, 3, 6, 9, 999]
labels = ['1-3', '4-6', '7-9', '10+']
df['depth_bin'] = pd.cut(df['n_events'], bins=bins, labels=labels, right=True)
depth_stats = df.groupby('depth_bin', observed=True)['abs_err_h'].agg(['mean', 'count'])
depth_stats.columns = ['MAE (h)', 'n']
print(depth_stats.to_string())

print("\nTop-5 highest true remaining times vs predictions:")
print(last.nlargest(5, 'true_h')[['order_id', 'true_h', 'pred_h', 'abs_err_h']].to_string(index=False))

print("\nTop-5 largest errors:")
print(last.nlargest(5, 'abs_err_h')[['order_id', 'true_h', 'pred_h', 'abs_err_h']].to_string(index=False))

# Residual plot
out_dir = f"files/explainer_outputs/{DATABASE}/validation_{CANT}"
os.makedirs(out_dir, exist_ok=True)

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
axes[0].scatter(last['true_h'], last['pred_h'], alpha=0.5, s=20)
lim = max(last['true_h'].max(), last['pred_h'].max()) * 1.05
axes[0].plot([0, lim], [0, lim], 'r--', lw=1)
axes[0].set_xlabel('True remaining time (h)')
axes[0].set_ylabel('Predicted remaining time (h)')
axes[0].set_title(f'Predicted vs. True  (R²={r2_last:.3f})')
axes[1].hist(last['pred_h'] - last['true_h'], bins=30, edgecolor='k', linewidth=0.4)
axes[1].axvline(0, color='r', linestyle='--')
axes[1].set_xlabel('Residual (pred − true, h)')
axes[1].set_title(f'Residuals  (MAE={mae_last:.1f}h, RMSE={rmse_last:.1f}h)')
plt.tight_layout()
plt.savefig(f'{out_dir}/residuals.png', dpi=150)
plt.close()
print(f"\nResidual plot saved to {out_dir}/residuals.png")

# ── 3. Model comparison (HGT vs. HomoGNN vs. Mean vs. GBT) ───────────────────
print("\n" + "=" * 60)
print("MODEL COMPARISON — HGT vs. baselines")
print("=" * 60)
e = exp.Explainer(DATABASE, CANT)
homo_model_path = e.model_path.replace(".pth", "_homo.pth")
homo_params_path = homo_model_path.replace(".pth", "_meta.json")
homo_is_stale = os.path.exists(homo_model_path) and not os.path.exists(homo_params_path)
if not os.path.exists(homo_model_path) or homo_is_stale:
    print("Training HomoGNN baseline (missing or stale for current hyperparameters)...")
    e.Homo_Reg_Modelling()
comparison_df = e.compare_to_baselines()
print(comparison_df.to_string(index=False))

# ── 4. Feature attribution ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("FEATURE ATTRIBUTION (InputXGradient)")
print("=" * 60)
e.explain_feature_attribution()

# ── 5. Aggregate LOO ──────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("AGGREGATE LOO EXPLANATION (n=50 traces)")
print("=" * 60)
e.explain_aggregate(n_traces=50, top_k=5)

# ── 6. Per-trace explanations ─────────────────────────────────────────────────
last_sorted = last.sort_values('true_h')
fast_id = int(last_sorted.iloc[len(last_sorted) // 5]['order_id'])
slow_id = int(last_sorted.iloc[-len(last_sorted) // 5]['order_id'])

print("\n" + "=" * 60)
fast_true = last_sorted[last_sorted['order_id'] == fast_id]['true_h'].values[0]
print(f"TRACE EXPLANATION — fast TransportDocument (id={fast_id}, true={fast_true:.0f}h)")
print("=" * 60)
e.explain_trace(fast_id, top_k=5)

print("\n" + "=" * 60)
slow_true = last_sorted[last_sorted['order_id'] == slow_id]['true_h'].values[0]
print(f"TRACE EXPLANATION — slow TransportDocument (id={slow_id}, true={slow_true:.0f}h)")
print("=" * 60)
e.explain_trace(slow_id, top_k=5)
