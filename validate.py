"""
Model validation: test-set metrics, prediction analysis, and explainability outputs
for the enriched-feature order_management model.
"""
import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os

import training as t
import explainer as exp

DATABASE = 'order_management'
CANT = 1000

# ── 1. Load model and run predictions on every test graph ────────────────────
m = t.Modelling(DATABASE, CANT)
m.model.load_state_dict(torch.load(m.model_path, weights_only=False))
m.model.eval()

vp = m.viewpoint_object   # 'Orders'
records = []

with torch.no_grad():
    for g in m.test_data:
        pred_norm = m.model(g.x_dict, g.edge_index_dict)[0].item()
        pred_h    = pred_norm * m.target_std.item() + m.target_mean.item()
        pred_h   /= 3600.0

        true_norm = g[vp].y[0].item()
        true_h    = true_norm * m.target_std.item() + m.target_mean.item()
        true_h   /= 3600.0

        order_id   = int(g[vp].id[0].item())
        last_event = g[vp].last_event[0].item()
        n_events   = g['Events'].num_nodes

        records.append({
            'order_id':   order_id,
            'n_events':   n_events,
            'true_h':     true_h,
            'pred_h':     pred_h,
            'abs_err_h':  abs(true_h - pred_h),
            'last_event': last_event,
        })

df = pd.DataFrame(records)
last = df[df['last_event']]

# ── 2. Overall test metrics (all prefixes and last-event) ────────────────────
def metrics(subset, label):
    mae_  = subset['abs_err_h'].mean()
    rmse_ = np.sqrt((subset['abs_err_h'] ** 2).mean())
    ss_res = ((subset['true_h'] - subset['pred_h']) ** 2).sum()
    ss_tot = ((subset['true_h'] - subset['true_h'].mean()) ** 2).sum()
    r2_   = 1 - ss_res / ss_tot
    print(f"\n{'='*60}")
    print(f"TEST SET METRICS — {label}  (n={len(subset)})")
    print(f"{'='*60}")
    print(f"  MAE  : {mae_:.1f} h")
    print(f"  RMSE : {rmse_:.1f} h")
    print(f"  R²   : {r2_:.3f}")
    print(f"  Mean true remaining : {subset['true_h'].mean():.1f} h  "
          f"(std={subset['true_h'].std():.1f} h)")
    return mae_, rmse_, r2_

mae_all,  rmse_all,  r2_all  = metrics(df,   "ALL prefixes")
mae_last, rmse_last, r2_last = metrics(last, "LAST-EVENT prefixes only")

# ── 3. Accuracy by prefix depth (# events seen) ──────────────────────────────
print("\nMAE by prefix depth (all prefixes):")
bins   = [0, 3, 6, 9, 999]
labels = ['1-3', '4-6', '7-9', '10+']
df['depth_bin'] = pd.cut(df['n_events'], bins=bins, labels=labels, right=True)
depth_stats = df.groupby('depth_bin', observed=True)['abs_err_h'].agg(['mean', 'count'])
depth_stats.columns = ['MAE (h)', 'n']
print(depth_stats.to_string())

# ── 4. Worst and best predictions ────────────────────────────────────────────
print("\nTop-5 highest true remaining times vs. predictions:")
print(last.nlargest(5, 'true_h')[['order_id', 'true_h', 'pred_h', 'abs_err_h']].to_string(index=False))

print("\nTop-5 orders with largest absolute error:")
print(last.nlargest(5, 'abs_err_h')[['order_id', 'true_h', 'pred_h', 'abs_err_h']].to_string(index=False))

# ── 5. Save residual plot ─────────────────────────────────────────────────────
out_dir = f"files/explainer_outputs/{DATABASE}/validation"
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
axes[1].set_ylabel('Count')
axes[1].set_title(f'Residuals  (MAE={mae_last:.1f}h, RMSE={rmse_last:.1f}h)')

plt.tight_layout()
plt.savefig(f'{out_dir}/residuals.png', dpi=150)
plt.close()
print(f"\nResidual plot saved to {out_dir}/residuals.png")

# ── 6. Feature attribution (IG) ──────────────────────────────────────────────
print("\n" + "=" * 60)
print("FEATURE ATTRIBUTION (InputXGradient)")
print("=" * 60)
e = exp.Explainer(DATABASE, CANT)
e.explain_feature_attribution()

# ── 7. Global LOO aggregate ───────────────────────────────────────────────────
print("\n" + "=" * 60)
print("AGGREGATE LOO EXPLANATION (n=50 traces)")
print("=" * 60)
e.explain_aggregate(n_traces=50, top_k=5)

# ── 8. Trace explanations for one fast + one slow order ──────────────────────
last_sorted = last.sort_values('true_h')
fast_id = int(last_sorted.iloc[len(last_sorted) // 5]['order_id'])   # ~20th percentile
slow_id = int(last_sorted.iloc[-len(last_sorted) // 5]['order_id'])  # ~80th percentile

print("\n" + "=" * 60)
print(f"TRACE EXPLANATION — fast order (id={fast_id}, true={last_sorted[last_sorted['order_id']==fast_id]['true_h'].values[0]:.0f}h)")
print("=" * 60)
e.explain_trace(fast_id, top_k=5)

print("\n" + "=" * 60)
print(f"TRACE EXPLANATION — slow order (id={slow_id}, true={last_sorted[last_sorted['order_id']==slow_id]['true_h'].values[0]:.0f}h)")
print("=" * 60)
e.explain_trace(slow_id, top_k=5)
