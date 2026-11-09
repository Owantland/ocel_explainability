"""
Full pipeline for cant=1000, logistics, kpi_viewpoint=CustomerOrder (new target).
Unlike pipeline_1000_logistics.py (which assumes a trained TimeFrom_TransportDocument_to_Depart
checkpoint already exists), this script builds TimeFrom_CustomerOrder_to_Depart from scratch:
regenerate ev_log/all_kpis for the new kpi_viewpoint -> rebuild train/test split -> rebuild
hetero graphs -> sweep -> train -> train HomoGNN baseline -> compare to baselines -> explain.

The files/graph_structures/logistics/1000/ and files/hetero_structures/logistics/1000/ caches
this regenerates are NOT viewpoint-qualified in their filenames, so this overwrites the data
the existing TimeFrom_TransportDocument_to_Depart checkpoint depends on for re-validation --
a backup of the pre-existing files was taken before running this (see
files/graph_structures/logistics/1000_transportdocument_backup/ and the hetero_structures
equivalent) so that model's cache can be restored later if needed.
"""
import torch
import numpy as np
import pandas as pd
import os

import process_generation as pg
import train_test_builder as tb
import hetero_graphs as hg
import training as t
import explainer as exp

DATABASE = 'logistics'
CANT = 1000

# ── 1. Regenerate ev_log/all_kpis for the new kpi_viewpoint (CustomerOrder) ──────
print("=" * 60)
print("REGENERATING EVENT LOG FOR kpi_viewpoint=CustomerOrder")
print("=" * 60)
p = pg.ProcessGeneration(DATABASE, CANT)
nodes = p.related_nodes()
p.get_ev_log(nodes)

# ── 2. Rebuild train/test split ──────────────────────────────────────────────
print("\nBuilding train/test split...")
ttb = tb.TrainTestBuilder(DATABASE, CANT)
train_ts, val_ts, test_ts = ttb.timestamps_generator()

# ── 3. Rebuild hetero graphs for the new target ──────────────────────────────
print("\nBuilding hetero graphs...")
hgg = hg.HeteroGraphsGenerator(DATABASE, CANT, train_ts, val_ts, test_ts)
hgg.trace_kpi()

# ── 4. Sweep hyperparameters (no saved entry exists for this new task_id) ───
print("\n" + "=" * 60)
print("HYPERPARAMETER SWEEP")
print("=" * 60)
m = t.Modelling(DATABASE, CANT)
print(f"Task ID: {m.task_id}")
m.sweep()

# ── 5. Train with swept hyperparameters ──────────────────────────────────────
print("\n" + "=" * 60)
print("TRAINING")
print("=" * 60)
m = t.Modelling(DATABASE, CANT)  # reload params.json picked up by sweep()
print(f"  Loaded params: {m.params}")
m.Modelling()

# ── 6. Train HomoGNN baseline ─────────────────────────────────────────────────
print("\n" + "=" * 60)
print("HOMOGNN BASELINE")
print("=" * 60)
m.Homo_Reg_Modelling()

# ── 7. Validation ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("VALIDATION")
print("=" * 60)

m2 = t.Modelling(DATABASE, CANT)
m2.model.load_state_dict(torch.load(m2.model_path, weights_only=False))
m2.model.eval()

vp = m2.kpi_viewpoint
records = []
with torch.no_grad():
    for g in m2.test_data:
        pred_norm = m2.model(g.x_dict, g.edge_index_dict)[0].item()
        pred_h    = (pred_norm * m2.target_std.item() + m2.target_mean.item()) / 3600.0
        true_h    = (g[vp].y[0].item() * m2.target_std.item() + m2.target_mean.item()) / 3600.0
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

# ── 8. Model comparison (HGT vs. HomoGNN vs. Mean vs. GBT) ───────────────────
print("\n" + "=" * 60)
print("MODEL COMPARISON — HGT vs. baselines")
print("=" * 60)
e = exp.Explainer(DATABASE, CANT)
comparison_df = e.compare_to_baselines()
print(comparison_df.to_string(index=False))

# ── 9. Feature attribution ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("FEATURE ATTRIBUTION (InputXGradient)")
print("=" * 60)
e.explain_feature_attribution()

# ── 10. Aggregate LOO ─────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("AGGREGATE LOO EXPLANATION (n=50 traces)")
print("=" * 60)
e.explain_aggregate(n_traces=50, top_k=5)

# ── 11. Per-trace explanations ────────────────────────────────────────────────
last_sorted = last.sort_values('true_h')
fast_id = int(last_sorted.iloc[len(last_sorted) // 5]['order_id'])
slow_id = int(last_sorted.iloc[-len(last_sorted) // 5]['order_id'])

print("\n" + "=" * 60)
fast_true = last_sorted[last_sorted['order_id'] == fast_id]['true_h'].values[0]
print(f"TRACE EXPLANATION — fast CustomerOrder (id={fast_id}, true={fast_true:.0f}h)")
print("=" * 60)
e.explain_trace(fast_id, top_k=5)

print("\n" + "=" * 60)
slow_true = last_sorted[last_sorted['order_id'] == slow_id]['true_h'].values[0]
print(f"TRACE EXPLANATION — slow CustomerOrder (id={slow_id}, true={slow_true:.0f}h)")
print("=" * 60)
e.explain_trace(slow_id, top_k=5)
