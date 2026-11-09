"""
Experiment: does num_layers=5, weight_decay=0 (the recipe that improved
TimeFrom_CustomerOrder_to_Depart) also improve TimeFrom_TransportDocument_to_Depart?

NOTE on provenance: model_params.json's saved entry for this task_id already records
{hidden_channels: 64, num_layers: 5, num_heads: 2, lr: 0.01, weight_decay: 0.0} -- but the
actual on-disk TimeFrom_TransportDocument_to_Depart.pth was NOT trained with those params.
Inspecting its state_dict directly shows 3 conv layers (not 5), and its training log shows
lr=0.001 (not 0.01) -- so model_params.json had drifted out of sync with the real checkpoint
before this script ran. This script (a) evaluates the REAL current checkpoint using its
actual architecture (hidden=64, layers=3, heads=2) as the honest baseline, then (b) trains
a fresh model using the layers=5/wd=0 params already sitting in model_params.json, and
compares the two on the real test set in hours.

Requires files/hetero_structures/logistics/1000/*.pt to be the TransportDocument-viewpoint
graphs (restored from the 1000_transportdocument_backup/ snapshot) and config.yml's logistics
kpi_viewpoint temporarily set to "TransportDocument" -- both must be reverted afterward since
the active pipeline state is CustomerOrder.

Saves to a separate checkpoint path, does NOT overwrite the official
TimeFrom_TransportDocument_to_Depart.pth.
"""
import copy
import json
import time
import random
import torch
import numpy as np
import pandas as pd
from torch_geometric.loader import DataLoader

import training as t

DATABASE = 'logistics'
CANT = 1000
CHECKPOINT_PATH = 'files/models/logistics/1000/Hetero/TimeFrom_TransportDocument_to_Depart_layers5_wd0_experiment.pth'
BASELINE_ARCH = {'hidden_channels': 64, 'num_layers': 3, 'num_heads': 2}

m = t.Modelling(DATABASE, CANT)
vp = m.kpi_viewpoint
print(f"Task ID: {m.task_id}")
print(f"Params loaded from model_params.json (used for the NEW experiment): {m.params}")
print(f"Actual architecture of the on-disk checkpoint (from state_dict inspection): {BASELINE_ARCH}")


def build_and_evaluate(params, state_dict_path, label):
    model = m._build_model(params).to(m.device)
    with torch.no_grad():
        g0 = m.train_data[0].to(m.device)
        model(g0.x_dict, g0.edge_index_dict)
    sd = torch.load(state_dict_path, map_location=m.device, weights_only=False)
    model.load_state_dict(sd)
    model.eval()

    records = []
    with torch.no_grad():
        for g in m.test_data:
            g = g.to(m.device)
            pred_norm = model(g.x_dict, g.edge_index_dict)[0].item()
            pred_h = (pred_norm * m.target_std.item() + m.target_mean.item()) / 3600.0
            true_h = (g[vp].y[0].item() * m.target_std.item() + m.target_mean.item()) / 3600.0
            records.append({'true_h': true_h, 'pred_h': pred_h, 'abs_err_h': abs(true_h - pred_h),
                             'last_event': g[vp].last_event[0].item()})
    df = pd.DataFrame(records)

    def metrics(subset, sublabel):
        mae_ = subset['abs_err_h'].mean()
        rmse_ = np.sqrt((subset['abs_err_h'] ** 2).mean())
        ss_res = ((subset['true_h'] - subset['pred_h']) ** 2).sum()
        ss_tot = ((subset['true_h'] - subset['true_h'].mean()) ** 2).sum()
        r2_ = 1 - ss_res / ss_tot
        print(f"  [{label}] {sublabel} (n={len(subset)}): MAE={mae_:.1f}h RMSE={rmse_:.1f}h R2={r2_:.3f}")

    metrics(df, "ALL prefixes")
    metrics(df[df['last_event']], "LAST-EVENT only")
    return model


print("\n=== BASELINE: actual current checkpoint (layers=3, heads=2, hidden=64) ===")
build_and_evaluate(BASELINE_ARCH, m.model_path, "current-official")

# ── Train the layers=5 / weight_decay=0 experiment ────────────────────────────
print("\n=== TRAINING: num_layers=5, weight_decay=0.0 ===")
params = dict(m.params)  # already {hidden:64, layers:5, heads:2, lr:0.01, wd:0.0}

m.model = m._build_model(params).to(m.device)
m.params = params

torch.manual_seed(42)
random.seed(42)
np.random.seed(42)

batch_size = m.path_dict.get('batch_size', 16)
train_loader = DataLoader(m.train_data, batch_size=batch_size, shuffle=True)
val_loader   = DataLoader(m.val_data,   batch_size=batch_size, shuffle=False)
test_loader  = DataLoader(m.test_data,  batch_size=batch_size, shuffle=False)

model = m.model
criterion = torch.nn.L1Loss()

with torch.no_grad():
    batch = next(iter(train_loader)).to(m.device)
    model(batch.x_dict, batch.edge_index_dict)

optimizer = torch.optim.Adam(model.parameters(), lr=params['lr'], weight_decay=params['weight_decay'])
scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=10)

max_epochs, early_stop_patience = m.max_epochs, m.early_stop_patience
best_val_mae, best_state = float("inf"), None
epochs_without_improvement = 0
log = []
fit_start = time.time()

for epoch in range(1, max_epochs + 1):
    train_loss = m.het_train(model, train_loader, optimizer, criterion, m.device)
    val_mae = m.het_loss_test(val_loader, model, criterion, m.device)
    scheduler.step(val_mae)
    current_lr = optimizer.param_groups[0]["lr"]
    log.append({'epoch': epoch, 'train_loss': train_loss, 'val_mae': val_mae, 'lr': current_lr})
    print(f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}")

    if val_mae < best_val_mae:
        print("New Best!")
        best_val_mae = val_mae
        best_state = copy.deepcopy(model.state_dict())
        epochs_without_improvement = 0
    else:
        epochs_without_improvement += 1

    if epochs_without_improvement >= early_stop_patience:
        print(f"\nEarly stopping at epoch {epoch} (no val improvement for {early_stop_patience} epochs)")
        break

fit_time_s = time.time() - fit_start
print(f"Fitting time: {fit_time_s:.4f}s")

if best_state is not None:
    model.load_state_dict(best_state)
test_loss = m.het_loss_test(test_loader, model, criterion, m.device)
print(f"Final test MAE (normalized): {test_loss}")

torch.save(model.state_dict(), CHECKPOINT_PATH)
with open(CHECKPOINT_PATH.replace(".pth", "_norm.json"), "w") as f:
    json.dump({"target_mean": m.target_mean.item(), "target_std": m.target_std.item(),
               "fit_time_s": fit_time_s, "num_layers": 5, "weight_decay": 0.0}, f)
pd.DataFrame(log).to_csv(CHECKPOINT_PATH.replace(".pth", "_training_log.csv"), index=False)
print(f"Saved checkpoint to {CHECKPOINT_PATH}")

print("\n=== EXPERIMENT: num_layers=5, weight_decay=0.0 ===")
model.eval()
records = []
with torch.no_grad():
    for g in m.test_data:
        pred_norm = model(g.x_dict, g.edge_index_dict)[0].item()
        pred_h = (pred_norm * m.target_std.item() + m.target_mean.item()) / 3600.0
        true_h = (g[vp].y[0].item() * m.target_std.item() + m.target_mean.item()) / 3600.0
        records.append({'true_h': true_h, 'pred_h': pred_h, 'abs_err_h': abs(true_h - pred_h),
                         'last_event': g[vp].last_event[0].item()})
df = pd.DataFrame(records)
last = df[df['last_event']]


def print_metrics(subset, label):
    mae_ = subset['abs_err_h'].mean()
    rmse_ = np.sqrt((subset['abs_err_h'] ** 2).mean())
    ss_res = ((subset['true_h'] - subset['pred_h']) ** 2).sum()
    ss_tot = ((subset['true_h'] - subset['true_h'].mean()) ** 2).sum()
    r2_ = 1 - ss_res / ss_tot
    print(f"  [layers5-wd0] {label} (n={len(subset)}): MAE={mae_:.1f}h RMSE={rmse_:.1f}h R2={r2_:.3f}")


print_metrics(df, "ALL prefixes")
print_metrics(last, "LAST-EVENT only")

print("\n=== lin_dict weight health ===")
for node_type, lin in model.lin_dict.items():
    w = lin.weight
    print(f"  {node_type:20s}: abs_mean={w.abs().mean().item():.6f}")
