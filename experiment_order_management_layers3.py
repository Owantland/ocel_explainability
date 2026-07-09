"""
Experiment: does num_layers=3 (one more hop, same fix that dramatically improved
TimeFrom_CustomerOrder_to_Depart on logistics) also improve TimeFrom_Orders_to_PayOrder
on order_management? Unlike the CustomerOrder case, this model is NOT broken -- its
current num_layers=2 checkpoint already achieves MAE_last=102.7h, R2_last=0.097 (the
checkpoint used throughout this session's presentation examples). This tests whether
more depth improves an already-working model further, not whether it fixes a collapse.

Same hidden_channels/num_heads/lr/weight_decay as the currently-saved params (64/2/0.001/1e-5)
-- isolates the num_layers effect specifically. Saves to a separate checkpoint path, does NOT
overwrite the official TimeFrom_Orders_to_PayOrder.pth (already backed up separately).
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

DATABASE = 'order_management'
CANT = 2000
CHECKPOINT_PATH = 'files/models/order_management/2000/Hetero/TimeFrom_Orders_to_PayOrder_layers3_experiment.pth'

m = t.Modelling(DATABASE, CANT)
params = dict(m.params)
print(f"Baseline params (currently active, num_layers=2): {params}")
params['num_layers'] = 3
print(f"Experiment params: {params}")

m.model = m._build_model(params).to(m.device)
m.params = params

torch.manual_seed(42)
random.seed(42)
np.random.seed(42)

vp = m.viewpoint_object
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
               "fit_time_s": fit_time_s, "num_layers": 3}, f)
pd.DataFrame(log).to_csv(CHECKPOINT_PATH.replace(".pth", "_training_log.csv"), index=False)
print(f"Saved checkpoint to {CHECKPOINT_PATH}")

# ── Validation, in hours ──────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("VALIDATION (hours)")
print("=" * 60)
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
    print(f"TEST METRICS -- {label} (n={len(subset)})")
    print(f"  MAE  : {mae_:.1f} h")
    print(f"  RMSE : {rmse_:.1f} h")
    print(f"  R2   : {r2_:.3f}")
    print(f"  Mean true remaining : {subset['true_h'].mean():.1f} h  (std={subset['true_h'].std():.1f} h)")


print_metrics(df, "ALL prefixes")
print_metrics(last, "LAST-EVENT prefixes only")
print("\nSample last-event predictions:")
print(last.sample(min(5, len(last)), random_state=1)[['true_h', 'pred_h', 'abs_err_h']].to_string(index=False))

print("\n=== lin_dict weight health ===")
for node_type, lin in model.lin_dict.items():
    w = lin.weight
    print(f"  {node_type:20s}: abs_mean={w.abs().mean().item():.6f}")
