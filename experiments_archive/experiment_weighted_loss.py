"""
Experiment: test the depth-imbalance hypothesis for TimeFrom_CustomerOrder_to_Depart.
Last-event prefixes are ~1.6% of all training examples (138/8651 in the test split) --
a plain MAE loss pooled across all prefix depths gives almost no gradient signal toward
correctly predicting the rare, small, near-zero remaining-time endpoint cases. This trains
the same architecture (weight_decay=0, per the earlier lin_dict finding) but with the
per-sample loss up-weighted for last_event=True examples, to see if this fixes the
catastrophic last-event miscalibration (previously: predicts ~320h regardless of true
values of 14-30h). Saves to a SEPARATE checkpoint path, not overwriting either existing
variant (weight_decay=1e-5 original, or the weight_decay=0 retrain).
"""
import copy
import json
import time
import torch
import numpy as np
import pandas as pd
from torch_geometric.loader import DataLoader

import training as t

DATABASE = 'logistics'
CANT = 1000
LAST_EVENT_WEIGHT = 20.0
CHECKPOINT_PATH = 'files/models/logistics/1000/Hetero/TimeFrom_CustomerOrder_to_Depart_weighted_experiment.pth'

m = t.Modelling(DATABASE, CANT)
m.params['weight_decay'] = 0.0
print(f"Params: {m.params}, last_event_weight={LAST_EVENT_WEIGHT}")

torch.manual_seed(42)
import random
random.seed(42)
np.random.seed(42)

vp = m.kpi_viewpoint
batch_size = m.path_dict.get('batch_size', 16)
train_loader = DataLoader(m.train_data, batch_size=batch_size, shuffle=True)
val_loader   = DataLoader(m.val_data,   batch_size=batch_size, shuffle=False)
test_loader  = DataLoader(m.test_data,  batch_size=batch_size, shuffle=False)

model = m.model.to(m.device)
criterion_none = torch.nn.L1Loss(reduction='none')
criterion_mean = torch.nn.L1Loss()

with torch.no_grad():
    batch = next(iter(train_loader)).to(m.device)
    model(batch.x_dict, batch.edge_index_dict)

optimizer = torch.optim.Adam(model.parameters(), lr=m.params['lr'], weight_decay=m.params['weight_decay'])
scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=10)


def train_epoch_weighted():
    model.train()
    total_loss, total_examples = 0.0, 0
    for batch in train_loader:
        batch = batch.to(m.device)
        optimizer.zero_grad()
        out = model(batch.x_dict, batch.edge_index_dict)
        mask = batch[vp].mask.view(-1)
        y = batch[vp].y.view(-1, out.shape[-1])
        last_event_flags = batch[vp].last_event.view(-1)[mask]

        per_sample = criterion_none(out[mask], y[mask]).squeeze(-1)
        weights = torch.where(last_event_flags, LAST_EVENT_WEIGHT, 1.0)
        loss = (per_sample * weights).sum() / weights.sum()

        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()

        bsz = int(mask.sum())
        total_examples += bsz
        total_loss += float(loss) * bsz
    return total_loss / total_examples


@torch.no_grad()
def eval_epoch(loader):
    model.eval()
    total_loss, total_examples = 0.0, 0
    for batch in loader:
        batch = batch.to(m.device)
        out = model(batch.x_dict, batch.edge_index_dict)
        mask = batch[vp].mask.view(-1)
        y = batch[vp].y.view(-1, out.shape[-1])
        loss = criterion_mean(out[mask], y[mask])  # unweighted, for fair comparison
        bsz = int(mask.sum())
        total_examples += bsz
        total_loss += float(loss) * bsz
    return total_loss / total_examples


max_epochs, early_stop_patience = m.max_epochs, m.early_stop_patience
best_val_mae, best_state = float("inf"), None
epochs_without_improvement = 0
log = []
fit_start = time.time()

for epoch in range(1, max_epochs + 1):
    train_loss = train_epoch_weighted()
    val_mae = eval_epoch(val_loader)  # unweighted val MAE, matches earlier runs' criterion
    scheduler.step(val_mae)
    current_lr = optimizer.param_groups[0]["lr"]
    log.append({'epoch': epoch, 'train_loss': train_loss, 'val_mae': val_mae, 'lr': current_lr})
    print(f"Epoch {epoch:03d} | Train Loss (weighted): {train_loss:.4f} | Val MAE (unweighted): {val_mae:.4f} | LR: {current_lr:.2e}")

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
test_loss = eval_epoch(test_loader)
print(f"Final unweighted test MAE (normalized): {test_loss}")

torch.save(model.state_dict(), CHECKPOINT_PATH)
with open(CHECKPOINT_PATH.replace(".pth", "_norm.json"), "w") as f:
    json.dump({"target_mean": m.target_mean.item(), "target_std": m.target_std.item(),
               "fit_time_s": fit_time_s, "last_event_weight": LAST_EVENT_WEIGHT}, f)
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
