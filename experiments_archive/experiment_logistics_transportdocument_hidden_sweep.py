"""
Narrow hidden_channels resweep for logistics/TransportDocument->Depart.

Root-cause diagnostic (2026-07-25, see project memory project_capacity_vs_lastevent_mae_tradeoff.md)
found the deployed TransportDocument checkpoint uses hidden_channels=64 (the generic untuned
_DEFAULTS value), while CustomerOrder->Depart's dedicated 36-trial grid search found
hidden_channels=16 as its actual optimum at the same num_layers=3. TransportDocument has never
been given that same tuning treatment -- this script does a much narrower version: fix
num_layers=3 (already confirmed reachable/adequate), num_heads=2, lr=0.001 (TransportDocument's
currently-deployed lr) and weight_decay=1e-5, sweep ONLY hidden_channels in {8, 16, 32, 48}.

This intentionally does NOT resume the abandoned full 36-trial sweep_TimeFrom_TransportDocument_
to_Depart.db grid (6 hidden x 2 lr x 3 layers, ~30h total, only 4/36 complete) -- it's a fresh,
much smaller, targeted search using the sweep-style short training budget (30 epochs, patience 4,
L1Loss) for comparison, then a full production-style retrain (100 epochs, patience 10, matching
Het_Reg_Modelling / Modelling.__init__'s logistics branch) of the winning hidden_channels only.

Uses the backed-up TransportDocument graph caches directly (files/hetero_structures/logistics/
1000_transportdocument_backup/) rather than touching config.yml or the live logistics/1000/
graph cache (which currently holds CustomerOrder data) -- avoids the "manual dance" of swapping
production graph caches in and out.

Does NOT overwrite the production TimeFrom_TransportDocument_to_Depart.pth checkpoint or the
shared model_params.json -- saves a separate _hiddensweep_experiment.pth / _norm.json / a local
params json, exactly like the project's existing _layers5_wd0_experiment.pth convention.

Usage: python3 experiment_logistics_transportdocument_hidden_sweep.py
"""
import warnings
warnings.filterwarnings("ignore")

import copy
import json
import random
import time

import numpy as np
import pandas as pd
import torch
from torch_geometric.loader import DataLoader

from model_classes.HGT import HGT

TRAIN_PATH = "files/hetero_structures/logistics/1000_transportdocument_backup/train_graphs_sg.pt"
VAL_PATH   = "files/hetero_structures/logistics/1000_transportdocument_backup/val_graphs_sg.pt"
TEST_PATH  = "files/hetero_structures/logistics/1000_transportdocument_backup/test_graphs_sg.pt"

# Reuse the SAME saved normalization stats the deployed checkpoint used, for apples-to-apples
# comparability -- matches Modelling.__init__'s own behaviour of loading a pre-existing norm.json
# instead of recomputing, when one already exists.
DEPLOYED_NORM_PATH = "files/models/logistics/1000/Hetero/TimeFrom_TransportDocument_to_Depart_norm.json"

OUT_DIR = "files/models/logistics/1000/Hetero"
EXPERIMENT_TAG = "TimeFrom_TransportDocument_to_Depart_hiddensweep_experiment"
CKPT_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}.pth"
NORM_OUT_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}_norm.json"
PARAMS_OUT_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}_params.json"
SEARCH_LOG_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}_search_log.csv"

VP = "TransportDocument"
HIDDEN_CHOICES = [8, 16, 32, 48]
FIXED_NUM_LAYERS = 3
FIXED_NUM_HEADS = 2
FIXED_LR = 0.001
FIXED_WD = 1e-5
BATCH_SIZE = 16

# search-phase (sweep-style) budget, matching training.py sweep()'s objective()
SEARCH_MAX_EPOCHS = 30
SEARCH_PATIENCE = 4

# final-retrain budget, matching Modelling.__init__'s logistics branch (Het_Reg_Modelling)
FINAL_MAX_EPOCHS = 100
FINAL_PATIENCE = 10

CONTINUOUS_NODE_TYPES = ['CustomerOrder', 'Container', 'TransportDocument', 'Events']


def set_seeds():
    torch.manual_seed(42)
    random.seed(42)
    np.random.seed(42)


def load_and_normalize():
    print("Loading TransportDocument backup graphs (train/val/test)...")
    train_data = torch.load(TRAIN_PATH, weights_only=False)
    val_data = torch.load(VAL_PATH, weights_only=False)
    test_data = torch.load(TEST_PATH, weights_only=False)

    with open(DEPLOYED_NORM_PATH) as f:
        norm = json.load(f)
    target_mean = torch.tensor(norm["target_mean"])
    target_std = torch.tensor(norm["target_std"])

    for split in [train_data, val_data, test_data]:
        for g in split:
            g[VP].y = (g[VP].y - target_mean) / target_std

    print("Normalizing node features (train stats applied to all splits)...")
    for node_type in CONTINUOUS_NODE_TYPES:
        x_train = [g[node_type].x for g in train_data if g[node_type].num_nodes > 0]
        if not x_train:
            continue
        x_cat = torch.cat(x_train, dim=0)
        feat_mean = x_cat.mean(dim=0)
        feat_std = x_cat.std(dim=0).clamp(min=1e-8)
        for split in [train_data, val_data, test_data]:
            for g in split:
                if g[node_type].num_nodes > 0:
                    g[node_type].x = (g[node_type].x - feat_mean) / feat_std

    return train_data, val_data, test_data, target_mean, target_std


def build_model(hidden_channels, sample_graph):
    return HGT(hidden_channels=hidden_channels, out_channels=1,
               num_layers=FIXED_NUM_LAYERS, num_heads=FIXED_NUM_HEADS,
               data=sample_graph, viewpoint=VP)


def het_train(model, loader, optimizer, criterion, device):
    model.train()
    total_loss, total_examples = 0.0, 0
    for batch in loader:
        batch = batch.to(device)
        optimizer.zero_grad()
        out = model(batch.x_dict, batch.edge_index_dict)
        mask = batch[VP].mask.view(-1)
        y = batch[VP].y.view(-1, out.shape[-1])
        loss = criterion(out[mask], y[mask])
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        bs = int(mask.sum())
        total_examples += bs
        total_loss += float(loss) * bs
    return total_loss / total_examples


@torch.no_grad()
def het_loss_test(loader, model, criterion, device):
    model.eval()
    total_loss, total_examples = 0.0, 0
    for batch in loader:
        batch = batch.to(device)
        out = model(batch.x_dict, batch.edge_index_dict)
        mask = batch[VP].mask.view(-1)
        y = batch[VP].y.view(-1, out.shape[-1])
        loss = criterion(out[mask], y[mask])
        bs = int(mask.sum())
        total_examples += bs
        total_loss += float(loss) * bs
    return total_loss / total_examples


def search_phase(train_data, val_data, device):
    print(f"\n{'='*70}\nPHASE 1: narrow hidden_channels search {HIDDEN_CHOICES} "
          f"(num_layers={FIXED_NUM_LAYERS}, lr={FIXED_LR}, max_epochs={SEARCH_MAX_EPOCHS}, "
          f"patience={SEARCH_PATIENCE})\n{'='*70}")
    criterion = torch.nn.L1Loss()
    train_loader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE, shuffle=False)

    results = []
    for hidden_channels in HIDDEN_CHOICES:
        set_seeds()
        print(f"\n--- hidden_channels={hidden_channels} ---")
        t0 = time.time()
        model = build_model(hidden_channels, train_data[0]).to(device)
        with torch.no_grad():
            batch = next(iter(train_loader)).to(device)
            model(batch.x_dict, batch.edge_index_dict)
        optimizer = torch.optim.Adam(model.parameters(), lr=FIXED_LR, weight_decay=FIXED_WD)

        best_val, patience_count = float('inf'), 0
        for epoch in range(1, SEARCH_MAX_EPOCHS + 1):
            train_loss = het_train(model, train_loader, optimizer, criterion, device)
            val_mae = het_loss_test(val_loader, model, criterion, device)
            print(f"  epoch {epoch:02d}  train_loss={train_loss:.4f}  val_mae={val_mae:.4f}")
            if val_mae < best_val:
                best_val, patience_count = val_mae, 0
            else:
                patience_count += 1
                if patience_count >= SEARCH_PATIENCE:
                    print(f"  early stop at epoch {epoch}")
                    break
        elapsed = time.time() - t0
        print(f"  hidden_channels={hidden_channels} -> best_val_mae={best_val:.4f} ({elapsed:.0f}s)")
        results.append({'hidden_channels': hidden_channels, 'best_val_mae': best_val,
                        'elapsed_s': elapsed})
        pd.DataFrame(results).to_csv(SEARCH_LOG_PATH, index=False)

    results_df = pd.DataFrame(results).sort_values('best_val_mae')
    print(f"\nSearch results (sorted by val MAE):\n{results_df.to_string(index=False)}")
    best_hidden = int(results_df.iloc[0]['hidden_channels'])
    print(f"\nWinner: hidden_channels={best_hidden}")
    return best_hidden, results_df


def final_retrain(best_hidden, train_data, val_data, test_data, target_mean, target_std, device):
    print(f"\n{'='*70}\nPHASE 2: full retrain of winning hidden_channels={best_hidden} "
          f"(max_epochs={FINAL_MAX_EPOCHS}, patience={FINAL_PATIENCE})\n{'='*70}")
    set_seeds()
    criterion = torch.nn.L1Loss()
    train_loader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=BATCH_SIZE, shuffle=False)

    model = build_model(best_hidden, train_data[0]).to(device)
    with torch.no_grad():
        batch = next(iter(train_loader)).to(device)
        model(batch.x_dict, batch.edge_index_dict)

    optimizer = torch.optim.Adam(model.parameters(), lr=FIXED_LR, weight_decay=FIXED_WD)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=10)

    best_val_mae, best_state = float("inf"), None
    epochs_without_improvement = 0
    log = []
    fit_start = time.time()

    for epoch in range(1, FINAL_MAX_EPOCHS + 1):
        train_loss = het_train(model, train_loader, optimizer, criterion, device)
        val_mae = het_loss_test(val_loader, model, criterion, device)
        scheduler.step(val_mae)
        current_lr = optimizer.param_groups[0]["lr"]
        weight_norms = {f'norm_{nt}': model.lin_dict[nt].weight.norm().item() for nt in model.lin_dict}
        log.append({'epoch': epoch, 'train_loss': train_loss, 'val_mae': val_mae,
                    'lr': current_lr, **weight_norms})
        print(f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}")

        if val_mae < best_val_mae:
            print("New Best!")
            best_val_mae = val_mae
            best_state = copy.deepcopy(model.state_dict())
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1
        if epochs_without_improvement >= FINAL_PATIENCE:
            print(f"\nEarly stopping at epoch {epoch} (no val improvement for {FINAL_PATIENCE} epochs)")
            break

    fit_time_s = time.time() - fit_start
    print(f"Fitting time: {fit_time_s:.4f}s")

    if best_state is not None:
        model.load_state_dict(best_state)
    test_loss = het_loss_test(test_loader, model, criterion, device)
    print(f"Final test MAE (normalized): {test_loss}")

    torch.save(model.state_dict(), CKPT_PATH)
    with open(NORM_OUT_PATH, "w") as f:
        json.dump({"target_mean": target_mean.item(), "target_std": target_std.item(),
                   "fit_time_s": fit_time_s}, f)
    with open(PARAMS_OUT_PATH, "w") as f:
        json.dump({"hidden_channels": best_hidden, "num_layers": FIXED_NUM_LAYERS,
                   "num_heads": FIXED_NUM_HEADS, "lr": FIXED_LR, "weight_decay": FIXED_WD}, f, indent=2)
    pd.DataFrame(log).to_csv(f"{OUT_DIR}/{EXPERIMENT_TAG}_training_log.csv", index=False)
    print(f"Saved experiment checkpoint to {CKPT_PATH}")

    return model


@torch.no_grad()
def evaluate_last_event(model, test_data, target_mean, target_std, device):
    print(f"\n{'='*70}\nEVALUATION: last-event MAE/R2 for the new hiddensweep checkpoint\n{'='*70}")
    model.eval()
    records = []
    for g in test_data:
        if g[VP].y.shape[0] == 0:
            continue
        g = g.to(device)
        out = model(g.x_dict, g.edge_index_dict)
        pred_h = (out[0].item() * target_std.item() + target_mean.item()) / 3600.0
        true_h = (g[VP].y[0].item() * target_std.item() + target_mean.item()) / 3600.0
        records.append({'order_id': int(g[VP].id[0].item()), 'last_event': bool(g[VP].last_event[0].item()),
                        'true_h': true_h, 'pred_h': pred_h})
    df = pd.DataFrame(records)
    last = df[df.last_event].copy()
    last['err'] = last.pred_h - last.true_h
    mae = last.err.abs().mean()
    rmse = np.sqrt((last.err ** 2).mean())
    ss_res = (last.err ** 2).sum()
    ss_tot = ((last.true_h - last.true_h.mean()) ** 2).sum()
    r2 = 1 - ss_res / ss_tot
    print(f"n last-event = {len(last)}")
    print(f"MAE_last = {mae:.3f}h  RMSE_last = {rmse:.3f}h  R2_last = {r2:.3f}")
    print("(reference, deployed hidden_channels=64 checkpoint: MAE_last=9.056h  R2_last=-4.022)")
    df.to_csv(f"{OUT_DIR}/{EXPERIMENT_TAG}_test_predictions.csv", index=False)
    return mae, rmse, r2


def main():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}")
    train_data, val_data, test_data, target_mean, target_std = load_and_normalize()

    best_hidden, results_df = search_phase(train_data, val_data, device)
    model = final_retrain(best_hidden, train_data, val_data, test_data, target_mean, target_std, device)
    evaluate_last_event(model, test_data, target_mean, target_std, device)


if __name__ == '__main__':
    main()
