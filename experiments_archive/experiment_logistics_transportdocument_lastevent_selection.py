"""
Retrain logistics/TransportDocument->Depart with the new selection_metric='last_event' option
(training.py, added 2026-07-25 for OPEN_ISSUES_FEASIBILITY.md item 11), holding the ARCHITECTURE
identical to the currently-deployed checkpoint (hidden_channels=64, num_layers=3, num_heads=2,
lr=0.001, weight_decay=1e-5 -- from model_params.json's TimeFrom_TransportDocument_to_Depart
entry) so this is a clean ablation of the selection criterion alone, isolated from the (already
tried and rejected, see project_capacity_vs_lastevent_mae_tradeoff memory's 2026-07-25 update)
hidden_channels=8 resweep.

Reimplements the Het_Reg_Modelling training loop locally (calling the same het_train /
het_loss_test_split methods training.py's Modelling class exposes) rather than calling
Het_Reg_Modelling() directly, so it can add two resilience features Het_Reg_Modelling doesn't
have (deliberately not added there, to keep that shared/production code path unchanged for every
other checkpoint):
  1. Saves a checkpoint to disk every time a new best last-event val MAE is found, not just at
     the very end -- the first attempt at this retrain ran for 3h47m/18 epochs before being
     killed (cause unconfirmed; system swap was at ~85% capacity at the time, consistent with
     memory pressure) and lost all progress since nothing had been persisted yet.
  2. Resumable: if a partial checkpoint + resume-state json already exist on disk, picks up from
     that epoch instead of restarting from scratch.

Does NOT overwrite the production TimeFrom_TransportDocument_to_Depart.pth or model_params.json --
saves to a separate _lastevent_experiment.pth, matching the project's existing experiment-file
convention (e.g. _layers5_wd0_experiment.pth, _hiddensweep_experiment.pth).

Usage: python3 experiment_logistics_transportdocument_lastevent_selection.py
"""
import warnings
warnings.filterwarnings("ignore")

import json
import os
import random
import time

import numpy as np
import pandas as pd
import torch
from torch_geometric.loader import DataLoader

import training as t
from model_classes.HGT import HGT

TRAIN_PATH = "files/hetero_structures/logistics/1000_transportdocument_backup/train_graphs_sg.pt"
VAL_PATH   = "files/hetero_structures/logistics/1000_transportdocument_backup/val_graphs_sg.pt"
TEST_PATH  = "files/hetero_structures/logistics/1000_transportdocument_backup/test_graphs_sg.pt"
DEPLOYED_NORM_PATH = "files/models/logistics/1000/Hetero/TimeFrom_TransportDocument_to_Depart_norm.json"

OUT_DIR = "files/models/logistics/1000/Hetero"
EXPERIMENT_TAG = "TimeFrom_TransportDocument_to_Depart_lastevent_experiment"
CKPT_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}.pth"          # best-so-far checkpoint (model weights only,
                                                        # improvement-only -- the deployable artifact)
LATEST_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}_latest.pt"  # full trainer state (model+optimizer+scheduler
                                                        # +epoch/patience bookkeeping), saved EVERY
                                                        # epoch -- this, not CKPT_PATH, is what resume
                                                        # actually continues from. A first version of
                                                        # this script only saved CKPT_PATH (improvement-
                                                        # only, no optimizer state) and resumed from
                                                        # that -- looked like it resumed correctly
                                                        # (epoch/patience counters were right) but was
                                                        # actually restarting the optimizer from scratch
                                                        # against stale epoch-1 weights every time,
                                                        # silently invalidating the run (caught when
                                                        # epoch 9's train loss jumped back up to ~epoch
                                                        # 2's level instead of continuing down from
                                                        # epoch 8's).
LOG_PATH = f"{OUT_DIR}/{EXPERIMENT_TAG}_training_log.csv"

VP = "TransportDocument"
# Matches the deployed checkpoint's own architecture exactly (model_params.json) -- only the
# selection_metric changes in this experiment.
PARAMS = {'hidden_channels': 64, 'num_layers': 3, 'num_heads': 2, 'lr': 0.001, 'weight_decay': 1e-5}
BATCH_SIZE = 16
MAX_EPOCHS = 100          # matches Modelling.__init__'s logistics branch
EARLY_STOP_PATIENCE = 10  # matches Modelling.__init__'s logistics branch
CONTINUOUS_NODE_TYPES = ['CustomerOrder', 'Container', 'TransportDocument', 'Events']


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


@torch.no_grad()
def evaluate_last_event(model, test_data, target_mean, target_std, device):
    print(f"\n{'='*70}\nEVALUATION: last-event MAE/R2\n{'='*70}")
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
    print("(reference, deployed hidden_channels=64/pooled-selection checkpoint: MAE_last=9.056h  R2_last=-4.022)")
    print("(reference, hidden_channels=8/pooled-selection experiment [rejected]: MAE_last=17.224h  R2_last=-12.706)")
    df.to_csv(f"{OUT_DIR}/{EXPERIMENT_TAG}_test_predictions.csv", index=False)
    return mae, rmse, r2


def train(train_data, val_data, test_data, target_mean, target_std, device):
    torch.manual_seed(42)
    random.seed(42)
    np.random.seed(42)

    train_loader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)
    val_loader   = DataLoader(val_data,   batch_size=BATCH_SIZE, shuffle=False)

    model = HGT(hidden_channels=PARAMS['hidden_channels'], out_channels=1,
                num_layers=PARAMS['num_layers'], num_heads=PARAMS['num_heads'],
                data=train_data[0], viewpoint=VP).to(device)
    with torch.no_grad():
        batch = next(iter(train_loader)).to(device)
        model(batch.x_dict, batch.edge_index_dict)

    optimizer = torch.optim.Adam(model.parameters(), lr=PARAMS['lr'], weight_decay=PARAMS['weight_decay'])
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=10)
    criterion = torch.nn.L1Loss()

    # bound methods off the real Modelling class -- same training/eval code the production
    # pipeline uses (including the last-event split added 2026-07-25), not a duplicate.
    class _Stub:
        pass
    stub = _Stub()
    stub.kpi_viewpoint = VP

    start_epoch = 1
    best_val_mae, epochs_without_improvement = float("inf"), 0
    log = []
    if os.path.exists(LATEST_PATH):
        resume = torch.load(LATEST_PATH, weights_only=False)
        model.load_state_dict(resume["model_state"])
        optimizer.load_state_dict(resume["optimizer_state"])
        scheduler.load_state_dict(resume["scheduler_state"])
        start_epoch = resume["epoch"] + 1
        best_val_mae = resume["best_val_mae"]
        epochs_without_improvement = resume["epochs_without_improvement"]
        if os.path.exists(LOG_PATH):
            log = pd.read_csv(LOG_PATH).to_dict("records")
        print(f"RESUMING from epoch {start_epoch} (best_val_mae so far: {best_val_mae:.4f}, "
              f"{epochs_without_improvement} epochs without improvement) -- full optimizer/"
              f"scheduler state restored, not just the best-checkpoint weights.")

    fit_start = time.time()
    for epoch in range(start_epoch, MAX_EPOCHS + 1):
        train_loss = t.Modelling.het_train(stub, model, train_loader, optimizer, criterion, device)
        val_mae, val_mae_last_event = t.Modelling.het_loss_test_split(stub, val_loader, model, criterion, device)
        scheduler.step(val_mae_last_event)

        current_lr = optimizer.param_groups[0]["lr"]
        log.append({'epoch': epoch, 'train_loss': train_loss, 'val_mae': val_mae,
                    'val_mae_last_event': val_mae_last_event, 'lr': current_lr})
        print(f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | Val MAE (pooled): {val_mae:.4f} | "
              f"Val MAE (last-event): {val_mae_last_event:.4f} | Selecting on: last_event | LR: {current_lr:.2e}")

        if val_mae_last_event < best_val_mae:
            print("New Best! (saving checkpoint)")
            best_val_mae = val_mae_last_event
            epochs_without_improvement = 0
            torch.save(model.state_dict(), CKPT_PATH)
        else:
            epochs_without_improvement += 1

        # Full trainer state, saved EVERY epoch regardless of improvement -- this is what a
        # resume actually loads (model + optimizer + scheduler + bookkeeping), so a resume
        # continues the real training trajectory, not just the epoch/patience counters.
        torch.save({
            "model_state": model.state_dict(),
            "optimizer_state": optimizer.state_dict(),
            "scheduler_state": scheduler.state_dict(),
            "epoch": epoch,
            "best_val_mae": best_val_mae,
            "epochs_without_improvement": epochs_without_improvement,
        }, LATEST_PATH)
        pd.DataFrame(log).to_csv(LOG_PATH, index=False)

        if epochs_without_improvement >= EARLY_STOP_PATIENCE:
            print(f"\nEarly stopping at epoch {epoch} (no last-event val improvement for "
                  f"{EARLY_STOP_PATIENCE} epochs)")
            break

    fit_time_s = time.time() - fit_start
    print(f"Fitting time this run: {fit_time_s:.4f}s")

    model.load_state_dict(torch.load(CKPT_PATH, weights_only=False))  # reload best-so-far
    with open(f"{OUT_DIR}/{EXPERIMENT_TAG}_norm.json", "w") as f:
        json.dump({"target_mean": target_mean.item(), "target_std": target_std.item(),
                   "fit_time_s": fit_time_s}, f)
    print(f"Best checkpoint (last-event val MAE={best_val_mae:.4f}) at {CKPT_PATH}")
    return model


def main():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}")
    train_data, val_data, test_data, target_mean, target_std = load_and_normalize()

    print(f"Training with params={PARAMS}, selection_metric='last_event' "
          f"(early stopping + checkpoint-per-improvement, resumable)")
    model = train(train_data, val_data, test_data, target_mean, target_std, device)

    evaluate_last_event(model, test_data, target_mean, target_std, device)


if __name__ == '__main__':
    main()
