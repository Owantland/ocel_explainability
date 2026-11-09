"""One-off diagnostic for checked_thesis_comments.md comment #18 (Fig. 5.4/5.5's
train-loss-above-val-loss pattern). Reloads an already-trained, already-saved HGT
checkpoint (no retraining) and evaluates het_loss_test() -- already eval()-mode,
no_grad -- on the TRAINING set as well as val/test, to check whether the "training
loss" reported during training (an average over intermediate, mid-epoch weight
states, per het_train()'s accumulation pattern) differs from the true post-training,
eval-mode training-set MAE. If eval-mode train MAE ~= val MAE, the apparent
train/val gap in the reported training curves is a measurement artifact, not real
generalization difficulty. Read-only: loads existing .pth files, writes nothing.

Usage: python3 diagnose_train_val_gap.py <database> <cant>
"""
import sys
import torch
import training as t


def denorm_hours(v, mean, std):
    return (v * std + mean) / 3600.0


def main(database, cant):
    m = t.Modelling(database, cant)
    print(f"\n=== {database} (cant={cant}) task_id={m.task_id} ===")
    m.model.load_state_dict(torch.load(m.model_path, weights_only=False))
    m.model.eval()

    criterion = torch.nn.L1Loss()
    train_loader = torch.utils.data.DataLoader(m.train_data, batch_size=16, shuffle=False,
                                                collate_fn=lambda b: b)
    from torch_geometric.loader import DataLoader as GeoDataLoader
    train_loader = GeoDataLoader(m.train_data, batch_size=16, shuffle=False)
    val_loader   = GeoDataLoader(m.val_data,   batch_size=16, shuffle=False)
    test_loader  = GeoDataLoader(m.test_data,  batch_size=16, shuffle=False)

    train_mae_norm = m.het_loss_test(train_loader, m.model, criterion, m.device)
    val_mae_norm   = m.het_loss_test(val_loader,   m.model, criterion, m.device)
    test_mae_norm  = m.het_loss_test(test_loader,  m.model, criterion, m.device)

    mean, std = m.target_mean.item(), m.target_std.item()
    print(f"Normalized MAE   -- train (eval mode): {train_mae_norm:.4f}  "
          f"val: {val_mae_norm:.4f}  test: {test_mae_norm:.4f}")
    print(f"Denormalized [h] -- train (eval mode): {denorm_hours(train_mae_norm, mean, std):.2f}  "
          f"val: {denorm_hours(val_mae_norm, mean, std):.2f}  "
          f"test: {denorm_hours(test_mae_norm, mean, std):.2f}")

    # Compare against the last logged (during-training) train_loss for context
    log_path = m.model_path.replace(".pth", "_training_log.csv")
    try:
        import pandas as pd
        log = pd.read_csv(log_path)
        last_row = log.iloc[-1]
        print(f"Last logged (during-training, mid-epoch-averaged) train_loss: "
              f"{last_row['train_loss']:.4f}  logged val_mae: {last_row.get('val_mae', float('nan')):.4f}")
    except Exception as e:
        print(f"(could not read training log for comparison: {e})")


if __name__ == "__main__":
    database = sys.argv[1] if len(sys.argv) > 1 else "order_management"
    cant = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    main(database, cant)
