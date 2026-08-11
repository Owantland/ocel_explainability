"""Read-only investigation for checked_thesis_comments.md comment #16/#18 (Fix 3):
does order_management/PayOrder's confirmed train>val>...test MAE gap (see Fix 1's
diagnose_train_val_gap.py) reflect a genuine distributional difference between the
temporal train/val/test splits, independent of any model? Loads no new data beyond
what Modelling() already loads; writes nothing.

Usage: python3 investigate_payorder_split_shift.py
"""
import torch
import training as t

DB, CANT = "order_management", 2000


def denorm(y_norm, mean, std):
    return (y_norm * std + mean) / 3600.0  # seconds -> hours


def target_stats(m, data, name, train_mean_h):
    ys = torch.cat([g[m.kpi_viewpoint].y for g in data]).squeeze()
    ys_h = denorm(ys, m.target_mean.cpu(), m.target_std.cpu())
    mean_pred_mae = (ys_h - train_mean_h).abs().mean().item()
    print(f"  {name:6s}: n={ys_h.numel():5d}  mean={ys_h.mean():.1f}h  std={ys_h.std():.1f}h  "
          f"min={ys_h.min():.1f}h  max={ys_h.max():.1f}h  "
          f"mean-predictor MAE (vs. train mean)={mean_pred_mae:.1f}h")
    return ys_h


def feature_stats(m, node_type):
    print(f"\n  -- {node_type} --")
    for name, data in [("train", m.train_data), ("val", m.val_data), ("test", m.test_data)]:
        xs = [g[node_type].x for g in data if g[node_type].num_nodes > 0]
        if not xs:
            print(f"    {name:6s}: no nodes of this type")
            continue
        x = torch.cat(xs, dim=0)
        print(f"    {name:6s}: n_nodes={x.shape[0]:6d}  per-dim mean={x.mean(dim=0).numpy().round(3)}  "
              f"per-dim std={x.std(dim=0).numpy().round(3)}")


def graph_counts(m):
    print("\nSplit composition (graph counts):")
    for name, data in [("train", m.train_data), ("val", m.val_data), ("test", m.test_data)]:
        print(f"  {name:6s}: {len(data)} graphs")


def main():
    m = t.Modelling(DB, CANT)

    train_mean_h = denorm(
        torch.cat([g[m.kpi_viewpoint].y for g in m.train_data]).squeeze(),
        m.target_mean.cpu(), m.target_std.cpu()
    ).mean()

    print(f"\n=== Target (remaining time to PayOrder) per split, denormalized to hours ===")
    target_stats(m, m.train_data, "train", train_mean_h)
    target_stats(m, m.val_data,   "val",   train_mean_h)
    target_stats(m, m.test_data,  "test",  train_mean_h)

    print(f"\n=== Per-node-type feature drift (z-normalized w/ train-fit stats; train ~ (0,1) by construction) ===")
    for node_type in ["Orders", "Items", "Packages", "Products", "Events"]:
        feature_stats(m, node_type)

    graph_counts(m)


if __name__ == "__main__":
    main()
