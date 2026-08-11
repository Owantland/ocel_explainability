"""Phase A of diagnosing WHY logistics/Depart's HGT collapses on fresh retrains (not just
THAT it does -- see project_hgt_dead_features_finding memory's 2026-08-03 update for the
confirmed 7/7-collapse background this investigates).

Free finding from the already-saved training logs (no new compute needed for this part):
the 5 doomed node types (TransportDocument, Vehicle, HandlingUnit, Container, Truck) are
already at effectively-zero lin_dict weight by the END OF EPOCH 1 in every collapsed run
-- ruling out a slow multi-epoch weight_decay-driven decay (weight_decay=1e-5 is far too
small to zero a weight in one epoch on its own) and pointing to something fast happening
within epoch 1's own gradient steps.

Architecture detail (model_classes/HGT.py:9-12,27-30): each node type's lin_dict[node_type]
= Linear(-1, hidden_channels) is a SEPARATE lazy layer (random init scale depends on that
type's own raw feature count / fan-in), but the PReLU right after it (self.pre_act) is a
SINGLE SHARED module reused for every node type -- so whatever makes the collapse
node-type-SELECTIVE has to trace to the per-type Linear layers (differing fan-in / how
rarely each type appears in a training batch), not the activation, which is identical for
all 8 types.

This script does NOT call Het_Reg_Modelling() -- it reimplements a minimal, manual version
of het_train()'s exact per-batch loop (training.py:254-271: zero_grad -> forward -> loss ->
backward -> clip_grad_norm_(max_norm=1.0) -> step, batch_size=16) so it can log at
per-batch granularity, which the production loop doesn't do. Two parts:

1. Node-type rarity census (no training at all) -- for each of the 8 node types: % of
   training graphs with >=1 node of that type, raw feature dimensionality (fan-in), and
   post-z-normalization feature magnitude stats.
2. Per-batch trace of epoch 1 ONLY, across a few seeds already known to collapse (default:
   0, 1, 2) -- after every batch, log each type's lin_dict weight norm, its PRE-clip
   gradient norm, and its POST-clip gradient norm (only when present in that batch), so
   the exact batch each doomed type's weight collapses -- and whether the step right
   before is a large-gradient overshoot vs. something else -- is directly visible.

Uses Depart's graph cache (the same backup/swap/restore dance as
experiment_logistics_depart_*.py) since Depart and LoadToVehicle share one non-KPI-suffixed
cache path. Does NOT touch the production checkpoint, model_params.json, or run more than
one epoch per seed -- this is read-only-in-spirit diagnostics, not a retrain.

Usage: python3 diagnose_depart_collapse_mechanism.py
"""
import shutil

import pandas as pd
import torch
from torch_geometric.loader import DataLoader

import training as t

CONFIG_PATH = "files/config.yml"
GRAPH_DIR = "files/hetero_structures/logistics"
DEPART_BACKUP = f"{GRAPH_DIR}/1000_customerorder_depart_backup"
LOADTOVEHICLE_BACKUP = f"{GRAPH_DIR}/1000_loadtovehicle_backup"
LIVE_DIR = f"{GRAPH_DIR}/1000"
GRAPH_FILES = ["train_graphs_sg.pt", "val_graphs_sg.pt", "test_graphs_sg.pt"]

LOGISTICS_LOADTOVEHICLE_LINE = '  kpi_event: "LoadToVehicle" # Depart'
LOGISTICS_DEPART_LINE = '  kpi_event: "Depart" # LoadToVehicle'

SEEDS = [0, 1, 2]
OUT_DIR = "files/models/logistics/1000/Hetero"
CENSUS_CSV = f"{OUT_DIR}/depart_nodetype_census.csv"
TRACE_CSV_TEMPLATE = f"{OUT_DIR}/depart_collapse_perbatch_seed{{seed}}.csv"

DEAD_THRESHOLD = 1e-4  # "effectively dead" cutoff for flagging a batch in the printed summary


def _swap_graphs(backup_dir):
    for fname in GRAPH_FILES:
        shutil.copy(f"{backup_dir}/{fname}", f"{LIVE_DIR}/{fname}")


def _set_kpi_event(target):
    old_line, new_line = (
        (LOGISTICS_LOADTOVEHICLE_LINE, LOGISTICS_DEPART_LINE) if target == "Depart"
        else (LOGISTICS_DEPART_LINE, LOGISTICS_LOADTOVEHICLE_LINE)
    )
    with open(CONFIG_PATH) as f:
        text = f.read()
    if text.count(old_line) != 1:
        raise RuntimeError(
            f"Expected exactly one occurrence of {old_line!r} in {CONFIG_PATH}, "
            f"found {text.count(old_line)} -- refusing to edit blindly.")
    text = text.replace(old_line, new_line)
    with open(CONFIG_PATH, "w") as f:
        f.write(text)


def node_type_census(m):
    """Purely descriptive -- no training. For each node type: presence rate across
    training graphs, raw feature dim, and post-normalization feature magnitude stats."""
    node_types = list(m.train_data[0].node_types)
    rows = []
    for nt in node_types:
        n_graphs_present = sum(1 for g in m.train_data if g[nt].num_nodes > 0)
        pct_present = 100.0 * n_graphs_present / len(m.train_data)
        feat_dim = m.train_data[0][nt].x.shape[1]
        all_x = torch.cat([g[nt].x for g in m.train_data if g[nt].num_nodes > 0], dim=0) \
            if n_graphs_present > 0 else None
        mean_abs = all_x.abs().mean().item() if all_x is not None else float('nan')
        std = all_x.std().item() if all_x is not None else float('nan')
        rows.append({
            'node_type': nt, 'pct_graphs_present': round(pct_present, 2),
            'feat_dim': feat_dim, 'mean_abs_feat': mean_abs, 'std_feat': std,
        })
        print(f"  {nt:20s} present in {pct_present:6.2f}% of graphs  "
              f"feat_dim={feat_dim}  mean|x|={mean_abs:.4f}  std={std:.4f}")
    df = pd.DataFrame(rows)
    df.to_csv(CENSUS_CSV, index=False)
    print(f"Saved census to {CENSUS_CSV}")
    return df


def per_batch_trace(m, seed, watched_types):
    """Manual replica of het_train()'s per-batch loop (training.py:254-271), instrumented
    to log per-node-type weight/gradient norms after every batch of epoch 1 only."""
    torch.manual_seed(seed)  # matches Het_Reg_Modelling's own training-loop seeding
    batch_size = m.path_dict.get('batch_size', 16)
    train_loader = DataLoader(m.train_data, batch_size=batch_size, shuffle=True)

    model = m.model.to(m.device)
    criterion = torch.nn.L1Loss()
    with torch.no_grad():
        batch0 = next(iter(train_loader)).to(m.device)
        model(batch0.x_dict, batch0.edge_index_dict)  # materialize lazy Linear dims
    optimizer = torch.optim.Adam(model.parameters(), lr=m.params['lr'],
                                  weight_decay=m.params['weight_decay'])

    rows = []
    for batch_idx, batch in enumerate(train_loader):
        batch = batch.to(m.device)
        optimizer.zero_grad()
        out = model(batch.x_dict, batch.edge_index_dict)
        mask = batch[m.kpi_viewpoint].mask.view(-1)
        y = batch[m.kpi_viewpoint].y.view(-1, out.shape[-1])
        loss = criterion(out[mask], y[mask])
        loss.backward()

        row = {'batch': batch_idx, 'loss': float(loss)}
        for nt in watched_types:
            n_present = int(batch[nt].num_nodes)
            w = model.lin_dict[nt].weight
            pre_clip_grad_norm = w.grad.norm().item() if w.grad is not None else 0.0
            row[f'{nt}_n_present'] = n_present
            row[f'{nt}_pre_clip_grad_norm'] = pre_clip_grad_norm
            row[f'{nt}_weight_norm_before'] = w.norm().item()

        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)

        for nt in watched_types:
            w = model.lin_dict[nt].weight
            post_clip_grad_norm = w.grad.norm().item() if w.grad is not None else 0.0
            row[f'{nt}_post_clip_grad_norm'] = post_clip_grad_norm

        optimizer.step()

        for nt in watched_types:
            w = model.lin_dict[nt].weight
            row[f'{nt}_weight_norm_after'] = w.norm().item()

        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = TRACE_CSV_TEMPLATE.format(seed=seed)
    df.to_csv(out_path, index=False)
    print(f"seed={seed}: saved {len(df)}-batch trace to {out_path}")

    print(f"seed={seed}: first batch each watched type's weight_norm_after drops below "
          f"{DEAD_THRESHOLD}:")
    for nt in watched_types:
        col = f'{nt}_weight_norm_after'
        below = df[df[col] < DEAD_THRESHOLD]
        if len(below) > 0:
            first_batch = int(below.iloc[0]['batch'])
            grad_at_death = df.loc[df['batch'] == first_batch, f'{nt}_pre_clip_grad_norm'].iloc[0]
            print(f"    {nt}: batch {first_batch} (of {len(df)})  "
                  f"pre-clip grad norm at that step={grad_at_death:.6f}")
        else:
            print(f"    {nt}: never below threshold in epoch 1 "
                  f"(final weight_norm={df[col].iloc[-1]:.6f})")
    return df


def main():
    print(f"Backing up current (LoadToVehicle) graph cache to {LOADTOVEHICLE_BACKUP}...")
    for fname in GRAPH_FILES:
        shutil.copy(f"{LIVE_DIR}/{fname}", f"{LOADTOVEHICLE_BACKUP}/{fname}")

    print("Swapping in Depart's graph cache and setting kpi_event='Depart'...")
    _swap_graphs(DEPART_BACKUP)
    _set_kpi_event("Depart")

    try:
        m = t.Modelling('logistics', 1000, seed=SEEDS[0])
        print(f"Task ID: {m.task_id}")
        print(f"Params: {m.params}")

        print(f"\n{'='*70}\nPART 1: node-type rarity census\n{'='*70}")
        census_df = node_type_census(m)
        watched_types = [nt for nt in m.train_data[0].node_types
                          if nt not in (m.kpi_viewpoint, 'Events')]

        for seed in SEEDS:
            print(f"\n{'='*70}\nPART 2: per-batch trace, seed={seed}\n{'='*70}")
            m_seed = t.Modelling('logistics', 1000, seed=seed)
            per_batch_trace(m_seed, seed, watched_types)

    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event...")
        _swap_graphs(LOADTOVEHICLE_BACKUP)
        _set_kpi_event("LoadToVehicle")
        print("Restored.")


if __name__ == '__main__':
    main()
