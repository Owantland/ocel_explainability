"""Phase 0 of the "find a genuinely stable Depart solution" investigation: fast check of
whether AdamW (decoupled weight decay) prevents the collapse mechanism
diagnose_depart_collapse_mechanism.py root-caused under plain Adam.

That diagnosis found: doomed node types' true loss-gradient is tiny (~1e-6), so once
Adam's L2-style weight_decay (added directly to the gradient BEFORE Adam's adaptive
per-parameter normalization) dominates, Adam treats the resulting consistent, always-
same-direction signal as confident and takes a step of size ~lr every batch, regardless
of the true gradient's magnitude -- a near-constant proportional weight shrink from batch
0 that compounds to zero within a few hundred of 2727 batches. AdamW's decoupled weight
decay is applied directly to the parameter as a fixed fraction, separately from the
gradient Adam's adaptive normalization sees -- removing exactly this interaction, in
principle.

This script reruns the SAME per-batch instrumentation as diagnose_depart_collapse_
mechanism.py's per_batch_trace(), against the EXACT collapse-inducing config
(hidden_channels=16, num_layers=3, lr=0.01, weight_decay=1e-5 -- Depart's actual deployed
hyperparameters), swapping only the optimizer (torch.optim.AdamW instead of Adam, via
Het_Reg_Modelling's new `optimizer` parameter -- see training.py). Only 2 seeds (fast
check, not a full multi-seed claim) -- if this looks promising, Phase 2's larger
multi-seed screen (retune_depart_full_search.py) follows up properly.

Does NOT call the production Het_Reg_Modelling for the full run here either -- same
manual per-batch loop as the Adam version, for the same per-batch-granularity reason.

Usage: python3 diagnose_depart_collapse_mechanism_adamw.py
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

SEEDS = [0, 1]
OUT_DIR = "files/models/logistics/1000/Hetero"
TRACE_CSV_TEMPLATE = f"{OUT_DIR}/depart_collapse_perbatch_adamw_seed{{seed}}.csv"
DEAD_THRESHOLD = 1e-4


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


def per_batch_trace_adamw(m, seed, watched_types):
    """Same as diagnose_depart_collapse_mechanism.py's per_batch_trace(), but AdamW
    instead of Adam -- the only change."""
    torch.manual_seed(seed)
    batch_size = m.path_dict.get('batch_size', 16)
    train_loader = DataLoader(m.train_data, batch_size=batch_size, shuffle=True)

    model = m.model.to(m.device)
    criterion = torch.nn.L1Loss()
    with torch.no_grad():
        batch0 = next(iter(train_loader)).to(m.device)
        model(batch0.x_dict, batch0.edge_index_dict)
    optimizer = torch.optim.AdamW(model.parameters(), lr=m.params['lr'],
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
            w = model.lin_dict[nt].weight
            row[f'{nt}_pre_clip_grad_norm'] = w.grad.norm().item() if w.grad is not None else 0.0
            row[f'{nt}_weight_norm_before'] = w.norm().item()

        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()

        for nt in watched_types:
            row[f'{nt}_weight_norm_after'] = model.lin_dict[nt].weight.norm().item()

        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = TRACE_CSV_TEMPLATE.format(seed=seed)
    df.to_csv(out_path, index=False)
    print(f"seed={seed}: saved {len(df)}-batch AdamW trace to {out_path}")

    print(f"seed={seed}: first batch each watched type's weight_norm_after drops below "
          f"{DEAD_THRESHOLD}:")
    for nt in watched_types:
        col = f'{nt}_weight_norm_after'
        below = df[df[col] < DEAD_THRESHOLD]
        if len(below) > 0:
            first_batch = int(below.iloc[0]['batch'])
            print(f"    {nt}: batch {first_batch} (of {len(df)})")
        else:
            print(f"    {nt}: never below threshold "
                  f"(final weight_norm={df[col].iloc[-1]:.6f}, "
                  f"initial={df[col].iloc[0]:.6f})")
    # Per-batch shrink ratio, first 30 batches -- directly comparable to the Adam version's
    # near-constant ~0.93-0.9999 finding.
    print(f"seed={seed}: per-batch weight_norm ratio (first 30 batches):")
    for nt in watched_types:
        ratios = df[f'{nt}_weight_norm_after'] / df[f'{nt}_weight_norm_before']
        print(f"    {nt:20s} mean={ratios[:30].mean():.5f}  std={ratios[:30].std():.5f}")
    return df


def main():
    print(f"Backing up current (LoadToVehicle) graph cache to {LOADTOVEHICLE_BACKUP}...")
    for fname in GRAPH_FILES:
        shutil.copy(f"{LIVE_DIR}/{fname}", f"{LOADTOVEHICLE_BACKUP}/{fname}")

    print("Swapping in Depart's graph cache and setting kpi_event='Depart'...")
    _swap_graphs(DEPART_BACKUP)
    _set_kpi_event("Depart")

    try:
        for seed in SEEDS:
            print(f"\n{'='*70}\nAdamW per-batch trace, seed={seed} "
                  f"(lr=0.01, weight_decay=1e-5 -- Depart's exact deployed config)\n{'='*70}")
            m = t.Modelling('logistics', 1000, seed=seed)
            watched_types = [nt for nt in m.train_data[0].node_types
                              if nt not in (m.kpi_viewpoint, 'Events')]
            per_batch_trace_adamw(m, seed, watched_types)

    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event...")
        _swap_graphs(LOADTOVEHICLE_BACKUP)
        _set_kpi_event("LoadToVehicle")
        print("Restored.")


if __name__ == '__main__':
    main()
