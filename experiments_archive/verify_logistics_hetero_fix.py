"""
One-off verification of hetero_graphs.py's uncommitted changes (preprocessing_steps() bug fix +
fail-loud loading/reshape) against the logistics database, cant=1000, kpi_viewpoint=CustomerOrder.

Rebuilds ev_log/all_kpis -> train/test split -> hetero graph cache under the current code, then
diffs the rebuilt {train,val,test}_graphs_sg.pt against the pre-fix backup taken at
files/graph_structures/logistics/1000_prehterofix_backup/ and
files/hetero_structures/logistics/1000_prehterofix_backup/ (copied before this script ran).

Does NOT retrain any model -- only rebuilds the graph cache and checks downstream load/inference
compatibility with the existing TimeFrom_CustomerOrder_to_Depart checkpoint.
"""
import sys
import torch

import process_generation as pg
import train_test_builder as tb
import hetero_graphs as hg
import training as t

DATABASE = 'logistics'
CANT = 1000
BACKUP_HETERO = f'files/hetero_structures/{DATABASE}/{CANT}_prehterofix_backup'
LIVE_HETERO = f'files/hetero_structures/{DATABASE}/{CANT}'

# ── 1. Rebuild ev_log/all_kpis -> split -> hetero graphs under current code ──────
print("=" * 70)
print("STAGE 1: regenerate ev_log/all_kpis (ProcessGeneration)")
print("=" * 70)
p = pg.ProcessGeneration(DATABASE, CANT)
nodes = p.related_nodes()
p.get_ev_log(nodes)

print("\n" + "=" * 70)
print("STAGE 2: rebuild train/val/test split (TrainTestBuilder)")
print("=" * 70)
ttb = tb.TrainTestBuilder(DATABASE, CANT)
train_ts, val_ts, test_ts = ttb.timestamps_generator()
print(f"  train={len(train_ts)} val={len(val_ts)} test={len(test_ts)}")

print("\n" + "=" * 70)
print("STAGE 3: rebuild hetero graph cache (HeteroGraphsGenerator)")
print("=" * 70)
hgg = hg.HeteroGraphsGenerator(DATABASE, CANT, train_ts, val_ts, test_ts)
hgg.trace_kpi()
print("Rebuild completed with no unhandled exceptions.")

# ── 2. Diff old (backup) vs new (just-rebuilt) cache ──────────────────────────
print("\n" + "=" * 70)
print("STAGE 4: diff pre-fix backup vs rebuilt cache")
print("=" * 70)

overall_ok = True

def load_split(base, split):
    return torch.load(f"{base}/{split}_graphs_sg.pt", weights_only=False)

for split in ['train', 'val', 'test']:
    old_graphs = load_split(BACKUP_HETERO, split)
    new_graphs = load_split(LIVE_HETERO, split)

    print(f"\n--- {split} ---")
    print(f"  graph count: old={len(old_graphs)} new={len(new_graphs)}")
    if len(old_graphs) != len(new_graphs):
        print(f"  MISMATCH: graph counts differ.")
        overall_ok = False
        continue

    n_mismatch = 0
    n_multi_co_old = 0
    n_multi_co_new = 0
    for i, (og, ng) in enumerate(zip(old_graphs, new_graphs)):
        if og['CustomerOrder'].num_nodes > 1:
            n_multi_co_old += 1
        if ng['CustomerOrder'].num_nodes > 1:
            n_multi_co_new += 1

        mismatch_reasons = []
        old_types = set(og.node_types)
        new_types = set(ng.node_types)
        if old_types != new_types:
            mismatch_reasons.append(f"node_types differ: {old_types} vs {new_types}")
        else:
            for ntype in old_types:
                if og[ntype].num_nodes != ng[ntype].num_nodes:
                    mismatch_reasons.append(f"{ntype}.num_nodes {og[ntype].num_nodes} vs {ng[ntype].num_nodes}")
                    continue
                if hasattr(og[ntype], 'x') and hasattr(ng[ntype], 'x'):
                    if not torch.allclose(og[ntype].x, ng[ntype].x, atol=1e-5, equal_nan=True):
                        mismatch_reasons.append(f"{ntype}.x differs")
                if hasattr(og[ntype], 'y') and hasattr(ng[ntype], 'y'):
                    if not torch.allclose(og[ntype].y, ng[ntype].y, atol=1e-5, equal_nan=True):
                        mismatch_reasons.append(f"{ntype}.y differs (old={og[ntype].y.flatten().tolist()}, new={ng[ntype].y.flatten().tolist()})")
                if hasattr(og[ntype], 'mask') and hasattr(ng[ntype], 'mask'):
                    if not torch.equal(og[ntype].mask, ng[ntype].mask):
                        mismatch_reasons.append(f"{ntype}.mask differs (old={og[ntype].mask.flatten().tolist()}, new={ng[ntype].mask.flatten().tolist()})")

        old_edge_types = set(og.edge_types)
        new_edge_types = set(ng.edge_types)
        if old_edge_types != new_edge_types:
            mismatch_reasons.append(f"edge_types differ: {old_edge_types} vs {new_edge_types}")
        else:
            for etype in old_edge_types:
                oei = og[etype].edge_index
                nei = ng[etype].edge_index
                if oei.shape != nei.shape or not torch.equal(oei, nei):
                    mismatch_reasons.append(f"{etype}.edge_index differs")

        if mismatch_reasons:
            n_mismatch += 1
            if n_mismatch <= 5:
                print(f"  [graph {i}] MISMATCH: {'; '.join(mismatch_reasons)}")

    print(f"  graphs with >1 CustomerOrder node: old={n_multi_co_old} new={n_multi_co_new}")
    print(f"  graphs with any tensor mismatch: {n_mismatch} / {len(old_graphs)}")
    if n_mismatch > 0:
        overall_ok = False

print("\n" + "=" * 70)
print("STAGE 4 RESULT:", "NO DIFFERENCES (fix confirmed no-op for logistics)" if overall_ok else "DIFFERENCES FOUND -- see above")
print("=" * 70)

# ── 3. Downstream compatibility smoke test ────────────────────────────────────
print("\n" + "=" * 70)
print("STAGE 5: downstream compatibility -- training.Modelling + existing checkpoint")
print("=" * 70)
try:
    m = t.Modelling(DATABASE, CANT)
    print(f"  Modelling('{DATABASE}', {CANT}) initialized OK. task_id={m.task_id}")
    ckpt_path = m.model_path
    print(f"  Loading checkpoint: {ckpt_path}")
    m.model.load_state_dict(torch.load(ckpt_path, weights_only=False))
    m.model.eval()
    n_checked = 0
    n_bad = 0
    with torch.no_grad():
        for g in m.test_data[:20]:
            pred = m.model(g.x_dict, g.edge_index_dict)
            val = pred[0].item() if pred.numel() > 0 else float('nan')
            n_checked += 1
            if not torch.isfinite(torch.tensor(val)):
                n_bad += 1
    print(f"  Ran inference on {n_checked} test graphs; non-finite predictions: {n_bad}")
    if n_bad == 0 and n_checked > 0:
        print("  Checkpoint remains usable on rebuilt cache -- no retraining necessitated by this fix.")
    else:
        print("  WARNING: some predictions were non-finite or no graphs were checked.")
        overall_ok = False
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")
    overall_ok = False

print("\n" + "=" * 70)
print("FINAL RESULT:", "PASS" if overall_ok else "FAIL -- see details above")
print("=" * 70)
sys.exit(0 if overall_ok else 1)
