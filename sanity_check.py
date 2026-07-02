"""
Sanity check for the training and validation pipeline.

Runs 6 sequential check groups and prints PASS / FAIL for each.
Exits with code 1 if any check fails.

Usage: python sanity_check.py
"""
import sys
import os
import json
import torch
import warnings
from collections import defaultdict
from torch_geometric.loader import DataLoader

warnings.filterwarnings("ignore")

DATABASE = 'order_management'
CANT = 2000

# ── helpers ─────────────────────────────────────────────────────────────────

failures = []

def _ok(name, detail=""):
    pad = max(0, 45 - len(name))
    print(f"  [PASS] {name}{' ' * pad}{detail}")

def _fail(name, detail=""):
    pad = max(0, 45 - len(name))
    print(f"  [FAIL] {name}{' ' * pad}{detail}")
    failures.append(name)

def _warn(msg):
    print(f"  [WARN] {msg}")

def _info(msg):
    print(f"         {msg}")

def _header(n, title):
    print(f"\n{'='*60}")
    print(f"CHECK {n} — {title}")
    print('='*60)

# ── check 1: artefact existence ───────────────────────────────────────────

_header(1, "Artefact Existence")

import sup_funcs as sf
funcs = sf.SupportFunctions(DATABASE, CANT)
path_dict = funcs.get_paths()

vp      = path_dict['kpi_viewpoint']
kpi_ev  = path_dict['kpi_event']
task_id = f"TimeFrom_{vp}_to_{kpi_ev}"

required = {
    "train graphs"      : f"{path_dict['pytorch_path']}/train_graphs_sg.pt",
    "val graphs"        : f"{path_dict['pytorch_path']}/val_graphs_sg.pt",
    "test graphs"       : f"{path_dict['pytorch_path']}/test_graphs_sg.pt",
    "model checkpoint"  : f"{path_dict['model_path']}/Hetero/{task_id}.pth",
    "model_params.json" : f"{path_dict['model_path']}/Hetero/model_params.json",
    "tensor_dict.json"  : f"{path_dict['graph_output_path']}/tensor_dict.json",
}

for label, path in required.items():
    if os.path.exists(path) and os.path.getsize(path) > 0:
        _ok(label, path)
    else:
        _fail(label, f"MISSING or empty: {path}")

# ── check 2: data integrity (raw, pre-normalisation) ─────────────────────

_header(2, "Data Integrity (raw data)")

# Load raw graphs directly — bypass Modelling.__init__ to see unnormalised values
raw_train = torch.load(f"{path_dict['pytorch_path']}/train_graphs_sg.pt", weights_only=False)
raw_val   = torch.load(f"{path_dict['pytorch_path']}/val_graphs_sg.pt",   weights_only=False)
raw_test  = torch.load(f"{path_dict['pytorch_path']}/test_graphs_sg.pt",  weights_only=False)

# --- 2a. Internal feature-dim consistency across all splits ---
# Build reference dims from the first training graph
ref_dims = {}
for nt in raw_train[0].node_types:
    if hasattr(raw_train[0][nt], 'x') and raw_train[0][nt].x is not None:
        ref_dims[nt] = raw_train[0][nt].x.size(-1)

mismatch_found = False
for split_name, split in [("train", raw_train), ("val", raw_val), ("test", raw_test)]:
    for i, g in enumerate(split):
        for nt, expected_dim in ref_dims.items():
            if g[nt].num_nodes > 0:
                actual = g[nt].x.size(-1)
                if actual != expected_dim:
                    _fail("Feature dim consistency",
                          f"{split_name}[{i}] {nt}: expected {expected_dim}, got {actual}")
                    mismatch_found = True
                    break
        if mismatch_found:
            break
    if mismatch_found:
        break

if not mismatch_found:
    _ok("Feature dim consistency",
        f"  ({', '.join(f'{k}={v}' for k,v in ref_dims.items())})")

# --- 2b. Raw y-values are positive ---
all_y = torch.cat([g[vp].y.flatten() for g in raw_train])
n_neg = (all_y <= 0).sum().item()
if n_neg == 0:
    _ok("y-values positive (raw train)")
else:
    _fail("y-values positive (raw train)", f"{n_neg} non-positive values found")

# --- 2c. y-value range plausible (100h–500h mean on train) ---
mean_h = all_y.mean().item() / 3600.0
if 100 <= mean_h <= 500:
    _ok("y mean in plausible range [100h–500h]", f"mean={mean_h:.1f}h")
else:
    _fail("y mean in plausible range [100h–500h]", f"mean={mean_h:.1f}h")

# --- 2d. No order_id overlap between splits ---
def _ids(split):
    return {int(g[vp].id[0].item()) for g in split}

train_ids = _ids(raw_train)
val_ids   = _ids(raw_val)
test_ids  = _ids(raw_test)

tv_overlap = train_ids & val_ids
tt_overlap = train_ids & test_ids
vt_overlap = val_ids   & test_ids

if not tv_overlap and not tt_overlap and not vt_overlap:
    _ok("No order_id overlap between splits",
        f"  train={len(train_ids)}, val={len(val_ids)}, test={len(test_ids)}")
else:
    if tv_overlap:
        _fail("No train/val order_id overlap", f"{len(tv_overlap)} shared IDs")
    if tt_overlap:
        _fail("No train/test order_id overlap", f"{len(tt_overlap)} shared IDs")
    if vt_overlap:
        _fail("No val/test order_id overlap", f"{len(vt_overlap)} shared IDs")

# --- 2e. Each test order has exactly one last_event=True graph ---
last_ev_count = defaultdict(int)
for g in raw_test:
    oid = int(g[vp].id[0].item())
    if g[vp].last_event[0].item():
        last_ev_count[oid] += 1

zero_last = [oid for oid in test_ids if last_ev_count[oid] == 0]
multi_last = [oid for oid in test_ids if last_ev_count[oid] > 1]

if not zero_last and not multi_last:
    _ok("Exactly one last_event per test order",
        f"  ({len(test_ids)} orders checked)")
else:
    if zero_last:
        _fail("last_event missing", f"{len(zero_last)} test orders have no last_event graph")
    if multi_last:
        _fail("last_event duplicated", f"{len(multi_last)} test orders have >1 last_event graph")

# ── check 3: normalisation correctness ───────────────────────────────────

_header(3, "Normalisation Correctness")

import training as t
m = t.Modelling(DATABASE, CANT)

# --- 3a. Train y after normalisation: mean≈0, std≈1 ---
norm_y = torch.cat([g[vp].y.flatten() for g in m.train_data])
y_mean = norm_y.mean().item()
y_std  = norm_y.std().item()

if abs(y_mean) < 0.05:
    _ok("Normalised train y mean ≈ 0", f"mean={y_mean:.4f}")
else:
    _fail("Normalised train y mean ≈ 0", f"mean={y_mean:.4f} (expected |mean|<0.05)")

if abs(y_std - 1.0) < 0.1:
    _ok("Normalised train y std ≈ 1", f"std={y_std:.4f}")
else:
    _fail("Normalised train y std ≈ 1", f"std={y_std:.4f} (expected |std-1|<0.1)")

# --- 3b. target_mean/std are in seconds (not hours) ---
tm = m.target_mean.item()
ts = m.target_std.item()
if tm > 300_000:   # >83h in seconds
    _ok("target_mean in seconds", f"mean={tm:.0f}s ({tm/3600:.1f}h)")
else:
    _fail("target_mean in seconds", f"mean={tm:.1f} — looks like hours, not seconds?")

if ts > 100_000:
    _ok("target_std in seconds", f"std={ts:.0f}s ({ts/3600:.1f}h)")
else:
    _fail("target_std in seconds", f"std={ts:.1f} — looks like hours, not seconds?")

# --- 3c. Normalised node features on training data have mean ≈ 0 per dim ---
# Check Events and viewpoint_object as representative types
problem_features = []
for nt in ['Events', vp]:
    xs = [g[nt].x for g in m.train_data if g[nt].num_nodes > 0]
    if not xs:
        continue
    x_cat = torch.cat(xs, dim=0)
    per_dim_mean = x_cat.mean(dim=0).abs()
    bad = (per_dim_mean > 0.1).nonzero(as_tuple=True)[0].tolist()
    if bad:
        problem_features.append(f"{nt} dims {bad} (max|mean|={per_dim_mean.max():.3f})")

if not problem_features:
    _ok("Normalised node features mean ≈ 0 (Events, Orders)")
else:
    _fail("Normalised node features mean ≈ 0", "; ".join(problem_features))

# --- 3d. Val/test normalised y within ±5σ ---
out_of_range = 0
for split_name, split in [("val", m.val_data), ("test", m.test_data)]:
    for g in split:
        vals = g[vp].y.flatten()
        out_of_range += ((vals < -5) | (vals > 5)).sum().item()

if out_of_range == 0:
    _ok("Val/test normalised y within ±5σ")
else:
    _warn(f"Val/test y: {out_of_range} values outside ±5σ (extreme outlier orders)")

# ── check 4: forward pass & loss ─────────────────────────────────────────

_header(4, "Forward Pass & Loss")

m.model.load_state_dict(torch.load(m.model_path, weights_only=False))

# --- 4a. Output shape ---
m.model.eval()
loader = DataLoader(m.test_data, batch_size=4)
batch  = next(iter(loader))

with torch.no_grad():
    out = m.model(batch.x_dict, batch.edge_index_dict)

n_vp_batch = batch[vp].y.size(0)
if out.shape == (n_vp_batch, 1):
    _ok("Output shape correct", f"out.shape={list(out.shape)}")
else:
    _fail("Output shape correct",
          f"out.shape={list(out.shape)}, expected [{n_vp_batch}, 1]")

# --- 4b. No NaN/Inf in output ---
if torch.isfinite(out).all():
    _ok("No NaN/Inf in output")
else:
    n_bad = (~torch.isfinite(out)).sum().item()
    _fail("No NaN/Inf in output", f"{n_bad} non-finite values")

# --- 4c. Loss computation is finite ---
criterion = torch.nn.L1Loss()
with torch.no_grad():
    loss_val = criterion(out, batch[vp].y).item()

if torch.isfinite(torch.tensor(loss_val)) and loss_val > 0:
    _ok("Loss is finite and positive", f"L1={loss_val:.4f}")
else:
    _fail("Loss is finite and positive", f"L1={loss_val}")

# --- 4d. Gradient flow: all params receive gradients ---
# Use a large batch so that every node type has at least some nodes represented
# (small batches can have 0-node types → no gradient for their projection matrices)
big_loader = DataLoader(m.train_data, batch_size=64, shuffle=False)
big_batch  = next(iter(big_loader))

# Identify which node types are present in this batch (num_nodes > 0)
active_types = {nt for nt in big_batch.node_types if big_batch[nt].num_nodes > 0}

m.model.train()
out_train  = m.model(big_batch.x_dict, big_batch.edge_index_dict)
loss_train = criterion(out_train, big_batch[vp].y)
loss_train.backward()

params_no_grad   = []   # unexpected None grads (active node type)
params_zero_grad = []   # zero grads (active node type)

for name, p in m.model.named_parameters():
    if not p.requires_grad:
        continue
    # Determine which node type this parameter is associated with
    param_type = None
    for nt in big_batch.node_types:
        if f".{nt}." in name or f".{nt}]" in name:
            param_type = nt
            break
    # Skip parameters for node types absent from this batch
    if param_type is not None and param_type not in active_types:
        continue
    if p.grad is None:
        params_no_grad.append(name)
    elif p.grad.abs().sum().item() == 0:
        params_zero_grad.append(name)

m.model.zero_grad()
m.model.eval()

# Separate expected-dead params: any param in convs[last_layer] that writes to a
# non-viewpoint node type (out_lin AND skip connections). Their outputs are discarded
# after the last layer since only x_dict[viewpoint] feeds the output head.
n_layers    = m.params['num_layers']
last_prefix = f"convs.{n_layers - 1}."
non_vp_types = [nt for nt in big_batch.node_types if nt != vp]

def _is_expected_dead(name):
    if last_prefix not in name:
        return False
    return any(f".{nt}" in name or f".{nt}[" in name or name.endswith(f".{nt}")
               for nt in non_vp_types)

unexpected_no_grad = [p for p in params_no_grad if not _is_expected_dead(p)]
dead_no_grad       = [p for p in params_no_grad if _is_expected_dead(p)]

absent = sorted(set(big_batch.node_types) - active_types)
if not unexpected_no_grad and not params_zero_grad:
    detail = f"  (batch_size=64)"
    _ok("All active parameters receive non-zero gradients", detail)
    if dead_no_grad:
        _warn(f"{len(dead_no_grad)} dead params in final conv out_lin for non-viewpoint types "
              f"(architectural — their outputs are discarded after the last layer). "
              f"Consider applying out_lin only to the viewpoint type in the final layer.")
else:
    if unexpected_no_grad:
        _fail("Gradient flow (unexpected None grads)",
              f"{len(unexpected_no_grad)} params: {unexpected_no_grad[:3]}")
    if params_zero_grad:
        _warn(f"Zero gradients on {len(params_zero_grad)} params: {params_zero_grad[:3]}")
    if dead_no_grad:
        _warn(f"{len(dead_no_grad)} expected-dead params (final conv out_lin, non-viewpoint)")
if absent:
    _info(f"Node types absent from batch {absent}: their params excluded from gradient check")

# --- 4e. Denormalised predictions in plausible range ---
with torch.no_grad():
    out_eval = m.model(batch.x_dict, batch.edge_index_dict)
pred_h = (out_eval * m.target_std + m.target_mean) / 3600.0
if (pred_h >= 0).all() and (pred_h <= 1000).all():
    _ok("Denormalised predictions in [0h, 1000h]",
        f"range=[{pred_h.min().item():.1f}h, {pred_h.max().item():.1f}h]")
else:
    n_neg = (pred_h < 0).sum().item()
    n_big = (pred_h > 1000).sum().item()
    _fail("Denormalised predictions in [0h, 1000h]",
          f"{n_neg} negative, {n_big} >1000h")

# ── check 5: checkpoint round-trip ────────────────────────────────────────

_header(5, "Checkpoint Round-Trip")

# --- 5a. Two independent loads produce identical predictions ---
from model_classes import HGT

params = m.params
model_a = HGT.HGT(
    hidden_channels=params['hidden_channels'], out_channels=1,
    num_layers=params['num_layers'],           num_heads=params['num_heads'],
    data=m.train_data[0],                      viewpoint=vp,
).to(m.device)
model_b = HGT.HGT(
    hidden_channels=params['hidden_channels'], out_channels=1,
    num_layers=params['num_layers'],           num_heads=params['num_heads'],
    data=m.train_data[0],                      viewpoint=vp,
).to(m.device)

# Materialise lazy layers on a dummy forward pass
with torch.no_grad():
    _dummy = next(iter(DataLoader(m.test_data, batch_size=1)))
    model_a(_dummy.x_dict, _dummy.edge_index_dict)
    model_b(_dummy.x_dict, _dummy.edge_index_dict)

model_a.load_state_dict(torch.load(m.model_path, weights_only=False))
model_b.load_state_dict(torch.load(m.model_path, weights_only=False))
model_a.eval()
model_b.eval()

sample_graphs = m.test_data[:10]
loader_sample = DataLoader(sample_graphs, batch_size=10)
b = next(iter(loader_sample))

with torch.no_grad():
    preds_a = model_a(b.x_dict, b.edge_index_dict)
    preds_b = model_b(b.x_dict, b.edge_index_dict)

if torch.allclose(preds_a, preds_b, atol=1e-6):
    _ok("Two independent checkpoint loads produce identical predictions")
else:
    max_diff = (preds_a - preds_b).abs().max().item()
    _fail("Two independent checkpoint loads produce identical predictions",
          f"max diff={max_diff:.2e}")

# --- 5b. model_params.json matches actual model ---
with open(m._params_path) as f:
    saved_params = json.load(f).get(m.task_id, {})

mismatched_keys = []
for k in ['hidden_channels', 'num_layers', 'num_heads']:
    if k in saved_params and saved_params[k] != params[k]:
        mismatched_keys.append(f"{k}: json={saved_params[k]}, loaded={params[k]}")

if not mismatched_keys:
    _ok("model_params.json matches loaded params")
else:
    _fail("model_params.json matches loaded params", "; ".join(mismatched_keys))

# --- 5c. target_mean/std sidecar warning ---
norm_sidecar = m.model_path.replace(".pth", "_norm.json")
if os.path.exists(norm_sidecar):
    _ok("Normalisation sidecar exists", norm_sidecar)
else:
    _warn(f"No normalisation sidecar ({task_id}_norm.json). target_mean/std are re-derived "
          f"from train graphs at runtime — will be wrong if graphs are regenerated. "
          f"See Improvement #1 in the audit report.")

# ── check 6: sweep / training procedure consistency (informational) ────────

_header(6, "Sweep / Training Discrepancy (informational — no PASS/FAIL)")

with open(m._params_path) as f:
    all_saved = json.load(f)

saved = all_saved.get(m.task_id, {})
print()
print(f"  Saved hyperparameters for '{m.task_id}':")
for k, v in saved.items():
    print(f"    {k:<20}: {v}")

print()
print("  Sweep vs. final training discrepancies:")
print("    sweep() patience      :  5 epochs   |  Het_Reg_Modelling: 20 epochs")
print("    sweep() max_epochs    : 50           |  Het_Reg_Modelling: 200 epochs")
print("    sweep() LR scheduler  : NONE         |  Het_Reg_Modelling: ReduceLROnPlateau")
print()
print("  → Hyperparameters found on short 50-epoch runs may not be optimal for 200-epoch")
print("    final training (different convergence curve, no scheduler warm-up).")
print("    Consider aligning patience/epochs/scheduler between sweep and final training.")

# ── summary ───────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
if failures:
    print(f"RESULT: {len(failures)} check(s) FAILED:")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print("RESULT: All checks passed.")
