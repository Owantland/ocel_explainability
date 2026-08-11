"""Phase B: confirmatory experiment for diagnose_depart_collapse_mechanism.py's Phase A
finding.

Phase A's per-batch trace showed EXACTLY what kills the 5 doomed node types
(TransportDocument, Vehicle, HandlingUnit, Container, Truck -- plus the already-known-dead
Forklift): from batch 0, each one's lin_dict weight shrinks by a tight, near-constant
PROPORTION every single batch (e.g. Vehicle: ~1.5%/batch, Forklift: ~6.8%/batch, seed 0),
not a single catastrophic overshoot. This is the classic Adam + L2-style weight_decay
runaway-decay pattern: these types' TRUE loss gradient is tiny (~1e-6, confirmed directly
in the trace), so weight_decay's contribution (weight_decay * current_weight) quickly
dominates the total gradient Adam sees. Once a parameter's gradient is dominated by a
gradient that is *proportional to its own current value and always points the same way*,
Adam's per-parameter adaptive normalization (dividing by the RMS of recent gradients)
treats it as a confident, consistent signal and takes a step whose SIZE is close to `lr`
every batch -- at lr=0.01 that's a ~1-7%/batch proportional shrink, which compounds
exponentially into a dead weight within a few hundred of the epoch's 2727 batches.
CustomerOrder (the viewpoint, always present, directly determines the prediction) and
Events (always present, feat_dim=41) have strong enough true gradients to never enter this
regime.

This directly predicts: LOWERING lr should break the runaway, since Adam's step size for
this weight-decay-dominated regime scales with lr -- a smaller lr means a smaller
proportional shrink per batch, giving the (still weak but nonzero) true gradient more
relative influence, letting the weight stabilize instead of decaying to zero. This also
matches the ALREADY-documented Mechanism B precedent (project_hgt_dead_features_finding
memory, 2026-07-04): lr=0.01/weight_decay=0.001 reliably killed lin_dict; lr=0.001/
weight_decay=1e-5 reliably kept it alive. Today's finding refines *why* -- it's the ratio
between weight_decay's pull and the true gradient, amplified by lr via Adam, not
weight_decay's absolute value alone.

This script reruns the EXACT same seeds (0, 1, 2) as
experiment_logistics_depart_seed_robustness.py's baseline (which collapsed 3/3 -- and 5/5
across all seeds 0-4 -- at lr=0.01), changing ONLY lr to 0.001, same architecture
(hidden_channels=16, num_layers=3, num_heads=2, weight_decay=1e-5 unchanged), same
PRODUCTION budget (max_epochs=100, early_stop_patience=10 -- the logistics default, not
overridden). Directly comparable collapse-rate measurement.

Does NOT touch the production checkpoint or model_params.json -- saves under its own
`_phaseb_lowlr_seed{N}_experiment` suffix.

Usage: python3 diagnose_depart_collapse_phaseb_lowlr.py
"""
import shutil

import pandas as pd

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
LOW_LR = 0.001
OUT_CSV = "files/models/logistics/1000/Hetero/depart_phaseb_lowlr_summary.csv"

WATCHED_NODE_TYPES = ['TransportDocument', 'Vehicle', 'HandlingUnit', 'Container', 'Truck']
DEAD_THRESHOLD = 1e-6


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


def main():
    print(f"Backing up current (LoadToVehicle) graph cache to {LOADTOVEHICLE_BACKUP}...")
    for fname in GRAPH_FILES:
        shutil.copy(f"{LIVE_DIR}/{fname}", f"{LOADTOVEHICLE_BACKUP}/{fname}")

    print("Swapping in Depart's graph cache and setting kpi_event='Depart'...")
    _swap_graphs(DEPART_BACKUP)
    _set_kpi_event("Depart")

    results = []
    try:
        for seed in SEEDS:
            print(f"\n{'='*70}\nSEED {seed} -- lr={LOW_LR} (vs. production 0.01), "
                  f"production budget (max_epochs=100, early_stop_patience=10)\n{'='*70}")
            m = t.Modelling('logistics', 1000, seed=seed)
            m.params['lr'] = LOW_LR
            m.model_path = m.model_path.replace(
                ".pth", f"_phaseb_lowlr_seed{seed}_experiment.pth")
            print(f"Params (lr overridden): {m.params}")
            m.Het_Reg_Modelling(m.train_data, m.val_data, m.test_data)

            dead_types = [nt for nt in WATCHED_NODE_TYPES
                          if m.model.lin_dict[nt].weight.norm().item() < DEAD_THRESHOLD]
            collapsed = len(dead_types) >= 2

            import baselines as bl
            hgt_df, _ = bl.hgt_predictions(m)
            last = hgt_df[hgt_df['last_event']]
            m_last = bl.metrics(last['true_h'].values, last['hgt_pred_h'].values)

            print(f"seed={seed}: dead node types (of {WATCHED_NODE_TYPES}) = {dead_types}")
            print(f"seed={seed}: collapsed={collapsed}  "
                  f"MAE_last={m_last['mae']:.3f}h  R2_last={m_last['r2']:.3f}")

            results.append({
                'seed': seed, 'lr': LOW_LR, 'collapsed': collapsed,
                'n_dead_watched_types': len(dead_types), 'dead_types': ",".join(dead_types),
                'mae_last': m_last['mae'], 'r2_last': m_last['r2'],
            })
            pd.DataFrame(results).to_csv(OUT_CSV, index=False)

    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event...")
        _swap_graphs(LOADTOVEHICLE_BACKUP)
        _set_kpi_event("LoadToVehicle")
        print("Restored.")

    df = pd.DataFrame(results)
    print(f"\n{'='*70}\nSUMMARY across {len(SEEDS)} seeds at lr={LOW_LR}\n{'='*70}")
    print(df.to_string(index=False))
    n_collapsed = df['collapsed'].sum()
    print(f"\n{n_collapsed}/{len(SEEDS)} seeds collapsed at lr={LOW_LR} "
          f"(baseline at lr=0.01, same seeds: 3/3 collapsed)")
    print(f"Saved summary to {OUT_CSV}")


if __name__ == '__main__':
    main()
