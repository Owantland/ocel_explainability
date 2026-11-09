"""Follow-up to experiment_logistics_depart_longpatience.py: that script found HGT on
logistics/CustomerOrder->Depart collapsing into a widespread dead-projection pathology
(6 of 8 node types at exactly-zero lin_dict weight) on TWO consecutive reruns -- one
unseeded, one seeded (torch.manual_seed(42), after training.py's Modelling.__init__ was
fixed to seed BEFORE model construction, see that fix's commit). Both reruns collapsed at
nearly the same severity, while the currently-deployed production checkpoint did NOT
(only the already-documented dead Forklift, all other node types healthy).

Since the collapse is present from epoch 1 itself in both reruns (before training has had
time to meaningfully diverge), this points to the RANDOM WEIGHT INITIALIZATION itself
determining whether specific node-type input projections die immediately (a dying-ReLU/
PReLU-style pathology at init, consistent with this project's already-documented
"Mechanism B" -- see project_hgt_dead_features_finding memory) -- not something that
epoch/patience budget can fix.

This script directly tests that hypothesis: train Depart's HGT from scratch across
SEVERAL different init seeds (0-4), at the PRODUCTION budget (max_epochs=100,
early_stop_patience=10 -- NOT the extended long-patience budget, since the question here
is "how often does the PRODUCTION recipe collapse", not "does more patience help"), and
record for each seed whether it collapses (>=2 non-Forklift node types dead) or trains
cleanly like the deployed checkpoint. `Modelling(..., seed=N)` now exists specifically to
support this (training.py, seed defaults to 42 for every other existing caller -- this is
the only script that varies it).

Does NOT touch the production checkpoint or model_params.json -- each seed's checkpoint
saves under its own `_seedrobustness_seed{N}_experiment` suffix.

Usage: python3 experiment_logistics_depart_seed_robustness.py
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

SEEDS = [0, 1, 2, 3, 4]
OUT_CSV = "files/models/logistics/1000/Hetero/depart_seed_robustness_summary.csv"

# node types other than the viewpoint (CustomerOrder) and Events, which reliably retain
# weight even in a collapsed run (see the long-patience experiment's logs) -- Forklift is
# excluded from the "collapsed" tally since it's already independently documented as
# reliably dead regardless of collapse (project_hgt_dead_features_finding memory).
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
            print(f"\n{'='*70}\nSEED {seed} -- production budget "
                  f"(max_epochs=100, early_stop_patience=10)\n{'='*70}")
            m = t.Modelling('logistics', 1000, seed=seed)
            m.model_path = m.model_path.replace(
                ".pth", f"_seedrobustness_seed{seed}_experiment.pth")
            # production budget for logistics is already the __init__ default (100/10) --
            # left unchanged here, deliberately, since this tests the deployed RECIPE's
            # reliability, not a different budget.
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
                'seed': seed, 'collapsed': collapsed,
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
    print(f"\n{'='*70}\nSUMMARY across {len(SEEDS)} seeds\n{'='*70}")
    print(df.to_string(index=False))
    n_collapsed = df['collapsed'].sum()
    print(f"\n{n_collapsed}/{len(SEEDS)} seeds collapsed "
          f"(production checkpoint reference: MAE_last=7.470h, R2_last=-0.746)")
    print(f"Saved summary to {OUT_CSV}")


if __name__ == '__main__':
    main()
