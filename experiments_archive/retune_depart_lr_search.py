"""Retunes logistics/Depart's HGT learning rate, following up on
project_hgt_dead_features_finding memory's 2026-08-04 root-cause: `lr=0.01` (the currently
deployed value) reliably collapses training via an Adam + weight_decay runaway-decay
pathology (7 of 8 known from-scratch attempts collapsed), and a direct test confirmed
`lr=0.001` prevents the collapse (0/3 seeds) but was never actually *tuned* for accuracy --
it was just the other value already in sweep()'s own lr grid, dropped in without a search.

Why a real retune is needed rather than just adopting 0.001: sweep()'s objective() (see
training.py) already includes lr in {1e-3, 1e-2}, but each Optuna trial only ever tries ONE
seed -- whatever the global RNG happened to be at that grid cell, not a controlled seed.
That's exactly how lr=0.01 got selected as "best" for Depart originally: it happened to
land a non-collapsed draw in that one trial. This script fixes that gap by testing several
seeds per lr candidate.

Two stages, both reusing training.Modelling/Het_Reg_Modelling directly (no reimplemented
training loop -- unlike diagnose_depart_collapse_mechanism.py, this only needs per-RUN
outcomes, not per-batch detail):

Stage 1 (cheap screen): lr in {0.001, 0.002, 0.003, 0.005, 0.007} (0.01 excluded, already
disproven) x seed in {0,1,2} = 15 runs, at the SHORT search budget sweep() itself uses for
screening (max_epochs=30, early_stop_patience=4) -- collapse is fully visible well within
this window (per the per-batch trace: doomed types die within a few hundred of 2727 batches
in epoch 1 alone). hidden_channels/num_layers/num_heads/weight_decay all held at Depart's
already-validated values (16/3/2/1e-5) -- this retunes lr only.

Stage 2 (confirmation): the lr with the lowest Stage-1 collapse rate (tie-break: lowest
mean val MAE among 0%-collapse candidates), same 3 seeds, at the PRODUCTION budget
(max_epochs=100, early_stop_patience=10 -- logistics' actual deployed budget, i.e. NOT
overridden). Full last-event test MAE/R2 via baselines.hgt_predictions()/metrics(), directly
comparable to the deployed checkpoint's reference numbers (MAE_last=7.470h, R2_last=-0.746).

Does NOT touch the production checkpoint or model_params.json -- every run here saves under
its own suffixed filename. The decision gate (promote a winning seed to production if
non-collapsed AND MAE_last <= 7.47h) is applied and executed SEPARATELY, after reviewing
this script's output, not automatically inside it -- overwriting production files
unattended for a multi-hour run is a deliberate, separate, verified step.

Usage: python3 retune_depart_lr_search.py
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

LR_CANDIDATES = [0.001, 0.002, 0.003, 0.005, 0.007]
SEEDS = [0, 1, 2]
SEARCH_MAX_EPOCHS = 30
SEARCH_PATIENCE = 4
PRODUCTION_MAE_LAST_REF = 7.470012
PRODUCTION_R2_LAST_REF = -0.745665

OUT_DIR = "files/models/logistics/1000/Hetero"
STAGE1_CSV = f"{OUT_DIR}/depart_lrretune_stage1_search.csv"
STAGE2_CSV = f"{OUT_DIR}/depart_lrretune_stage2_confirmation.csv"

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


def is_collapsed(model):
    dead = [nt for nt in WATCHED_NODE_TYPES
            if model.lin_dict[nt].weight.norm().item() < DEAD_THRESHOLD]
    return len(dead) >= 2, dead


def run_one(lr, seed, max_epochs, patience, tag):
    m = t.Modelling('logistics', 1000, seed=seed)
    m.params['lr'] = lr
    m.max_epochs = max_epochs
    m.early_stop_patience = patience
    m.model_path = m.model_path.replace(".pth", f"_{tag}_lr{lr}_seed{seed}_experiment.pth")
    m.Het_Reg_Modelling(m.train_data, m.val_data, m.test_data)

    collapsed, dead_types = is_collapsed(m.model)
    log_df = pd.read_csv(m.model_path.replace(".pth", "_training_log.csv"))
    best_val_mae = float(log_df['val_mae'].min())

    return m, collapsed, dead_types, best_val_mae


def stage1(seeds=SEEDS):
    print(f"\n{'='*70}\nSTAGE 1 -- search budget ({SEARCH_MAX_EPOCHS} epochs / "
          f"patience={SEARCH_PATIENCE}), {len(LR_CANDIDATES)} lr x {len(seeds)} seeds\n{'='*70}")
    rows = []
    for lr in LR_CANDIDATES:
        for seed in seeds:
            print(f"\n--- Stage 1: lr={lr}  seed={seed} ---")
            m, collapsed, dead_types, best_val_mae = run_one(
                lr, seed, SEARCH_MAX_EPOCHS, SEARCH_PATIENCE, "lrretune_stage1")
            print(f"lr={lr} seed={seed}: collapsed={collapsed} dead={dead_types} "
                  f"best_val_mae={best_val_mae:.4f}")
            rows.append({'lr': lr, 'seed': seed, 'collapsed': collapsed,
                         'dead_types': ",".join(dead_types), 'best_val_mae': best_val_mae})
            pd.DataFrame(rows).to_csv(STAGE1_CSV, index=False)

    df = pd.DataFrame(rows)
    summary = df.groupby('lr').agg(
        collapse_rate=('collapsed', 'mean'),
        n_collapsed=('collapsed', 'sum'),
        mean_val_mae_all=('best_val_mae', 'mean'),
    )
    non_collapsed_mean = (df[~df['collapsed']].groupby('lr')['best_val_mae'].mean()
                           .rename('mean_val_mae_stable_only'))
    summary = summary.join(non_collapsed_mean)
    print(f"\n{'='*70}\nSTAGE 1 SUMMARY\n{'='*70}")
    print(summary.to_string())
    return df, summary


def pick_winner(summary):
    stable = summary[summary['collapse_rate'] == 0.0]
    if len(stable) == 0:
        # no fully-stable candidate -- fall back to lowest collapse rate, then best MAE
        winner_lr = summary.sort_values(
            ['collapse_rate', 'mean_val_mae_all']).index[0]
        print(f"\nNo lr had 0% collapse rate in Stage 1. Falling back to lowest "
              f"collapse-rate/best-MAE candidate: lr={winner_lr}")
    else:
        winner_lr = stable.sort_values('mean_val_mae_stable_only').index[0]
        print(f"\nStage 1 winner (0% collapse, best mean val MAE among stable "
              f"candidates): lr={winner_lr}")
    return winner_lr


def stage2(winner_lr, seeds=SEEDS):
    print(f"\n{'='*70}\nSTAGE 2 -- production budget (100 epochs / patience=10), "
          f"lr={winner_lr} x {len(seeds)} seeds\n{'='*70}")
    import baselines as bl
    rows = []
    for seed in seeds:
        print(f"\n--- Stage 2: lr={winner_lr}  seed={seed} ---")
        m, collapsed, dead_types, best_val_mae = run_one(
            winner_lr, seed, 100, 10, "lrretune_stage2")
        hgt_df, _ = bl.hgt_predictions(m)
        last = hgt_df[hgt_df['last_event']]
        m_last = bl.metrics(last['true_h'].values, last['hgt_pred_h'].values)
        meets_gate = (not collapsed) and (m_last['mae'] <= PRODUCTION_MAE_LAST_REF)
        print(f"seed={seed}: collapsed={collapsed} dead={dead_types}  "
              f"MAE_last={m_last['mae']:.3f}h  R2_last={m_last['r2']:.3f}  "
              f"meets_gate={meets_gate}")
        rows.append({
            'lr': winner_lr, 'seed': seed, 'collapsed': collapsed,
            'dead_types': ",".join(dead_types), 'mae_last': m_last['mae'],
            'rmse_last': m_last['rmse'], 'r2_last': m_last['r2'],
            'meets_gate': meets_gate, 'model_path': m.model_path,
        })
        pd.DataFrame(rows).to_csv(STAGE2_CSV, index=False)

    df = pd.DataFrame(rows)
    print(f"\n{'='*70}\nSTAGE 2 SUMMARY (lr={winner_lr})\n{'='*70}")
    print(df.to_string(index=False))
    print(f"\nProduction reference: MAE_last={PRODUCTION_MAE_LAST_REF:.3f}h  "
          f"R2_last={PRODUCTION_R2_LAST_REF:.3f}")
    n_meets_gate = df['meets_gate'].sum()
    print(f"{n_meets_gate}/{len(seeds)} seeds meet the promotion gate "
          f"(non-collapsed AND MAE_last <= {PRODUCTION_MAE_LAST_REF:.3f}h)")
    return df


def main():
    print(f"Backing up current (LoadToVehicle) graph cache to {LOADTOVEHICLE_BACKUP}...")
    for fname in GRAPH_FILES:
        shutil.copy(f"{LIVE_DIR}/{fname}", f"{LOADTOVEHICLE_BACKUP}/{fname}")

    print("Swapping in Depart's graph cache and setting kpi_event='Depart'...")
    _swap_graphs(DEPART_BACKUP)
    _set_kpi_event("Depart")

    try:
        stage1_df, stage1_summary = stage1()
        winner_lr = pick_winner(stage1_summary)
        stage2_df = stage2(winner_lr)

        print(f"\n{'='*70}\nFINAL: winner lr={winner_lr}\n{'='*70}")
        if stage2_df['meets_gate'].any():
            best_row = stage2_df[stage2_df['meets_gate']].sort_values('mae_last').iloc[0]
            print(f"PROMOTION CANDIDATE FOUND: seed={int(best_row['seed'])} "
                  f"lr={best_row['lr']} MAE_last={best_row['mae_last']:.3f}h "
                  f"checkpoint={best_row['model_path']}")
            print("Promotion itself is NOT automatic in this script -- apply the decision "
                  "gate steps from the plan manually, using this checkpoint as the source.")
        else:
            print("No Stage-2 seed met the promotion gate -- production checkpoint stays "
                  "unchanged. See Stage 1/2 CSVs for full results.")

    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event...")
        _swap_graphs(LOADTOVEHICLE_BACKUP)
        _set_kpi_event("LoadToVehicle")
        print("Restored.")


if __name__ == '__main__':
    main()
