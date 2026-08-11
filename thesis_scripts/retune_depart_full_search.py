"""Phases 2-3 of the "find a genuinely stable Depart solution" investigation.

Phase 0 (diagnose_depart_collapse_mechanism_adamw.py) found AdamW's decoupled weight decay
completely prevents the collapse even at Depart's exact deployed lr=0.01 -- both seeds
tested showed flat, stable weight norms (per-batch ratio ~0.997-1.001) across every
previously-doomed node type, vs. Adam's aggressive ~0.93-0.9999 near-constant shrink at the
same config. This script confirms that across more seeds and gets real accuracy numbers.

Stage 1 (cheap multi-seed screen, short budget matching sweep()'s own convention --
30 epochs/patience=4): AdamW x lr in {0.01, 0.005, 0.003, 0.001} x seed in {0,1,2} = 12
runs. hidden_channels=16, num_layers=3, num_heads=2, weight_decay=1e-5 held fixed (Depart's
already-validated architecture). lr=0.01 is INCLUDED here (unlike the earlier Adam-only
retune, which excluded it as already-disproven) -- under AdamW it's a live candidate again,
per Phase 0's finding.

Stage 2 (confirmation, production budget -- 100 epochs/patience=10): the Stage-1 winner,
5 seeds (not 3 -- a stronger reliability claim for what's meant to become the new
production checkpoint if it qualifies). Full last-event MAE/R2 via
baselines.hgt_predictions()/metrics(), same pipeline as every other checkpoint in this
project.

Acceptance bar (confirmed with user): adopt the best STABLE config found, even if its
accuracy doesn't quite match the deployed checkpoint's 7.47h -- reproducibility over a
lucky number. This script reports against the 7.47h reference for context but does not
gate its own "winner" selection on beating it.

Does NOT touch the production checkpoint or model_params.json -- every run saves under its
own suffixed filename. Promotion (if warranted) is a separate, deliberate step after
reviewing this script's output, not automatic.

Usage: python3 retune_depart_full_search.py
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

LR_CANDIDATES = [0.01, 0.005, 0.003, 0.001]
STAGE1_SEEDS = [0, 1, 2]
STAGE2_SEEDS = [0, 1, 2, 3, 4]
PRODUCTION_MAE_LAST_REF = 7.470012
PRODUCTION_R2_LAST_REF = -0.745665

OUT_DIR = "files/models/logistics/1000/Hetero"
STAGE1_CSV = f"{OUT_DIR}/depart_fullsearch_stage1_adamw.csv"
STAGE2_CSV = f"{OUT_DIR}/depart_fullsearch_stage2_confirmation.csv"

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
    m.Het_Reg_Modelling(m.train_data, m.val_data, m.test_data, optimizer='adamw')

    collapsed, dead_types = is_collapsed(m.model)
    log_df = pd.read_csv(m.model_path.replace(".pth", "_training_log.csv"))
    best_val_mae = float(log_df['val_mae'].min())

    return m, collapsed, dead_types, best_val_mae


def stage1():
    print(f"\n{'='*70}\nSTAGE 1 -- AdamW, search budget (30 epochs/patience=4), "
          f"{len(LR_CANDIDATES)} lr x {len(STAGE1_SEEDS)} seeds\n{'='*70}")
    rows = []
    for lr in LR_CANDIDATES:
        for seed in STAGE1_SEEDS:
            print(f"\n--- Stage 1: AdamW lr={lr}  seed={seed} ---")
            m, collapsed, dead_types, best_val_mae = run_one(
                lr, seed, 30, 4, "fullsearch_stage1")
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
    print(f"\n{'='*70}\nSTAGE 1 SUMMARY (AdamW)\n{'='*70}")
    print(summary.to_string())
    return df, summary


def pick_winner(summary):
    stable = summary[summary['collapse_rate'] == 0.0]
    if len(stable) == 0:
        winner_lr = summary.sort_values(['collapse_rate', 'mean_val_mae_all']).index[0]
        print(f"\nNo lr had 0% collapse rate under AdamW in Stage 1 (unexpected given "
              f"Phase 0). Falling back to lowest collapse-rate/best-MAE: lr={winner_lr}")
    else:
        # Among fully-stable candidates, prefer the LARGEST lr with competitive accuracy --
        # larger lr converges faster/better within a fixed epoch budget when stable, and
        # Phase 0 already showed lr=0.01 itself doesn't collapse under AdamW.
        winner_lr = stable.sort_values('mean_val_mae_stable_only').index[0]
        print(f"\nStage 1 winner (0% collapse, best mean val MAE among stable "
              f"candidates): lr={winner_lr}")
    return winner_lr


def stage2(winner_lr):
    print(f"\n{'='*70}\nSTAGE 2 -- AdamW lr={winner_lr}, production budget "
          f"(100 epochs/patience=10), {len(STAGE2_SEEDS)} seeds\n{'='*70}")
    import baselines as bl
    rows = []
    for seed in STAGE2_SEEDS:
        print(f"\n--- Stage 2: AdamW lr={winner_lr}  seed={seed} ---")
        m, collapsed, dead_types, best_val_mae = run_one(
            winner_lr, seed, 100, 10, "fullsearch_stage2")
        hgt_df, _ = bl.hgt_predictions(m)
        last = hgt_df[hgt_df['last_event']]
        m_last = bl.metrics(last['true_h'].values, last['hgt_pred_h'].values)
        beats_reference = (not collapsed) and (m_last['mae'] <= PRODUCTION_MAE_LAST_REF)
        print(f"seed={seed}: collapsed={collapsed} dead={dead_types}  "
              f"MAE_last={m_last['mae']:.3f}h  R2_last={m_last['r2']:.3f}  "
              f"beats_7.47h_reference={beats_reference}")
        rows.append({
            'lr': winner_lr, 'seed': seed, 'collapsed': collapsed,
            'dead_types': ",".join(dead_types), 'mae_last': m_last['mae'],
            'rmse_last': m_last['rmse'], 'r2_last': m_last['r2'],
            'beats_reference': beats_reference, 'model_path': m.model_path,
        })
        pd.DataFrame(rows).to_csv(STAGE2_CSV, index=False)

    df = pd.DataFrame(rows)
    print(f"\n{'='*70}\nSTAGE 2 SUMMARY (AdamW, lr={winner_lr})\n{'='*70}")
    print(df.to_string(index=False))
    print(f"\nProduction reference: MAE_last={PRODUCTION_MAE_LAST_REF:.3f}h  "
          f"R2_last={PRODUCTION_R2_LAST_REF:.3f}")
    n_stable = (~df['collapsed']).sum()
    n_beats_ref = df['beats_reference'].sum()
    print(f"{n_stable}/{len(STAGE2_SEEDS)} seeds stable (non-collapsed)")
    print(f"{n_beats_ref}/{len(STAGE2_SEEDS)} seeds also beat the 7.47h reference")
    if n_stable > 0:
        best_stable = df[~df['collapsed']].sort_values('mae_last').iloc[0]
        print(f"\nBest STABLE candidate (acceptance bar per user decision -- best stable, "
              f"not strict beat-the-reference): seed={int(best_stable['seed'])} "
              f"MAE_last={best_stable['mae_last']:.3f}h R2_last={best_stable['r2_last']:.3f} "
              f"checkpoint={best_stable['model_path']}")
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

        print(f"\n{'='*70}\nFINAL\n{'='*70}")
        if (~stage2_df['collapsed']).any():
            best_row = stage2_df[~stage2_df['collapsed']].sort_values('mae_last').iloc[0]
            print(f"RECOMMENDED CONFIG: AdamW, lr={best_row['lr']}, seed={int(best_row['seed'])}, "
                  f"MAE_last={best_row['mae_last']:.3f}h (vs. 7.47h deployed reference), "
                  f"checkpoint={best_row['model_path']}")
            print("Promotion itself is NOT automatic in this script -- apply the promotion "
                  "steps from the plan manually, using this checkpoint's config as the source.")
        else:
            print("No Stage-2 seed was stable under AdamW either (unexpected). "
                  "See CSVs for full results -- architecture-level changes likely needed.")

    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event...")
        _swap_graphs(LOADTOVEHICLE_BACKUP)
        _set_kpi_event("LoadToVehicle")
        print("Restored.")


if __name__ == '__main__':
    main()
