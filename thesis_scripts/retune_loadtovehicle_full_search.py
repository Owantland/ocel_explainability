"""AdamW retune for logistics/CustomerOrder_to_LoadToVehicle, mirroring
retune_depart_full_search.py's Depart investigation.

Trigger: a direct state-dict check of the deployed checkpoint (hidden_channels=48,
num_layers=4, num_heads=2, lr=0.001, weight_decay=1e-5, optimizer=adam -- the default)
found the same dead-lin_dict-projection signature Depart had before its AdamW fix:
Forklift exactly dead (weight norm 0.0), HandlingUnit effectively dead (abs-mean 1.4e-5).
Unlike Depart, this wasn't previously flagged for this checkpoint -- found while answering
"could AdamW help the other three production models" (PayOrder/PackageDelivered run
weight_decay=0.0, so AdamW is a no-op for them; LoadToVehicle is the one with a live,
fixable instance of the problem).

Unlike the Depart investigation, NO graph-cache/kpi_event swap-dance is needed here:
config.yml's kpi_event is already "LoadToVehicle" and the live graph cache already matches
files/hetero_structures/logistics/1000_loadtovehicle_backup/ (verified before writing this
script) -- LoadToVehicle IS the resting/default state this repo is currently left in. This
script asserts that expectation up front and refuses to run if it's not true, rather than
silently swapping anything.

Stage 1 (cheap multi-seed screen, 30 epochs/patience=4): AdamW x lr in
{0.01, 0.005, 0.003, 0.001} x seed in {0,1,2} = 12 runs. hidden_channels=48, num_layers=4,
num_heads=2, weight_decay=1e-5 held fixed (LoadToVehicle's own already-validated
architecture, unchanged -- same principle as the Depart script: this investigates the
optimizer/lr, not the architecture).

Stage 2 (confirmation, production budget -- 100 epochs/patience=10): the Stage-1 winner,
5 seeds. Full last-event MAE/R2 via baselines.hgt_predictions()/metrics().

Acceptance bar: unlike Depart (which had one known-good reference to match), LoadToVehicle's
deployed checkpoint is ALREADY partially broken (2 dead types), so the bar here is simpler --
prefer the lr with the most seeds showing ZERO dead watched node types (not just "<2", the
threshold Depart used when a couple of already-marginal types were tolyerated); among
zero-dead candidates, prefer best mean val MAE. Reports against the deployed checkpoint's own
MAE_last/R2_last (computed separately, see reference computation run alongside this) for
context, but does not gate on beating it -- resolving the dead types is the primary goal.

Does NOT touch the production checkpoint or model_params.json -- every run saves under its
own suffixed filename. Promotion (if warranted) is a separate, deliberate step, same as Depart.

Usage: python3 retune_loadtovehicle_full_search.py
"""
import pandas as pd

import training as t

CONFIG_PATH = "files/config.yml"
EXPECTED_KPI_LINE = '  kpi_event: "LoadToVehicle" # Depart'

LR_CANDIDATES = [0.01, 0.005, 0.003, 0.001]
STAGE1_SEEDS = [0, 1, 2]
STAGE2_SEEDS = [0, 1, 2, 3, 4]

OUT_DIR = "files/models/logistics/1000/Hetero"
STAGE1_CSV = f"{OUT_DIR}/loadtovehicle_fullsearch_stage1_adamw.csv"
STAGE2_CSV = f"{OUT_DIR}/loadtovehicle_fullsearch_stage2_confirmation.csv"

# All non-viewpoint, non-Events node types -- CustomerOrder (viewpoint) and Events are
# always-present and never seen dead in any investigation to date, same convention as
# retune_depart_full_search.py's WATCHED_NODE_TYPES.
WATCHED_NODE_TYPES = ['Container', 'Forklift', 'HandlingUnit', 'TransportDocument', 'Truck', 'Vehicle']
DEAD_THRESHOLD = 1e-6


def _assert_live_state():
    with open(CONFIG_PATH) as f:
        text = f.read()
    if text.count(EXPECTED_KPI_LINE) != 1:
        raise RuntimeError(
            f"Expected config.yml's live kpi_event to already be {EXPECTED_KPI_LINE!r} "
            f"(LoadToVehicle is this repo's resting state) -- found {text.count(EXPECTED_KPI_LINE)} "
            f"occurrences. Refusing to run without the graph-cache-swap-dance this script "
            f"deliberately doesn't implement -- check config.yml/live graph cache state manually.")


def is_collapsed(model):
    dead = [nt for nt in WATCHED_NODE_TYPES
            if model.lin_dict[nt].weight.norm().item() < DEAD_THRESHOLD]
    return len(dead) > 0, dead


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
        print(f"\nNo lr had 0% collapse rate (zero dead watched types) under AdamW in Stage 1. "
              f"Falling back to lowest collapse-rate/best-MAE: lr={winner_lr}")
    else:
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
        print(f"seed={seed}: collapsed={collapsed} dead={dead_types}  "
              f"MAE_last={m_last['mae']:.3f}h  R2_last={m_last['r2']:.3f}")
        rows.append({
            'lr': winner_lr, 'seed': seed, 'collapsed': collapsed,
            'dead_types': ",".join(dead_types), 'mae_last': m_last['mae'],
            'rmse_last': m_last['rmse'], 'r2_last': m_last['r2'],
            'model_path': m.model_path,
        })
        pd.DataFrame(rows).to_csv(STAGE2_CSV, index=False)

    df = pd.DataFrame(rows)
    print(f"\n{'='*70}\nSTAGE 2 SUMMARY (AdamW, lr={winner_lr})\n{'='*70}")
    print(df.to_string(index=False))
    n_stable = (~df['collapsed']).sum()
    print(f"{n_stable}/{len(STAGE2_SEEDS)} seeds with zero dead watched node types")
    if n_stable > 0:
        best_stable = df[~df['collapsed']].sort_values('mae_last').iloc[0]
        print(f"\nBest fully-resolved candidate: seed={int(best_stable['seed'])} "
              f"MAE_last={best_stable['mae_last']:.3f}h R2_last={best_stable['r2_last']:.3f} "
              f"checkpoint={best_stable['model_path']}")
    return df


def main():
    print("Verifying live state is already LoadToVehicle (no swap needed)...")
    _assert_live_state()
    print("Confirmed. Proceeding without any graph-cache/kpi_event changes.")

    stage1_df, stage1_summary = stage1()
    winner_lr = pick_winner(stage1_summary)
    stage2_df = stage2(winner_lr)

    print(f"\n{'='*70}\nFINAL\n{'='*70}")
    if (~stage2_df['collapsed']).any():
        best_row = stage2_df[~stage2_df['collapsed']].sort_values('mae_last').iloc[0]
        print(f"RECOMMENDED CONFIG: AdamW, lr={best_row['lr']}, seed={int(best_row['seed'])}, "
              f"MAE_last={best_row['mae_last']:.3f}h, checkpoint={best_row['model_path']}")
        print("Promotion itself is NOT automatic in this script -- apply the promotion "
              "steps manually, using this checkpoint's config as the source.")
    else:
        print("No Stage-2 seed fully resolved all watched node types under AdamW either "
              "(unexpected). See CSVs for full results -- architecture-level changes or a "
              "different lr grid likely needed.")


if __name__ == '__main__':
    main()
