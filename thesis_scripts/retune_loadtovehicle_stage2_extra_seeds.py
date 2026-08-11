"""Extends retune_loadtovehicle_full_search.py's Stage 2 with 5 more seeds (5-9) at the
Stage 1 winner (AdamW, lr=0.003, production budget), to see whether seed=2's MAE_last=1.76h
is a genuine best-case or an outlier given the wide spread across the original 5 seeds
(1.76h-48.86h) -- user asked to see more seeds before deciding which checkpoint to promote.

Reuses retune_loadtovehicle_full_search.py's run_one()/is_collapsed() unchanged (same
architecture, same lr=0.003 winner, same production budget) and appends to the existing
Stage 2 CSV rather than overwriting it, so all 10 seeds end up in one place.

Does NOT touch the production checkpoint or model_params.json, same as the parent script.

Usage: python3 retune_loadtovehicle_stage2_extra_seeds.py
"""
import pandas as pd

import retune_loadtovehicle_full_search as base
import baselines as bl

WINNER_LR = 0.003
EXTRA_SEEDS = [5, 6, 7, 8, 9]


def main():
    print("Verifying live state is already LoadToVehicle (no swap needed)...")
    base._assert_live_state()
    print("Confirmed.")

    existing = pd.read_csv(base.STAGE2_CSV)
    rows = existing.to_dict('records')

    for seed in EXTRA_SEEDS:
        print(f"\n--- Stage 2 extension: AdamW lr={WINNER_LR}  seed={seed} ---")
        m, collapsed, dead_types, best_val_mae = base.run_one(
            WINNER_LR, seed, 100, 10, "fullsearch_stage2")
        hgt_df, _ = bl.hgt_predictions(m)
        last = hgt_df[hgt_df['last_event']]
        m_last = bl.metrics(last['true_h'].values, last['hgt_pred_h'].values)
        print(f"seed={seed}: collapsed={collapsed} dead={dead_types}  "
              f"MAE_last={m_last['mae']:.3f}h  R2_last={m_last['r2']:.3f}")
        rows.append({
            'lr': WINNER_LR, 'seed': seed, 'collapsed': collapsed,
            'dead_types': ",".join(dead_types), 'mae_last': m_last['mae'],
            'rmse_last': m_last['rmse'], 'r2_last': m_last['r2'],
            'model_path': m.model_path,
        })
        pd.DataFrame(rows).to_csv(base.STAGE2_CSV, index=False)

    df = pd.DataFrame(rows)
    print(f"\n{'='*70}\nSTAGE 2 SUMMARY -- ALL {len(df)} SEEDS (AdamW, lr={WINNER_LR})\n{'='*70}")
    print(df.to_string(index=False))
    n_stable = (~df['collapsed']).sum()
    print(f"\n{n_stable}/{len(df)} seeds with zero dead watched node types")
    stable_mae = df[~df['collapsed']]['mae_last']
    print(f"MAE_last across all stable seeds: mean={stable_mae.mean():.3f}h  "
          f"median={stable_mae.median():.3f}h  std={stable_mae.std():.3f}h  "
          f"min={stable_mae.min():.3f}h  max={stable_mae.max():.3f}h")
    best = df[~df['collapsed']].sort_values('mae_last').iloc[0]
    print(f"\nBest overall: seed={int(best['seed'])} MAE_last={best['mae_last']:.3f}h "
          f"R2_last={best['r2_last']:.3f} checkpoint={best['model_path']}")

    print(f"\n{'='*70}\nFINAL\n{'='*70}")
    print("Extension complete. Promotion decision is still manual -- review the full "
          "10-seed spread above/in the CSV before choosing which checkpoint to promote.")


if __name__ == '__main__':
    main()
