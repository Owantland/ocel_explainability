"""Tests de Leoni's comment #17 on `thesis_parts/results.txt` (Fig. 5.5, logistics/
CustomerOrder->Depart's training curves): "I don't see MAE's stabilization" -- is the
current early-stop-at-epoch-11 pattern genuine overfitting, or is the model just
undertrained under too short a patience budget?

Production run (`training.py`'s `Modelling.__init__`, logistics branch: max_epochs=100,
early_stop_patience=10) stopped at epoch 11: val MAE was already best at epoch 1 (0.6434)
and never improved again, while train loss kept dropping monotonically the whole time.
With only 10 epochs of patience, "never recovers within 10 epochs" and "never recovers,
period" are hard to tell apart -- exactly the ambiguity de Leoni's comment raises.

This script reruns HGT + HomoGNN on Depart with a 3x longer patience (30, up from 10) and
a modest epoch-cap increase (150, up from 100) -- nothing else changes (same
hyperparameters, same seed, same selection_metric='pooled') -- so if val MAE still never
beats its epoch-1 value across a full 30-epoch window, that's direct, strong evidence of
genuine overfitting rather than premature early stopping.

Does NOT touch the production TimeFrom_CustomerOrder_to_Depart(.pth/_homo.pth/
_training_log.csv/_norm.json) checkpoint or model_params.json -- saves everything under a
separate `_longpatience_experiment` suffix, matching the project's existing
`_hiddensweep_experiment`/`_layers3_experiment` convention (see
experiment_logistics_transportdocument_hidden_sweep.py).

Depart and LoadToVehicle share one non-KPI-suffixed hetero graph cache path
(files/hetero_structures/logistics/1000/*.pt) -- this script does the same backup/swap/
restore dance already established this session: back up the current (LoadToVehicle)
cache, swap in Depart's from `1000_customerorder_depart_backup/`, flip
`files/config.yml`'s `kpi_event` to "Depart", run, then restore both afterward
regardless of outcome (including on a crash, via try/finally).

Usage: python3 experiment_logistics_depart_longpatience.py
"""
import shutil
import time

import baselines as bl
import training as t

CONFIG_PATH = "files/config.yml"
GRAPH_DIR = "files/hetero_structures/logistics"
DEPART_BACKUP = f"{GRAPH_DIR}/1000_customerorder_depart_backup"
LOADTOVEHICLE_BACKUP = f"{GRAPH_DIR}/1000_loadtovehicle_backup"
LIVE_DIR = f"{GRAPH_DIR}/1000"
GRAPH_FILES = ["train_graphs_sg.pt", "val_graphs_sg.pt", "test_graphs_sg.pt"]

# Exact-line text substitution (not a YAML parse/dump round-trip) -- config.yml has
# inline comments (`# Depart`) that a yaml.safe_load/safe_dump round-trip would
# silently strip from the whole file, not just this one line.
LOGISTICS_LOADTOVEHICLE_LINE = '  kpi_event: "LoadToVehicle" # Depart'
LOGISTICS_DEPART_LINE = '  kpi_event: "Depart" # LoadToVehicle'

NEW_MAX_EPOCHS = 150
NEW_PATIENCE = 30

# Production reference numbers (compare_to_baselines(), this session, 2026-08-01) --
# printed alongside the new checkpoint's numbers at the end for a direct comparison.
PRODUCTION_MAE_LAST = 7.470012
PRODUCTION_R2_LAST = -0.745665


def _swap_graphs(backup_dir):
    for fname in GRAPH_FILES:
        shutil.copy(f"{backup_dir}/{fname}", f"{LIVE_DIR}/{fname}")


def _set_kpi_event(target):
    """target: 'Depart' or 'LoadToVehicle'. Swaps the exact known line in-place,
    leaving every other line (comments, order_management's own kpi_event, etc.)
    byte-for-byte untouched."""
    old_line, new_line = (
        (LOGISTICS_LOADTOVEHICLE_LINE, LOGISTICS_DEPART_LINE) if target == "Depart"
        else (LOGISTICS_DEPART_LINE, LOGISTICS_LOADTOVEHICLE_LINE)
    )
    with open(CONFIG_PATH) as f:
        text = f.read()
    if text.count(old_line) != 1:
        raise RuntimeError(
            f"Expected exactly one occurrence of {old_line!r} in {CONFIG_PATH}, "
            f"found {text.count(old_line)} -- refusing to edit blindly (config.yml "
            f"may have already been changed since this script was written).")
    text = text.replace(old_line, new_line)
    with open(CONFIG_PATH, "w") as f:
        f.write(text)


def main():
    print(f"Backing up current (LoadToVehicle) graph cache to {LOADTOVEHICLE_BACKUP} "
          f"as a safety copy before swapping in Depart's...")
    for fname in GRAPH_FILES:
        shutil.copy(f"{LIVE_DIR}/{fname}", f"{LOADTOVEHICLE_BACKUP}/{fname}")

    print("Swapping in Depart's graph cache and setting kpi_event='Depart'...")
    _swap_graphs(DEPART_BACKUP)
    _set_kpi_event("Depart")

    try:
        m = t.Modelling('logistics', 1000)
        print(f"Task ID: {m.task_id}")
        print(f"Params (unchanged from production): {m.params}")

        m.max_epochs = NEW_MAX_EPOCHS
        m.early_stop_patience = NEW_PATIENCE
        m.model_path = m.model_path.replace(".pth", "_longpatience_experiment.pth")
        print(f"Overridden: max_epochs={m.max_epochs}, "
              f"early_stop_patience={m.early_stop_patience}")
        print(f"Experiment checkpoint path: {m.model_path}")

        print("\n" + "=" * 70)
        print(f"HGT -- long-patience retrain (max_epochs={NEW_MAX_EPOCHS}, "
              f"patience={NEW_PATIENCE})")
        print("=" * 70)
        t0 = time.time()
        m.Het_Reg_Modelling(m.train_data, m.val_data, m.test_data)
        print(f"HGT wall-clock: {time.time() - t0:.1f}s")

        print("\n" + "=" * 70)
        print("HomoGNN -- same extended budget (cheap, kept comparable for Fig. 5.5)")
        print("=" * 70)
        t0 = time.time()
        m.Homo_Reg_Modelling()
        print(f"HomoGNN wall-clock: {time.time() - t0:.1f}s")

        print("\n" + "=" * 70)
        print("EVALUATION -- new checkpoint's last-event test MAE/R2 vs. production")
        print("=" * 70)
        hgt_df, _ = bl.hgt_predictions(m)
        last = hgt_df[hgt_df['last_event']]
        m_last = bl.metrics(last['true_h'].values, last['hgt_pred_h'].values)
        print(f"Long-patience checkpoint: MAE_last={m_last['mae']:.3f}h  "
              f"RMSE_last={m_last['rmse']:.3f}h  R2_last={m_last['r2']:.3f}")
        print(f"Production reference:    MAE_last={PRODUCTION_MAE_LAST:.3f}h  "
              f"R2_last={PRODUCTION_R2_LAST:.3f}")
        hgt_df.to_csv(
            m.model_path.replace(".pth", "_test_predictions.csv"), index=False)

    finally:
        print("\nRestoring LoadToVehicle graph cache and kpi_event...")
        _swap_graphs(LOADTOVEHICLE_BACKUP)
        _set_kpi_event("LoadToVehicle")
        print("Restored.")


if __name__ == '__main__':
    main()
