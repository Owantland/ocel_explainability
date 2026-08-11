"""Logistics num_layers ablation -- the figure behind thesis_structure.txt's claim
that "num_layers was instead tuned over {3, 4, 5}, with 3 found decisively best."

Reads the already-persisted Optuna sweep study for logistics/CustomerOrder->Depart
(files/models/logistics/1000/Hetero/sweep_TimeFrom_CustomerOrder_to_Depart.db) --
no re-sweeping, same read pattern as hyperparameter_sweep_summary.py's
load_completed_trials().

Usage: python3 num_layers_ablation_logistics.py
"""
import os

import matplotlib.pyplot as plt
import optuna
import pandas as pd

OUT_DIR = "thesis_parts/figures_tables"
DB_PATH = "files/models/logistics/1000/Hetero/sweep_TimeFrom_CustomerOrder_to_Depart.db"
STUDY_NAME = "logistics_TimeFrom_CustomerOrder_to_Depart"

NUM_LAYERS_ORDER = [3, 4, 5]
NUM_LAYERS_COLORS = {3: "#4e79a7", 4: "#f28e2b", 5: "#e15759"}


def load_trials():
    study = optuna.load_study(study_name=STUDY_NAME, storage=f"sqlite:///{DB_PATH}")
    rows = []
    for tr in study.trials:
        if tr.state != optuna.trial.TrialState.COMPLETE:
            continue
        rows.append({
            'trial_id': tr.number,
            'num_layers': tr.params.get('num_layers'),
            'hidden_channels': tr.params.get('hidden_channels'),
            'lr': tr.params.get('lr'),
            'val_mae_normalized': tr.value,
        })
    return pd.DataFrame(rows)


def render(df):
    fig, ax = plt.subplots(figsize=(7, 5.5))

    box_data = [df[df['num_layers'] == n]['val_mae_normalized'].values for n in NUM_LAYERS_ORDER]
    bp = ax.boxplot(box_data, positions=range(len(NUM_LAYERS_ORDER)), widths=0.5,
                     patch_artist=True, showmeans=True)
    for patch, n in zip(bp['boxes'], NUM_LAYERS_ORDER):
        patch.set_facecolor(NUM_LAYERS_COLORS[n])
        patch.set_alpha(0.5)

    for i, n in enumerate(NUM_LAYERS_ORDER):
        ys = df[df['num_layers'] == n]['val_mae_normalized'].values
        xs = [i] * len(ys)
        ax.scatter(xs, ys, color=NUM_LAYERS_COLORS[n], edgecolor='black',
                   linewidth=0.5, s=40, zorder=3)

    ax.set_xticks(range(len(NUM_LAYERS_ORDER)))
    ax.set_xticklabels([f"num_layers={n}\n(n={len(d)} trials)"
                         for n, d in zip(NUM_LAYERS_ORDER, box_data)])
    ax.set_ylabel("Validation MAE (normalized)")
    ax.set_title("Logistics — num_layers ablation\n"
                  "CustomerOrder→Depart hyperparameter sweep (all hidden_channels/lr trials)",
                  fontsize=11)
    ax.grid(True, axis='y', alpha=0.3)

    plt.tight_layout()
    png_path = os.path.join(OUT_DIR, "depart_ablation.png")
    plt.savefig(png_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {png_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = load_trials()
    csv_path = os.path.join(OUT_DIR, "depart_ablation.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path} ({len(df)} rows)")

    print(df.groupby('num_layers')['val_mae_normalized'].agg(['count', 'mean', 'min']))

    render(df)


if __name__ == '__main__':
    main()
