"""HOEG Fig. 3 (b/d/f)-style training/validation learning-curve plots, HGT vs. HomoGNN.

HOEG's own Fig. 3 has six panels: three violin plots of MAE distribution across
hyperparameter settings (a/c/e, not reproduced here), and three "Learning Curves of
Best Performing Models" line plots (b/d/f, one per dataset) -- Epochs on the x-axis,
Mean Absolute Error on the y-axis, 4 lines: {encoding} Train Loss / {encoding}
Validation Loss for each of their two encodings (EFG, HOEG). This reproduces the
line-plot half only, for this project's own two datasets and its own two models
(HGT vs. HomoGNN).

Pure read-and-plot over already-existing data -- no training or sweeping here.
Het_Reg_Modelling()/Homo_Reg_Modelling() (training.py) already log exactly this data
(epoch, train_loss, val_mae) to a *_training_log.csv every time they run, as part of
normal training; this script just reads the files already on disk for the current
citable checkpoints.

Usage: python3 training_curves_summary.py
"""
import os

import matplotlib.pyplot as plt
import pandas as pd

# (dataset display label, database, cant, task_id)
DATASETS = [
    ("Order Management", "order_management", 2000, "TimeFrom_Orders_to_PayOrder"),
    ("Logistics", "logistics", 1000, "TimeFrom_CustomerOrder_to_Depart"),
]

OUT_DIR = "thesis_parts/figures_tables"
CSV_PATH = os.path.join(OUT_DIR, "training_curves_summary.csv")

MODEL_COLORS = {"HGT (ours)": "#4e79a7", "HomoGNN (GCN)": "#e15759"}


def log_path(database, cant, task_id, homo=False):
    suffix = "_homo_training_log.csv" if homo else "_training_log.csv"
    return f"files/models/{database}/{cant}/Hetero/{task_id}{suffix}"


def load_model_log(label, model_name, path):
    df = pd.read_csv(path)
    df['model'] = model_name
    df['dataset'] = label
    return df


def render_dataset(label, df, png_path):
    fig, ax = plt.subplots(figsize=(8, 5.5))

    for model_name, color in MODEL_COLORS.items():
        sub = df[df['model'] == model_name].sort_values('epoch')
        ax.plot(sub['epoch'], sub['train_loss'], color=color, linestyle='-',
                 marker='o', markersize=3, label=f"{model_name} Train Loss")
        ax.plot(sub['epoch'], sub['val_mae'], color=color, linestyle='--',
                 marker='s', markersize=3, label=f"{model_name} Validation Loss")

    ax.set_xlabel("Epoch")
    ax.set_ylabel("Mean Absolute Error (normalized)")
    ax.set_title(f"Learning Curves of Best Performing Models on {label}\n"
                 f"(cf. HOEG Fig. 3(b)/(d)/(f); best/final training run of each "
                 f"model's current citable checkpoint)", fontsize=11)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(png_path, dpi=150, bbox_inches='tight')
    plt.close()


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    all_frames = []

    for label, database, cant, task_id in DATASETS:
        hgt_df = load_model_log(label, "HGT (ours)", log_path(database, cant, task_id, homo=False))
        homo_df = load_model_log(label, "HomoGNN (GCN)", log_path(database, cant, task_id, homo=True))
        print(f"{label}: HGT {len(hgt_df)} epochs, HomoGNN {len(homo_df)} epochs")

        df = pd.concat([hgt_df, homo_df], ignore_index=True)
        all_frames.append(df)

        png_path = os.path.join(OUT_DIR, f"training_curves_{database}.png")
        render_dataset(label, df, png_path)
        print(f"  Saved {png_path}")

    combined = pd.concat(all_frames, ignore_index=True)
    combined.to_csv(CSV_PATH, index=False)
    print(f"\nSaved combined CSV to {CSV_PATH} ({len(combined)} rows)")


if __name__ == '__main__':
    main()
