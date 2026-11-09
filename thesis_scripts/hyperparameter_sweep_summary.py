"""HOEG Fig. 2-style hyperparameter sweep summary, for all 5 trained models
(order_management/PayOrder, order_management/PackageDelivered,
logistics/CustomerOrder->Depart, logistics/CustomerOrder->LoadToVehicle,
logistics/TransportDocument->Depart).

HOEG's own Fig. 2 (Smit et al. 2024) is a grid of small multiples, one panel per
(encoding, dataset), each an MAE-vs-hidden-dimensions line chart with one line per
learning rate, y-axis independently scaled per panel ("scales are not aligned, as we
intend to compare hyperparameter settings within each encoding and dataset"). This
mirrors that layout, sourcing real per-trial data from each dataset's persisted Optuna
sweep study rather than approximating anything.

PayOrder and CustomerOrder->Depart's configs already match files/config.yml as-is, so
they only need a (possibly already-persisted) sweep. PackageDelivered and
TransportDocument->Depart need a different kpi_event/kpi_viewpoint than what's
currently configured, which means: back up the live graph_structures/hetero_structures
cache, edit config.yml, regenerate the graph dataset for that config, sweep, then
revert config.yml AND restore the original graph data from the fresh backup (a fast
copy, not a second regeneration) -- so the two currently-working checkpoints are
byte-for-byte back to where they started regardless of what the new sweeps find.

Usage: python3 hyperparameter_sweep_summary.py
"""
import json
import os
import shutil

import matplotlib.pyplot as plt
import optuna
import pandas as pd

import run_pipeline
import training as t

CONFIG_PATH = "files/config.yml"

# (display label, database, cant, needs a fresh sweep?, config_override or None)
# config_override is (old_line, new_line) -- exact, unique text of the one line in
# config.yml to swap out and back.
#
# Logistics/TransportDocument->Depart is deliberately deferred (needs_sweep left as
# metadata below but the entry is commented out of MODELS) -- its regenerate+sweep
# cycle was interrupted partway through the 36-trial grid (~4 trials completed) because
# each trial was taking far longer than expected (~30h projected total, vs. a few hours
# estimated). config.yml and the logistics graph_structures/hetero_structures caches
# were manually reverted/restored to their original CustomerOrder->Depart state after
# the interrupt (verified: task_id resolves back correctly, train/val/test sizes match
# the pre-run values, model_params.json unaffected). The partial sweep_TimeFrom_
# TransportDocument_to_Depart.db is left in place -- Optuna's persisted storage means
# resuming later continues from trial ~4 rather than restarting the grid.
MODELS = [
    ("Order Management / Orders → PayOrder", "order_management", 2000, False, None),
    ("Order Management / Orders → PackageDelivered", "order_management", 2000, False, None),
    ("Logistics / CustomerOrder → Depart", "logistics", 1000, False, None),
    # Logistics / CustomerOrder->LoadToVehicle: sweep already persisted (2026-07-25/26,
    # 36-trial grid, winner hidden_channels=48/num_layers=4/lr=0.001) -- read directly via
    # its task_id below, same as CustomerOrder->Depart, regardless of config.yml's live
    # kpi_event (currently left pointed at LoadToVehicle, see
    # project_customerorder_loadtovehicle_kpi memory -- doesn't matter here since
    # load_completed_trials() never goes through a live Modelling(database, cant) instance
    # for a needs_sweep=False entry).
    ("Logistics / CustomerOrder → LoadToVehicle", "logistics", 1000, False, None),
    # ("Logistics / TransportDocument → Depart", "logistics", 1000, True, (
    #     '  kpi_viewpoint: "CustomerOrder"\n',
    #     '  kpi_viewpoint: "TransportDocument"\n',
    # )),
]

# Explicit task_id per model, keyed by label -- needed because PackageDelivered's
# task_id can no longer be read off a live Modelling(database, cant) instance now that
# config.yml has been reverted back to PayOrder; db_dir only depends on database/cant
# (not on kpi_event/kpi_viewpoint), so this is enough to locate its persisted study
# without needing config.yml to match.
TASK_IDS = {
    "Order Management / Orders → PayOrder": "TimeFrom_Orders_to_PayOrder",
    "Order Management / Orders → PackageDelivered": "TimeFrom_Orders_to_PackageDelivered",
    "Logistics / CustomerOrder → Depart": "TimeFrom_CustomerOrder_to_Depart",
    "Logistics / CustomerOrder → LoadToVehicle": "TimeFrom_CustomerOrder_to_LoadToVehicle",
    "Logistics / TransportDocument → Depart": "TimeFrom_TransportDocument_to_Depart",
}

OUT_DIR = "thesis_parts/figures_tables"
CSV_PATH = os.path.join(OUT_DIR, "hyperparameter_sweep_summary.csv")
PNG_PATH = os.path.join(OUT_DIR, "hyperparameter_sweep_summary.png")

HIDDEN_ORDER = [8, 16, 24, 32, 48, 64, 128, 256]
LR_COLORS = {0.01: '#e15759', 0.001: '#4e79a7'}


def set_config_line(old_line, new_line):
    with open(CONFIG_PATH) as f:
        text = f.read()
    count = text.count(old_line)
    assert count == 1, f"expected exactly one match for {old_line!r} in {CONFIG_PATH}, found {count}"
    with open(CONFIG_PATH, 'w') as f:
        f.write(text.replace(old_line, new_line))


def backup_graph_data(database, cant, tag):
    graph_src = f"files/graph_structures/{database}/{cant}"
    hetero_src = f"files/hetero_structures/{database}/{cant}"
    graph_bak = f"{graph_src}_{tag}"
    hetero_bak = f"{hetero_src}_{tag}"
    for bak in (graph_bak, hetero_bak):
        if os.path.exists(bak):
            shutil.rmtree(bak)
    print(f"  Backing up {graph_src} -> {graph_bak}")
    shutil.copytree(graph_src, graph_bak)
    print(f"  Backing up {hetero_src} -> {hetero_bak}")
    shutil.copytree(hetero_src, hetero_bak)
    return graph_bak, hetero_bak


def restore_graph_data(database, cant, graph_bak, hetero_bak):
    graph_dst = f"files/graph_structures/{database}/{cant}"
    hetero_dst = f"files/hetero_structures/{database}/{cant}"
    print(f"  Restoring {graph_bak} -> {graph_dst}")
    shutil.rmtree(graph_dst)
    shutil.move(graph_bak, graph_dst)
    print(f"  Restoring {hetero_bak} -> {hetero_dst}")
    shutil.rmtree(hetero_dst)
    shutil.move(hetero_bak, hetero_dst)


def resweep_with_guard(database, cant):
    """Runs sweep() for (database, cant), protecting the already-trained checkpoint's
    model_params.json entry: the fixed seeds/GridSampler make sweep() very likely to
    reproduce the same best params as before, but if it doesn't, silently overwriting
    model_params.json would desync the saved config from the actual trained checkpoint
    weights (the .pth file), which explainer.py relies on matching. Snapshot before,
    restore after if changed -- the fresh Optuna .db (the actual thing this script
    needs) is kept regardless; only the config file is protected."""
    m = t.Modelling(database, cant)
    task_id = m.task_id
    params_path = m._params_path

    snapshot = None
    if os.path.exists(params_path):
        with open(params_path) as f:
            snapshot = json.load(f).get(task_id)

    print(f"\n{'='*60}\nRe-sweeping {database}/{task_id} (cant={cant})...\n{'='*60}")
    m.sweep()

    with open(params_path) as f:
        all_params = json.load(f)
    if snapshot is not None and all_params.get(task_id) != snapshot:
        print(f"  Re-sweep found different best params for {task_id} -- restoring the "
              f"original so the saved config stays consistent with the trained checkpoint.")
        print(f"    kept (original) : {snapshot}")
        print(f"    discarded (new) : {all_params.get(task_id)}")
        all_params[task_id] = snapshot
        with open(params_path, 'w') as f:
            json.dump(all_params, f, indent=2)
    else:
        print(f"  Re-sweep confirmed the same best params for {task_id} -- no restore needed.")

    return m, task_id


def load_completed_trials(database, cant, task_id):
    """Reads a persisted study directly from (database, cant, task_id) -- db_dir only
    depends on database/cant (files/models/{database}/{cant}/Hetero), not on
    config.yml's current kpi_event/kpi_viewpoint, so this works even when config.yml
    no longer matches the task_id being read (e.g. PackageDelivered, after config.yml
    has been reverted back to PayOrder)."""
    db_dir = f"files/models/{database}/{cant}/Hetero"
    storage = f"sqlite:///{db_dir}/sweep_{task_id}.db"
    study = optuna.load_study(study_name=f"{database}_{task_id}", storage=storage)
    return [tr for tr in study.trials if tr.state == optuna.trial.TrialState.COMPLETE]


def trials_to_rows(label, database, task_id, trials):
    rows = []
    for tr in trials:
        rows.append({
            'model': label,
            'database': database,
            'task_id': task_id,
            'hidden_channels': tr.params['hidden_channels'],
            'lr': tr.params['lr'],
            'num_layers': tr.params.get('num_layers'),
            'val_mae_normalized': tr.value,
        })
    return rows


def resweep_with_regeneration(label, database, cant, config_override):
    """PackageDelivered/TransportDocument->Depart path: back up the live graph data,
    swap config.yml to the needed kpi_event/kpi_viewpoint, regenerate, sweep, then
    ALWAYS revert config.yml and restore the original graph data from the backup --
    regardless of whether the regenerate/sweep succeeded -- so a failure here can't
    leave the two currently-working checkpoints in a broken state."""
    old_line, new_line = config_override
    print(f"\n{'#'*70}\n{label}: starting regenerate-and-sweep cycle for {database}/{cant}\n{'#'*70}")

    graph_bak, hetero_bak = backup_graph_data(database, cant, "presweep_snapshot")
    set_config_line(old_line, new_line)
    print(f"  config.yml: {old_line.strip()!r} -> {new_line.strip()!r}")

    rows, error = [], None
    try:
        print(f"\nRegenerating graph data for {label}...")
        run_pipeline.run(database, cant, {'regenerate', 'split', 'graphs'})

        m, task_id = resweep_with_guard(database, cant)
        trials = load_completed_trials(database, cant, task_id)
        print(f"  {len(trials)} completed trials loaded for {task_id}")
        rows = trials_to_rows(label, database, task_id, trials)
    except Exception as e:
        error = e
    finally:
        print(f"\nReverting config.yml and restoring original graph data for {database}/{cant}...")
        set_config_line(new_line, old_line)
        restore_graph_data(database, cant, graph_bak, hetero_bak)

    m_check = t.Modelling(database, cant)
    print(f"  Sanity check after restore: task_id={m_check.task_id}, "
          f"train_data={len(m_check.train_data)} graphs")

    if error is not None:
        raise error
    return rows


def render_grid(df):
    labels = list(dict.fromkeys(df['model']))
    n = len(labels)
    ncols = 2
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(6.5 * ncols, 5 * nrows))
    axes = axes.flatten() if n > 1 else [axes]

    for ax, label in zip(axes, labels):
        sub = df[df['model'] == label]
        subtitle = label
        if sub['num_layers'].notna().any():
            # Extra dimension HOEG's own grid doesn't have (order_management fixes
            # num_layers=2; logistics tunes it) -- slice at the best-found value so
            # every panel stays a clean 2D grid, matching HOEG's own layout, and say
            # so explicitly rather than silently collapsing the dimension.
            best_layers = sub.loc[sub['val_mae_normalized'].idxmin(), 'num_layers']
            sub = sub[sub['num_layers'] == best_layers]
            subtitle = f"{label}\n(num_layers={int(best_layers)}, best found)"

        for lr, color in LR_COLORS.items():
            lr_sub = sub[sub['lr'] == lr].sort_values('hidden_channels')
            if lr_sub.empty:
                continue
            ax.plot(lr_sub['hidden_channels'], lr_sub['val_mae_normalized'],
                    marker='o', color=color, label=f"lr={lr}")

        ax.set_xscale('log', base=2)
        ax.set_xticks(sorted(sub['hidden_channels'].unique()))
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
        ax.set_xlabel("Hidden Dimensions")
        ax.set_ylabel("Val MAE (normalized)")
        ax.set_title(subtitle, fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    for ax in axes[n:]:
        ax.axis('off')

    fig.suptitle("Hyperparameter sweep summary (cf. HOEG Fig. 2)\n"
                 "Note: y-axis scales are independent per panel, matching HOEG's own convention "
                 "-- intended for comparing settings within each model, not across models.",
                 fontsize=11)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    all_rows = []

    for label, database, cant, needs_sweep, config_override in MODELS:
        if config_override is not None:
            all_rows.extend(resweep_with_regeneration(label, database, cant, config_override))
            continue

        if needs_sweep:
            m, task_id = resweep_with_guard(database, cant)
        else:
            task_id = TASK_IDS[label]
            print(f"\n{label}: using existing persisted sweep data for {task_id}, no re-sweep needed.")

        trials = load_completed_trials(database, cant, task_id)
        print(f"  {len(trials)} completed trials loaded for {task_id}")
        all_rows.extend(trials_to_rows(label, database, task_id, trials))

    df = pd.DataFrame(all_rows)
    df.to_csv(CSV_PATH, index=False)
    print(f"\nSaved combined per-trial CSV to {CSV_PATH} ({len(df)} rows)")

    render_grid(df)
    print(f"Saved sweep summary figure to {PNG_PATH}")


if __name__ == '__main__':
    main()
