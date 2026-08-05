"""
Unified pipeline runner — consolidates pipeline_2000.py, pipeline_1000_logistics.py,
pipeline_1000_logistics_customerorder.py, and validate.py into one parameterized tool.
Each existing script's behavior is reproducible as one specific --stages combination:

    pipeline_2000.py                       ~= --database order_management --cant 2000 \
                                                --stages train,validate,compare,explain
    pipeline_1000_logistics.py             ~= --database logistics --cant 1000 \
                                                --stages validate,compare,explain
    pipeline_1000_logistics_customerorder  ~= --database logistics --cant 1000 \
                                                --stages regenerate,split,graphs,sweep,train,homo,validate,compare,explain
    validate.py                            ~= --database order_management --cant 1000 \
                                                --stages validate,explain

Stages (always run in the fixed order below, regardless of the order given on the CLI):
    regenerate  - ProcessGeneration: rebuild ev_log.csv/all_kpis.csv from the raw OCEL DB, AND
                  ocel_generator.Generator: rebuild ocel.csv/edges.csv from the same nodes object
                  -- hetero_graphs.py needs all three files in sync (previously 'regenerate' only
                  refreshed ev_log.csv, silently leaving ocel.csv/edges.csv stale and causing a
                  'malformed node or string' crash in hetero_graphs.py's ast.literal_eval; see
                  OPEN_ISSUES_FEASIBILITY.md item 6a).
                  WARNING: for logistics this overwrites the graph_structures/hetero_structures
                  cache in place (not viewpoint-qualified in its filename) -- back up first if
                  switching kpi_viewpoint, as done historically before the CustomerOrder switch.
    split       - TrainTestBuilder: rebuild the temporal train/val/test split.
    graphs      - HeteroGraphsGenerator: rebuild the prefix-graph .pt cache. Requires a split;
                  reuses one already computed earlier in the same run, or builds one fresh if
                  'graphs' is requested standalone.
    sweep       - Modelling.sweep(): Optuna hyperparameter search.
    train       - Modelling.Modelling(): train the HGT regression model. Always reloads
                  Modelling fresh (not whatever instance 'sweep' left behind) so it reads
                  model_params.json's just-swept best params, matching every original script's
                  own defensive reload-after-sweep convention.
    homo        - Modelling.Homo_Reg_Modelling(): train the HomoGNN baseline.
    kdim        - Modelling.KDim_Reg_Modelling(): train HOEG's own k-dim GNN baseline
                  (Morris et al. 2019, to_hetero()-wrapped). Reuses HGT's already-tuned
                  hyperparameters, no separate sweep -- see KDim_Reg_Modelling's docstring.
    validate    - test-set metrics (ALL-prefix + last-event MAE/RMSE/R2), MAE-by-depth,
                  top-5 best/worst predictions, residual plot -> validation_{cant}/residuals.png.
    compare     - Explainer.compare_to_baselines(): HGT vs HomoGNN vs Mean vs GBT. Trains HomoGNN
                  first if its checkpoint is missing or stale for the current hyperparameters.
    explain     - feature attribution (InputXGradient), aggregate LOO (n=50), and two single-trace
                  explanations (one fast-outcome, one slow-outcome order/case).

Usage:
    python3 run_pipeline.py --database order_management --cant 2000 --stages train,validate,compare,explain
    python3 run_pipeline.py --database logistics --cant 1000 --stages validate,compare,explain

Also importable: `import run_pipeline; run_pipeline.run('order_management', 2000, {'validate','explain'})`.
"""
import argparse
import os

import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import process_generation as pg
import ocel_generator as og
import train_test_builder as tb
import hetero_graphs as hg
import training as t
import explainer as exp

STAGE_ORDER = ['regenerate', 'split', 'graphs', 'sweep', 'train', 'homo', 'kdim',
               'validate', 'compare', 'explain']


def _print_metrics(subset, label):
    mae_ = subset['abs_err_h'].mean()
    rmse_ = np.sqrt((subset['abs_err_h'] ** 2).mean())
    ss_res = ((subset['true_h'] - subset['pred_h']) ** 2).sum()
    ss_tot = ((subset['true_h'] - subset['true_h'].mean()) ** 2).sum()
    r2_ = 1 - ss_res / ss_tot
    print(f"\n{'='*60}")
    print(f"TEST METRICS — {label}  (n={len(subset)})")
    print(f"{'='*60}")
    print(f"  MAE  : {mae_:.1f} h")
    print(f"  RMSE : {rmse_:.1f} h")
    print(f"  R²   : {r2_:.3f}")
    print(f"  Mean true remaining : {subset['true_h'].mean():.1f} h  "
          f"(std={subset['true_h'].std():.1f} h -- spread of the TRUE remaining-time "
          f"values in this subset, not a model-error metric)")
    return mae_, rmse_, r2_


def _predict_all(model_holder):
    """Run the (already state_dict-loaded, eval-mode) model on every test graph.
    model_holder is anything with .model/.test_data/.kpi_viewpoint/.target_std/.target_mean
    (i.e. a Modelling or Explainer instance -- Explainer IS a Modelling, so this works for
    either without constructing a second, redundant model instance)."""
    vp = model_holder.kpi_viewpoint
    records = []
    with torch.no_grad():
        for g in model_holder.test_data:
            pred_norm = model_holder.model(g.x_dict, g.edge_index_dict)[0].item()
            pred_h = (pred_norm * model_holder.target_std.item()
                      + model_holder.target_mean.item()) / 3600.0
            true_h = (g[vp].y[0].item() * model_holder.target_std.item()
                      + model_holder.target_mean.item()) / 3600.0
            records.append({
                'order_id': int(g[vp].id[0].item()),
                'n_events': g['Events'].num_nodes,
                'true_h': true_h,
                'pred_h': pred_h,
                'abs_err_h': abs(true_h - pred_h),
                'last_event': g[vp].last_event[0].item(),
            })
    return pd.DataFrame(records)


def run(database, cant, stages, selection_metric='pooled'):
    """Run the requested stages (a set/iterable of stage names) for (database, cant),
    always in STAGE_ORDER regardless of the input order. Unknown stage names are ignored
    here (main() validates and errors on them for CLI use).

    selection_metric: 'pooled' (default, matches every checkpoint trained to date) or
    'last_event' -- passed through to sweep()/Modelling() for the 'sweep'/'train' stages only.
    See training.py's Het_Reg_Modelling docstring and OPEN_ISSUES_FEASIBILITY.md item 11."""
    stages = set(stages)
    ordered = [s for s in STAGE_ORDER if s in stages]
    print(f"Running stages {ordered} for database={database}, cant={cant}")

    if 'regenerate' in stages:
        print("\n" + "=" * 60)
        print("REGENERATE — ev_log/all_kpis")
        print("=" * 60)
        p = pg.ProcessGeneration(database, cant)
        nodes = p.related_nodes()
        p.get_ev_log(nodes)
        og.Generator(database, cant).generate_ocel(nodes)

    train_ts = val_ts = test_ts = None
    if 'split' in stages:
        print("\n" + "=" * 60)
        print("SPLIT — train/val/test")
        print("=" * 60)
        ttb = tb.TrainTestBuilder(database, cant)
        train_ts, val_ts, test_ts = ttb.timestamps_generator()

    if 'graphs' in stages:
        print("\n" + "=" * 60)
        print("GRAPHS — hetero graph cache")
        print("=" * 60)
        if train_ts is None:
            # 'graphs' requested without 'split' in the same run -- build the split now.
            ttb = tb.TrainTestBuilder(database, cant)
            train_ts, val_ts, test_ts = ttb.timestamps_generator()
        hgg = hg.HeteroGraphsGenerator(database, cant, train_ts, val_ts, test_ts)
        hgg.trace_kpi()

    m = None
    if 'sweep' in stages:
        print("\n" + "=" * 60)
        print("SWEEP — hyperparameter search")
        print("=" * 60)
        m = t.Modelling(database, cant)
        print(f"Task ID: {m.task_id}")
        m.sweep(selection_metric=selection_metric)

    if 'train' in stages:
        print("\n" + "=" * 60)
        print("TRAIN — HGT regression model")
        print("=" * 60)
        # Always reload fresh here (not whatever 'm' sweep left behind) so this reads
        # model_params.json's just-swept best params -- matches every original script's
        # own defensive reload-after-sweep convention.
        m = t.Modelling(database, cant)
        print(f"  Loaded params: {m.params}")
        m.Modelling(selection_metric=selection_metric)

    if 'homo' in stages:
        print("\n" + "=" * 60)
        print("HOMO — HomoGNN baseline")
        print("=" * 60)
        if m is None:
            m = t.Modelling(database, cant)
        m.Homo_Reg_Modelling()

    if 'kdim' in stages:
        print("\n" + "=" * 60)
        print("KDIM — k-dim GNN baseline (HOEG's own architecture)")
        print("=" * 60)
        if m is None:
            m = t.Modelling(database, cant)
        m.KDim_Reg_Modelling()

    # ── Tail: validate / compare / explain, all operate on the trained checkpoint.
    # Built via a single Explainer instance (Explainer IS a Modelling) rather than the
    # original scripts' pattern of a separate Modelling instance for validate and another
    # Explainer instance for compare/explain -- same observable behavior, one fewer
    # redundant model load.
    need_tail = any(s in stages for s in ('validate', 'compare', 'explain'))
    last = None
    if need_tail:
        e = exp.Explainer(database, cant)
        e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
        e.model.eval()

        if 'validate' in stages:
            print("\n" + "=" * 60)
            print("VALIDATE")
            print("=" * 60)
            df = _predict_all(e)
            last = df[df['last_event']]

            _print_metrics(df, "ALL prefixes")
            mae_last, rmse_last, r2_last = _print_metrics(last, "LAST-EVENT prefixes only")

            print("\nMAE by prefix depth:")
            bins = [0, 3, 6, 9, 999]
            labels = ['1-3', '4-6', '7-9', '10+']
            df['depth_bin'] = pd.cut(df['n_events'], bins=bins, labels=labels, right=True)
            depth_stats = df.groupby('depth_bin', observed=True)['abs_err_h'].agg(['mean', 'count'])
            depth_stats.columns = ['MAE (h)', 'n']
            print(depth_stats.to_string())

            print("\nTop-5 highest true remaining times vs predictions:")
            print(last.nlargest(5, 'true_h')[['order_id', 'true_h', 'pred_h', 'abs_err_h']]
                  .to_string(index=False))

            print("\nTop-5 largest errors:")
            print(last.nlargest(5, 'abs_err_h')[['order_id', 'true_h', 'pred_h', 'abs_err_h']]
                  .to_string(index=False))

            out_dir = f"files/explainer_outputs/{database}/validation_{cant}"
            os.makedirs(out_dir, exist_ok=True)
            fig, axes = plt.subplots(1, 2, figsize=(12, 5))
            axes[0].scatter(last['true_h'], last['pred_h'], alpha=0.5, s=20)
            lim = max(last['true_h'].max(), last['pred_h'].max()) * 1.05
            axes[0].plot([0, lim], [0, lim], 'r--', lw=1)
            axes[0].set_xlabel('True remaining time (h)')
            axes[0].set_ylabel('Predicted remaining time (h)')
            axes[0].set_title(f'Predicted vs. True  (R²={r2_last:.3f})')
            axes[1].hist(last['pred_h'] - last['true_h'], bins=30, edgecolor='k', linewidth=0.4)
            axes[1].axvline(0, color='r', linestyle='--')
            axes[1].set_xlabel('Residual (pred − true, h)')
            axes[1].set_title(f'Residuals  (MAE={mae_last:.1f}h, RMSE={rmse_last:.1f}h)')
            plt.tight_layout()
            plt.savefig(f'{out_dir}/residuals.png', dpi=150)
            plt.close()
            print(f"\nResidual plot saved to {out_dir}/residuals.png")

        if 'compare' in stages:
            print("\n" + "=" * 60)
            print("COMPARE — HGT vs. baselines")
            print("=" * 60)
            homo_model_path = e.model_path.replace(".pth", "_homo.pth")
            homo_meta_path = homo_model_path.replace(".pth", "_meta.json")
            homo_is_stale = os.path.exists(homo_model_path) and not os.path.exists(homo_meta_path)
            if not os.path.exists(homo_model_path) or homo_is_stale:
                print("Training HomoGNN baseline (missing or stale for current hyperparameters)...")
                e.Homo_Reg_Modelling()
            comparison_df = e.compare_to_baselines()
            print(comparison_df.to_string(index=False))

        if 'explain' in stages:
            print("\n" + "=" * 60)
            print("EXPLAIN — feature attribution, aggregate LOO, trace explanations")
            print("=" * 60)
            e.explain_feature_attribution()

            print("\n" + "=" * 60)
            print("AGGREGATE LOO EXPLANATION (n=50 traces)")
            print("=" * 60)
            e.explain_aggregate(n_traces=50, top_k=5)

            if last is None:
                # 'explain' requested without 'validate' in the same run -- need
                # last-event predictions to pick a representative fast/slow order.
                last = _predict_all(e)
                last = last[last['last_event']]

            last_sorted = last.sort_values('true_h')
            fast_id = int(last_sorted.iloc[len(last_sorted) // 5]['order_id'])
            slow_id = int(last_sorted.iloc[-len(last_sorted) // 5]['order_id'])

            print("\n" + "=" * 60)
            fast_true = last_sorted[last_sorted['order_id'] == fast_id]['true_h'].values[0]
            print(f"TRACE EXPLANATION — fast {e.kpi_viewpoint} (id={fast_id}, true={fast_true:.0f}h)")
            print("=" * 60)
            e.explain_trace(fast_id, top_k=5)

            print("\n" + "=" * 60)
            slow_true = last_sorted[last_sorted['order_id'] == slow_id]['true_h'].values[0]
            print(f"TRACE EXPLANATION — slow {e.kpi_viewpoint} (id={slow_id}, true={slow_true:.0f}h)")
            print("=" * 60)
            e.explain_trace(slow_id, top_k=5)

    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--database', required=True, choices=['order_management', 'logistics'])
    parser.add_argument('--cant', required=True, type=int)
    parser.add_argument('--stages', required=True,
                         help=f"Comma-separated subset of: {','.join(STAGE_ORDER)}")
    parser.add_argument('--selection-metric', choices=['pooled', 'last_event'], default='pooled',
                         help="Val MAE used for early stopping / best-checkpoint selection in "
                              "the 'sweep' and 'train' stages. Default 'pooled' matches every "
                              "checkpoint trained to date; 'last_event' selects on the rare "
                              "last-event subset instead (see OPEN_ISSUES_FEASIBILITY.md item 11).")
    args = parser.parse_args()

    stages = {s.strip() for s in args.stages.split(',') if s.strip()}
    unknown = stages - set(STAGE_ORDER)
    if unknown:
        parser.error(f"Unknown stage(s): {sorted(unknown)}. Valid stages: {STAGE_ORDER}")

    run(args.database, args.cant, stages, selection_metric=args.selection_metric)


if __name__ == '__main__':
    main()
