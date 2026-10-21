"""Weighted vs. flat pooling in the all-prefixes global Shapley analysis.

explain_aggregate_shapley(scope='all_prefixes') explains every prefix of the first
N_TRACES test executions. With pooling='execution_mean' (the default) a label's LOO
shifts are averaged within each execution first, so a long execution counts once;
with pooling='flat' every prefix occurrence counts equally, so long executions weigh
more. This script runs both on the same executions (the LOO pass is cached on the
Explainer, so it is paid once per dataset) and draws a series of comparison graphs.

Weighting acts in two places, and the graphs cover both:
  - LOO pooling/ranking (loo_pooled_*.csv) -- decides which labels make the top 20.
  - Shapley revisits (aggregate_*_bars.csv) -- the displayed bar values.
A third run, pooling='flat_distinct', separates the two: it selects like 'flat'
but revisits like 'execution_mean'. So weighted vs. flat_distinct differs only in
selection, and flat_distinct vs. flat only in how revisit prefixes are sampled.

Graphs, per dataset and per chart kind (nodes / features), in
files/explainer_outputs/<db>/aggregate_weighting_comparison_<task_id>/:
  1. side_by_side_<kind>.png   weighted | flat_distinct | flat Shapley bar charts
  2. rank_shift_<kind>.png     LOO rank under each pooling (slope chart)
  3. loo_scatter_<kind>.png    weighted vs. flat LOO mean per label, y=x diagonal,
                               size = occurrences, colour = busiest-execution share
  4. dumbbell_<kind>.png       Shapley value per label under each pooling
  5. prefixes_per_execution.png  how unequal the execution lengths are
plus a CSV per graph and summary.csv (top-20 Jaccard overlaps, Spearman rank
correlation of LOO means, mean |Shapley difference| on shared labels per pair,
largest movers). Graphs 2-3 need only the weighted and flat runs (flat_distinct's
LOO ranking is flat's); the flat_distinct run is left out if it has not been
collected.

save_dirs are KPI-suffixed (aggregate_all_prefixes[_flat|_flat_distinct]_<task_id>)
rather than the function's default, same reasoning as
loo_shapley_aggregate_showcase.py.

Usage:
    python3 thesis_scripts/global_shapley_weighting_comparison.py --collect   # ~1 h
    python3 thesis_scripts/global_shapley_weighting_comparison.py --collect --poolings flat_distinct
    python3 thesis_scripts/global_shapley_weighting_comparison.py             # plots only
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATASETS = [
    ("order_management", 2000, "TimeFrom_Orders_to_PayOrder"),
    ("cargo_pickup_iot", 2000, "TimeFrom_Pickupplan_to_EvaluateTruckExit"),
]
N_TRACES = 50
TOP_N = 20
POOLINGS = [("execution_mean", "weighted (per-execution mean)"),
            ("flat_distinct", "flat selection, distinct-execution revisits"),
            ("flat", "flat (every prefix)")]
LOO_POOLINGS = [POOLINGS[0], POOLINGS[2]]  # flat_distinct's LOO ranking is flat's
POOLING_NAMES = dict(POOLINGS)
KINDS = [("nodes", "aggregate_explanation_bars.csv", "loo_pooled_nodes.csv"),
         ("features", "aggregate_feature_bars.csv", "loo_pooled_features.csv")]

INCREASE, DECREASE = "#2ca02c", "#d62728"  # plot_aggregate_explanation_bars() convention
POOLING_COLORS = {"execution_mean": "#4C72B0", "flat_distinct": "#55A868", "flat": "#DD8452"}


def run_dir(database, task_id, pooling):
    suffix = {"execution_mean": "", "flat": "_flat", "flat_distinct": "_flat_distinct"}[pooling]
    return f"files/explainer_outputs/{database}/aggregate_all_prefixes{suffix}_{task_id}"


def collect(poolings):
    import torch
    import explainer as exp

    for database, cant, expected_task in DATASETS:
        e = exp.Explainer(database, cant)
        e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
        e.model.eval()
        if e.task_id != expected_task:
            raise SystemExit(f"{database}: config.yml points at {e.task_id}, expected {expected_task}")
        for pooling in poolings:
            print(f"\n=== {database} / {e.task_id} / pooling={pooling}")
            e.explain_aggregate_shapley(n_traces=N_TRACES, scope='all_prefixes', pooling=pooling,
                                        save_dir=run_dir(database, e.task_id, pooling))


def _short(label, n=38):
    return label if len(label) <= n else label[:n - 1] + "…"


def _plot_side_by_side(bars, kind, title, path):
    panels = [(p, n) for p, n in POOLINGS if p in bars]
    fig, axes = plt.subplots(1, len(panels), figsize=(7.5 * len(panels), max(4, 0.38 * TOP_N + 1.5)),
                             sharex=True)
    lim = max(float(df.mean_signed_shift.abs().max()) for df in bars.values()) * 1.15
    for ax, (pooling, name) in zip(axes, panels):
        df = bars[pooling].head(TOP_N)
        vals = df.mean_signed_shift.values
        ax.barh(range(len(df)), vals, color=[INCREASE if v > 0 else DECREASE for v in vals])
        ax.set_yticks(range(len(df)))
        ax.set_yticklabels([_short(l) for l in df.label], fontsize=8)
        ax.invert_yaxis()
        ax.axvline(0, color="black", linewidth=0.8)
        for i, v in enumerate(vals):
            ax.text(v + (0.02 if v >= 0 else -0.02) * lim, i, f"{v:+.2f}h", va="center",
                    ha="left" if v >= 0 else "right", fontsize=7)
        ax.set_xlim(-lim, lim)
        ax.set_title(name, fontsize=11)
        ax.set_xlabel("Shapley influence on predicted remaining time (hours)")
        ax.grid(True, axis="x", alpha=0.3)
    fig.suptitle(f"{title} — global Shapley {kind} under each pooling")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def _ranked(loo):
    loo = loo.copy()
    loo["rank"] = loo.loo_mean.abs().rank(ascending=False, method="first").astype(int)
    return loo.set_index("label")


def _plot_rank_shift(loo_w, loo_f, kind, title, path):
    rw, rf = _ranked(loo_w), _ranked(loo_f)
    labels = sorted(set(rw.index[rw["rank"] <= TOP_N]) | set(rf.index[rf["rank"] <= TOP_N]),
                    key=lambda l: rw["rank"].get(l, 1e9))
    cap = TOP_N + 10
    rows = []
    n_capped = {0: 0, 1: 0}  # labels beyond the cap are stacked below it, not overlaid

    def _y(rank, side):
        if rank < cap:
            return rank
        n_capped[side] += 1
        return cap + n_capped[side] - 1

    fig, ax = plt.subplots(figsize=(9, max(6, 0.32 * len(labels) + 2)))
    for l in labels:
        a, b = int(rw["rank"].get(l, cap)), int(rf["rank"].get(l, cap))
        crosses = (a <= TOP_N) != (b <= TOP_N)
        ya, yb = _y(a, 0), _y(b, 1)
        ax.plot([0, 1], [ya, yb], color="#E69F00" if crosses else "#999999",
                linewidth=2.2 if crosses else 1.0, marker="o", markersize=4)
        ax.text(-0.03, ya, f"{_short(l, 34)}  {a if a < cap else f'>{cap - 1}'}",
                ha="right", va="center", fontsize=7)
        ax.text(1.03, yb, f"{b if b < cap else f'>{cap - 1}'}  {_short(l, 34)}",
                ha="left", va="center", fontsize=7)
        rows.append({"label": l, "rank_weighted": a, "rank_flat": b, "crosses_top20": crosses})
    ax.axhline(TOP_N + 0.5, color="black", linestyle=":", linewidth=1)
    ax.text(1.88, TOP_N + 0.4, f"top-{TOP_N} cut-off", ha="right", va="bottom", fontsize=8)
    lowest = max(max(r["rank_weighted"], r["rank_flat"]) for r in rows)
    ax.set_ylim(min(lowest, cap) + max(n_capped.values()) + 1, 0)
    ax.set_xlim(-0.9, 1.9)
    ax.set_xticks([0, 1])
    ax.set_xticklabels([n for _, n in LOO_POOLINGS])
    ax.set_yticks([])
    for s in ("left", "right", "top"):
        ax.spines[s].set_visible(False)
    ax.set_title(f"{title} — LOO rank of {kind} under each pooling\n"
                 f"(orange: enters or leaves the top {TOP_N})", fontsize=10)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return pd.DataFrame(rows)


def _plot_loo_scatter(loo_w, loo_f, kind, title, path):
    m = loo_w.merge(loo_f[["label", "loo_mean"]], on="label", suffixes=("_weighted", "_flat"))
    m["diff"] = m.loo_mean_flat - m.loo_mean_weighted
    fig, ax = plt.subplots(figsize=(8, 7))
    sc = ax.scatter(m.loo_mean_weighted, m.loo_mean_flat, s=12 + 6 * np.sqrt(m.n_occurrences),
                    c=m.max_exec_share, cmap="viridis", vmin=0, vmax=1, alpha=0.75,
                    edgecolors="black", linewidths=0.3)
    lim = float(np.abs(m[["loo_mean_weighted", "loo_mean_flat"]].values).max()) * 1.1
    ax.plot([-lim, lim], [-lim, lim], color="black", linestyle="--", linewidth=0.8)
    ax.axhline(0, color="#bbbbbb", linewidth=0.6)
    ax.axvline(0, color="#bbbbbb", linewidth=0.6)
    # Only labels visibly off the diagonal -- near-zero ones would pile up unreadably.
    movers = m.reindex(m["diff"].abs().sort_values(ascending=False).index).head(8)
    for r in movers[movers["diff"].abs() > 0.02 * lim].itertuples():
        ax.annotate(_short(r.label, 30), (r.loo_mean_weighted, r.loo_mean_flat), fontsize=7,
                    xytext=(5, 5), textcoords="offset points")
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-lim, lim)
    ax.set_xlabel("LOO mean, weighted (hours)")
    ax.set_ylabel("LOO mean, flat (hours)")
    plt.colorbar(sc, ax=ax, shrink=0.8, label="share of occurrences from busiest execution")
    ax.set_title(f"{title} — LOO mean per {kind[:-1]} label, weighted vs. flat\n"
                 f"(point size = occurrences; on the dashed line = pooling makes no difference)",
                 fontsize=10)
    ax.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return m


def _plot_dumbbell(bars, kind, title, path):
    panels = [(p, n) for p, n in POOLINGS if p in bars]
    vals = {p: bars[p].head(TOP_N).set_index("label").mean_signed_shift for p, _ in panels}
    labels = sorted(set().union(*(v.index for v in vals.values())),
                    key=lambda l: -max(abs(v.get(l, 0.0)) for v in vals.values()))
    fig, ax = plt.subplots(figsize=(9, max(5, 0.3 * len(labels) + 1.5)))
    rows = []
    for i, l in enumerate(labels):
        present = {p: v[l] for p, v in vals.items() if l in v.index}
        if len(present) > 1:
            ax.plot([min(present.values()), max(present.values())], [i, i], color="#999999",
                    linewidth=1.2, zorder=1)
        for p, x in present.items():
            ax.scatter(x, i, color=POOLING_COLORS[p], s=36, zorder=2)
        if len(present) < len(vals):
            missing = ", ".join(POOLING_NAMES[p].split(" (")[0] for p in vals if p not in present)
            ax.text(max(present.values()), i, f"  not in top {TOP_N}: {missing}",
                    va="center", ha="left", fontsize=7, color="#555555")
        rows.append({"label": l, **{f"shapley_{p}": present.get(p) for p in vals}})
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels([_short(l) for l in labels], fontsize=8)
    ax.invert_yaxis()
    ax.axvline(0, color="black", linewidth=0.8)
    for p, name in panels:
        ax.scatter([], [], color=POOLING_COLORS[p], label=name)
    ax.legend(fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=len(panels))
    ax.set_xlabel("Shapley influence on predicted remaining time (hours)")
    ax.set_title(f"{title}\nShapley value per {kind[:-1]} label under each pooling", fontsize=10)
    ax.grid(True, axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return pd.DataFrame(rows)


def _plot_prefix_histogram(metrics, title, path):
    per_exec = metrics.dropna(subset=["order_id"]).groupby("order_id").size()
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(per_exec.values, bins=range(1, int(per_exec.max()) + 2), color="#8C8C8C",
            edgecolor="white")
    ax.axvline(per_exec.median(), color="black", linestyle="--", linewidth=1,
               label=f"median {per_exec.median():.0f}")
    ax.set_xlabel("Prefixes per execution")
    ax.set_ylabel("Executions")
    ax.set_title(f"{title} — {len(per_exec)} executions, {int(per_exec.sum())} prefixes "
                 f"(range {per_exec.min()}–{per_exec.max()})", fontsize=10)
    ax.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return per_exec


def plot():
    for database, _, task_id in DATASETS:
        title = f"{database} / {task_id.replace('TimeFrom_', '').replace('_to_', ' → ')}"
        out = f"files/explainer_outputs/{database}/aggregate_weighting_comparison_{task_id}"
        os.makedirs(out, exist_ok=True)
        dirs = {p: run_dir(database, task_id, p) for p, _ in POOLINGS}
        if not os.path.isdir(dirs["flat_distinct"]):
            print(f"{database}: no flat_distinct run collected, comparing weighted vs. flat only")
            del dirs["flat_distinct"]
        summary = []
        for kind, bars_csv, loo_csv in KINDS:
            bars = {p: pd.read_csv(os.path.join(d, bars_csv)) for p, d in dirs.items()}
            loo_w = pd.read_csv(os.path.join(dirs["execution_mean"], loo_csv))
            loo_f = pd.read_csv(os.path.join(dirs["flat"], loo_csv))

            _plot_side_by_side(bars, kind, title, os.path.join(out, f"side_by_side_{kind}.png"))
            pd.concat({p: b.head(TOP_N) for p, b in bars.items()}, names=["pooling"]).to_csv(
                os.path.join(out, f"side_by_side_{kind}.csv"))
            ranks = _plot_rank_shift(loo_w, loo_f, kind, title, os.path.join(out, f"rank_shift_{kind}.png"))
            ranks.to_csv(os.path.join(out, f"rank_shift_{kind}.csv"), index=False)
            scatter = _plot_loo_scatter(loo_w, loo_f, kind, title, os.path.join(out, f"loo_scatter_{kind}.png"))
            scatter.to_csv(os.path.join(out, f"loo_scatter_{kind}.csv"), index=False)
            dumb = _plot_dumbbell(bars, kind, title, os.path.join(out, f"dumbbell_{kind}.png"))
            dumb.to_csv(os.path.join(out, f"dumbbell_{kind}.csv"), index=False)

            top_w, top_f = set(_ranked(loo_w).query("rank <= @TOP_N").index), set(_ranked(loo_f).query("rank <= @TOP_N").index)
            movers = scatter.reindex(scatter["diff"].abs().sort_values(ascending=False).index).head(5)
            row = {
                "kind": kind,
                "labels": len(scatter),
                "loo_top20_jaccard": round(len(top_w & top_f) / len(top_w | top_f), 3),
                "loo_mean_spearman": round(scatter[["loo_mean_weighted", "loo_mean_flat"]]
                                           .corr(method="spearman").iloc[0, 1], 3),
            }
            # Per pair of runs: overlap of the displayed top 20, and the mean absolute
            # Shapley difference on the labels both display. With flat_distinct present,
            # weighted vs. flat_distinct is the selection effect alone and
            # flat_distinct vs. flat the revisit-sampling effect alone.
            shap = {p: b.head(TOP_N).set_index("label").mean_signed_shift for p, b in bars.items()}
            pairs = [("execution_mean", "flat"), ("execution_mean", "flat_distinct"),
                     ("flat_distinct", "flat")]
            for a, b in pairs:
                if a not in shap or b not in shap:
                    continue
                shared = shap[a].index.intersection(shap[b].index)
                union = shap[a].index.union(shap[b].index)
                row[f"shapley_top20_jaccard_{a}_vs_{b}"] = round(len(shared) / len(union), 3)
                row[f"shapley_mean_abs_diff_h_{a}_vs_{b}"] = round(
                    float((shap[a][shared] - shap[b][shared]).abs().mean()), 3)
            summary.append({
                **row,
                "largest_movers": "; ".join(
                    f"{r.label}: {r.loo_mean_weighted:+.2f}h -> {r.loo_mean_flat:+.2f}h "
                    f"(busiest exec {r.max_exec_share:.0%} of {r.n_occurrences})"
                    for r in movers.itertuples()),
            })

        metrics = pd.read_csv(os.path.join(dirs["execution_mean"], "aggregate_metrics.csv"))
        _plot_prefix_histogram(metrics, title, os.path.join(out, "prefixes_per_execution.png")).to_csv(
            os.path.join(out, "prefixes_per_execution.csv"), header=["n_prefixes"])
        pd.DataFrame(summary).to_csv(os.path.join(out, "summary.csv"), index=False)
        print(f"\n{title}  ->  {out}")
        print(pd.DataFrame(summary).drop(columns="largest_movers").T.to_string(header=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true",
                        help="run the poolings per dataset first (~1 h), then plot")
    parser.add_argument("--poolings", nargs="+", choices=[p for p, _ in POOLINGS],
                        default=[p for p, _ in POOLINGS],
                        help="poolings to collect (default: all three)")
    args = parser.parse_args()
    if args.collect:
        collect(args.poolings)
    plot()
