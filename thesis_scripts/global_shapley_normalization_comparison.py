"""Time-normalized global Shapley (de Leoni's proposal) vs. the pooling options.

De Leoni proposes handling prefixes of differing lengths by dividing each Shapley value
by the elapsed time of the process, giving a unitless impact. explain_aggregate_shapley()
implements two readings as normalize='elapsed' (time since the execution started,
floored at 1 h) and normalize='total_duration' (elapsed + true remaining time). Every LOO
shift is divided before pooling, so the normalization also decides the top 20; the
Shapley revisits are divided the same way. Both are run with pooling='flat_distinct'
(flat selection, revisits in distinct executions), so the normalization is the only
length correction, and compared with the revisit_n=8 runs of
global_shapley_weighting_comparison.py (run that with --collect --revisit-n 8 first).

Graphs, per dataset, in
files/explainer_outputs/<db>/aggregate_normalization_comparison_<task_id>/:
  1. side_by_side_<kind>.png    raw | ÷elapsed | ÷total Shapley bars (own x-axes)
  2. rank_shift_<kind>_<norm>.png  LOO rank, raw vs. each normalization
  3. length_bias_<kind>.png     median |value| by elapsed-time decile, per method,
                                scaled by its own overall median
  4. floor_sensitivity.png      ÷elapsed top-20 overlap with the 1 h floor as the
                                floor varies, plus the share of prefixes floored
  5. relative_noise.png         per-label std/|mean| across Shapley revisits
plus a CSV per graph and summary.csv (top-20 Jaccard and LOO Spearman against every
existing pooling, length-bias Spearman per method, share floored, median CV, movers).

Usage:
    python3 thesis_scripts/global_shapley_normalization_comparison.py --collect   # ~1.5 h
    python3 thesis_scripts/global_shapley_normalization_comparison.py             # plots only
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from thesis_scripts.global_shapley_weighting_comparison import (
    DATASETS, N_TRACES, INCREASE, DECREASE, KINDS, _ranked, _plot_rank_shift, _short, run_dir)

REVISIT_N = 8
POOLING = "flat_distinct"
ELAPSED_FLOOR_H = 1.0
FLOORS_H = [0.25, 0.5, 1.0, 2.0, 4.0, 8.0]
TOP_N = 20
METHODS = [("raw", "flat_distinct (hours)"),
           ("elapsed", f"÷ elapsed (floor {ELAPSED_FLOOR_H:g} h)"),
           ("total_duration", "÷ total duration")]
METHOD_NAMES = dict(METHODS)
METHOD_COLORS = {"raw": "#55A868", "elapsed": "#C44E52", "total_duration": "#8172B2"}
NORM_SUFFIX = {"elapsed": "_norm_elapsed", "total_duration": "_norm_total"}
UNITS = {"raw": "hours", "elapsed": "unitless", "total_duration": "unitless"}


def method_dir(database, task_id, method):
    base = run_dir(database, task_id, POOLING, REVISIT_N)
    return base if method == "raw" else base + NORM_SUFFIX[method]


def collect():
    import torch
    import explainer as exp

    for database, cant, expected_task in DATASETS:
        e = exp.Explainer(database, cant)
        e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
        e.model.eval()
        if e.task_id != expected_task:
            raise SystemExit(f"{database}: config.yml points at {e.task_id}, expected {expected_task}")
        for method in ("elapsed", "total_duration"):
            print(f"\n=== {database} / {e.task_id} / normalize={method}")
            e.explain_aggregate_shapley(n_traces=N_TRACES, scope='all_prefixes', pooling=POOLING,
                                        revisit_n=REVISIT_N, normalize=method,
                                        elapsed_floor_h=ELAPSED_FLOOR_H,
                                        save_dir=method_dir(database, e.task_id, method))


def _values(occ, method, floor=ELAPSED_FLOOR_H):
    # Per-occurrence value under a method, recomputed from the raw shift.
    if method == "raw":
        return occ.raw_shift_h
    if method == "elapsed":
        return occ.raw_shift_h / occ.elapsed_h.clip(lower=floor)
    return occ.raw_shift_h / occ.total_h


def _top_from_occ(occ, method, floor=ELAPSED_FLOOR_H):
    # Flat pooling, as in explain_aggregate_shapley(pooling='flat_distinct').
    pooled = _values(occ, method, floor).groupby(occ.label).mean()
    return set(pooled.abs().sort_values(ascending=False, kind="stable").index[:TOP_N])


def _plot_side_by_side(bars, kind, title, path):
    fig, axes = plt.subplots(1, len(METHODS), figsize=(7.5 * len(METHODS), max(4, 0.38 * TOP_N + 1.5)))
    for ax, (method, name) in zip(axes, METHODS):
        df = bars[method].head(TOP_N)
        vals = df.mean_signed_shift.values
        lim = float(np.abs(vals).max()) * 1.25
        ax.barh(range(len(df)), vals, color=[INCREASE if v > 0 else DECREASE for v in vals])
        ax.set_yticks(range(len(df)))
        ax.set_yticklabels([_short(l) for l in df.label], fontsize=8)
        ax.invert_yaxis()
        ax.axvline(0, color="black", linewidth=0.8)
        fmt = "{:+.2f}h" if method == "raw" else "{:+.3f}"
        for i, v in enumerate(vals):
            ax.text(v + (0.02 if v >= 0 else -0.02) * lim, i, fmt.format(v), va="center",
                    ha="left" if v >= 0 else "right", fontsize=7)
        ax.set_xlim(-lim, lim)
        ax.set_title(name, fontsize=11)
        ax.set_xlabel(f"Shapley influence ({UNITS[method]})")
        ax.grid(True, axis="x", alpha=0.3)
    fig.suptitle(f"{title} — global Shapley {kind}: raw vs. time-normalized")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def _length_bias(occ, kind, title, path):
    # Does a method's typical |value| still depend on how far the process has run?
    d = occ.copy()
    d["decile"] = pd.qcut(d.elapsed_h.rank(method="first"), 10, labels=False)
    centres = d.groupby("decile").elapsed_h.median()
    rows, rhos = [], {}
    fig, ax = plt.subplots(figsize=(8, 5))
    for method, name in METHODS:
        v = _values(d, method).abs()
        curve = v.groupby(d.decile).median() / v.median()
        rhos[method] = float(pd.Series(v.values).corr(d.elapsed_h.reset_index(drop=True),
                                                      method="spearman"))
        ax.plot(centres.values, curve.values, marker="o", color=METHOD_COLORS[method],
                label=f"{name}  (Spearman ρ = {rhos[method]:+.2f})")
        rows += [{"method": method, "decile": int(k), "elapsed_h_median": centres[k],
                  "median_abs_scaled": curve[k]} for k in curve.index]
    ax.axhline(1, color="black", linestyle=":", linewidth=0.8)
    ax.set_yscale("log")
    ax.set_xlabel("Elapsed time at the prefix (h, decile median)")
    ax.set_ylabel("Median |value| in decile ÷ overall median")
    ax.set_title(f"{title} — length dependence of {kind[:-1]} LOO values\n"
                 f"(flat line at 1 = no dependence on elapsed time)", fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return pd.DataFrame(rows), rhos


def _floor_sensitivity(occs, title, path):
    # Prefix-level share floored: one elapsed time per prefix graph.
    prefix_elapsed = occs["nodes"].drop_duplicates("trace_idx").elapsed_h
    rows = []
    for floor in FLOORS_H:
        row = {"floor_h": floor, "share_prefixes_floored": float((prefix_elapsed < floor).mean())}
        for kind, occ in occs.items():
            ref, top = _top_from_occ(occ, "elapsed"), _top_from_occ(occ, "elapsed", floor)
            row[f"jaccard_{kind}"] = len(ref & top) / len(ref | top)
        rows.append(row)
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for kind, c in (("nodes", "#4C72B0"), ("features", "#DD8452")):
        ax.plot(df.floor_h, df[f"jaccard_{kind}"], marker="o", color=c, label=f"{kind} top-{TOP_N} overlap")
    ax.plot(df.floor_h, df.share_prefixes_floored, marker="s", linestyle="--", color="#555555",
            label="share of prefixes floored")
    ax.axvline(ELAPSED_FLOOR_H, color="black", linestyle=":", linewidth=0.8)
    ax.set_xscale("log", base=2)
    ax.set_xticks(FLOORS_H)
    ax.set_xticklabels([f"{f:g}" for f in FLOORS_H])
    ax.set_ylim(0, 1.05)
    ax.set_xlabel("Elapsed-time floor (h)")
    ax.set_ylabel(f"Jaccard vs. {ELAPSED_FLOOR_H:g} h floor / share")
    ax.set_title(f"{title} — ÷ elapsed: sensitivity to the floor", fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return df


def _relative_noise(bars_by_kind, title, path):
    rows = []
    for kind, bars in bars_by_kind.items():
        for method, _ in METHODS:
            df = bars[method].head(TOP_N)
            df = df[df["count"] > 1]
            for r in df.itertuples():
                rows.append({"kind": kind, "method": method, "label": r.label, "count": r.count,
                             "cv": r.std_shift / abs(r.mean_signed_shift)})
    d = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    groups = [(k, m) for k in bars_by_kind for m, _ in METHODS]
    data = [d[(d.kind == k) & (d.method == m)].cv.values for k, m in groups]
    bp = ax.boxplot(data, patch_artist=True, showfliers=True)
    for patch, (_, m) in zip(bp["boxes"], groups):
        patch.set_facecolor(METHOD_COLORS[m])
        patch.set_alpha(0.6)
    ax.set_xticks(range(1, len(groups) + 1))
    ax.set_xticklabels([f"{k}\n{METHOD_NAMES[m]}" for k, m in groups], fontsize=7)
    ax.set_yscale("log")
    ax.set_ylabel("std / |mean| across Shapley revisits")
    ax.set_title(f"{title} — relative noise of the Shapley bars (labels with >1 revisit)",
                 fontsize=10)
    ax.grid(True, axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return d


def plot():
    for database, _, task_id in DATASETS:
        title = f"{database} / {task_id.replace('TimeFrom_', '').replace('_to_', ' → ')}"
        out = f"files/explainer_outputs/{database}/aggregate_normalization_comparison_{task_id}"
        os.makedirs(out, exist_ok=True)
        dirs = {m: method_dir(database, task_id, m) for m, _ in METHODS}
        baselines = {p: run_dir(database, task_id, p, REVISIT_N) for p in ("execution_mean", "flat")}
        summary, occs, bars_by_kind = [], {}, {}
        for kind, bars_csv, loo_csv in KINDS:
            # Every run writes identical raw occurrences; the normalized runs have them.
            occ = pd.read_csv(os.path.join(dirs["elapsed"], f"loo_occurrences_{kind}.csv"))
            occs[kind] = occ
            bars = {m: pd.read_csv(os.path.join(d, bars_csv)) for m, d in dirs.items()}
            bars_by_kind[kind] = bars
            loo = {m: pd.read_csv(os.path.join(d, loo_csv)) for m, d in dirs.items()}
            loo.update({p: pd.read_csv(os.path.join(d, loo_csv)) for p, d in baselines.items()})

            # The raw flat_distinct ranking recomputed from occurrences must match its run.
            assert _top_from_occ(occ, "raw") == set(_ranked(loo["raw"]).query("rank <= @TOP_N").index), \
                f"{database}/{kind}: occurrence CSV does not reproduce the raw top {TOP_N}"

            _plot_side_by_side(bars, kind, title, os.path.join(out, f"side_by_side_{kind}.png"))
            pd.concat({m: b.head(TOP_N) for m, b in bars.items()}, names=["method"]).to_csv(
                os.path.join(out, f"side_by_side_{kind}.csv"))
            for method in ("elapsed", "total_duration"):
                ranks = _plot_rank_shift(loo["raw"], loo[method], kind, title,
                                         os.path.join(out, f"rank_shift_{kind}_{method}.png"),
                                         names=[METHOD_NAMES["raw"], METHOD_NAMES[method]])
                ranks.rename(columns={"rank_weighted": "rank_raw", "rank_flat": f"rank_{method}"}).to_csv(
                    os.path.join(out, f"rank_shift_{kind}_{method}.csv"), index=False)
            bias, rhos = _length_bias(occ, kind, title, os.path.join(out, f"length_bias_{kind}.png"))
            bias.to_csv(os.path.join(out, f"length_bias_{kind}.csv"), index=False)

            mean_elapsed = occ.groupby("label").elapsed_h.mean()
            tops = {m: set(_ranked(l).query("rank <= @TOP_N").index) for m, l in loo.items()}
            for method in ("elapsed", "total_duration"):
                row = {"kind": kind, "method": method,
                       "length_bias_rho": round(rhos[method], 3),
                       "length_bias_rho_raw": round(rhos["raw"], 3),
                       "top20_mean_elapsed_h": round(float(mean_elapsed[list(tops[method])].mean()), 1),
                       "top20_mean_elapsed_h_raw": round(float(mean_elapsed[list(tops["raw"])].mean()), 1)}
                for ref, ref_name in (("raw", "flat_distinct"), ("execution_mean", "weighted"),
                                      ("flat", "flat")):
                    a, b = tops[method], tops[ref]
                    row[f"top20_jaccard_vs_{ref_name}"] = round(len(a & b) / len(a | b), 3)
                    m = loo[method].merge(loo[ref], on="label", suffixes=("_n", "_r"))
                    row[f"loo_spearman_vs_{ref_name}"] = round(
                        m[["loo_mean_n", "loo_mean_r"]].corr(method="spearman").iloc[0, 1], 3)
                rr, rn = _ranked(loo["raw"])["rank"], _ranked(loo[method])["rank"]
                moved = (rn - rr.reindex(rn.index)).dropna()
                moved = moved[(rr.reindex(moved.index) <= TOP_N) | (rn.reindex(moved.index) <= TOP_N)]
                movers = moved.abs().sort_values(ascending=False).index[:5]
                row["largest_movers"] = "; ".join(
                    f"{l}: rank {int(rr[l])} -> {int(rn[l])} (mean elapsed {mean_elapsed[l]:.0f} h)"
                    for l in movers)
                summary.append(row)

        floor = _floor_sensitivity(occs, title, os.path.join(out, "floor_sensitivity.png"))
        floor.to_csv(os.path.join(out, "floor_sensitivity.csv"), index=False)
        noise = _relative_noise(bars_by_kind, title, os.path.join(out, "relative_noise.png"))
        noise.to_csv(os.path.join(out, "relative_noise.csv"), index=False)
        cv = noise.groupby(["kind", "method"]).cv.median()
        share = floor.set_index("floor_h").share_prefixes_floored[ELAPSED_FLOOR_H]
        for row in summary:
            row["median_cv"] = round(float(cv.get((row["kind"], row["method"]), np.nan)), 3)
            row["median_cv_raw"] = round(float(cv.get((row["kind"], "raw"), np.nan)), 3)
            row["share_prefixes_floored"] = round(float(share), 3) if row["method"] == "elapsed" else None
        df = pd.DataFrame(summary)
        df.to_csv(os.path.join(out, "summary.csv"), index=False)
        print(f"\n{title}  ->  {out}")
        print(df.drop(columns="largest_movers").set_index(["kind", "method"]).T.to_string())
        print(floor.round(3).to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true",
                        help="run both normalizations per dataset first (~1.5 h), then plot")
    args = parser.parse_args()
    if args.collect:
        collect()
    plot()
