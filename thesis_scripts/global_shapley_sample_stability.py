"""How much does the global Shapley top-20 depend on WHICH 50 executions were sampled?

global_shapley_methods_showcase.py runs flat and norm_total on the first 50 last-event test
executions of each KPI (window 0). Its --window option runs the same thing on further
disjoint 50-execution windows (window w = executions [50w, 50w+50)). The test splits hold
235-249 last-event executions for order management (4 windows) and 138-142 for logistics
and IoT (2 windows). This script compares the windows of each KPI × method × kind:

  jaccard_top20 / _top10  overlap of two windows' selected labels (|A∩B| / |A∪B|)
  present_elsewhere       share of one window's top-20 labels that OCCUR at all in the other
                          window's executions (selected or not), so a low Jaccard can be split
                          into "the label isn't there" (instance-specific nodes, e.g. one
                          customer) vs "it's there but ranked differently"
  spearman                rank correlation of the signed pooled LOO means, over the union of
                          both windows' top-20 labels that occur in both windows
  sign_agreement          share of those labels whose pooled LOO mean has the same sign
  spread_ratio            for labels in >= 2 windows' top 10: cross-window std of the Shapley
                          bar mean ÷ the median within-run revisit std (≈1: sampling the
                          executions adds no more noise than revisiting them; >1: the
                          windows genuinely disagree)
  n_persistent            labels in the top 10 of EVERY window

Pairwise metrics are averaged over window pairs (min/max kept for the whiskers).

Outputs, in files/explainer_outputs/global_shapley_sample_stability/:
  summary.csv / summary.tex      one row per KPI × method × kind
  pairs.csv                      every window pair's metrics
  stability_overview.png         Jaccard, ρ and sign agreement per KPI, flat vs norm_total
  jaccard_<kpi>.png              window × window top-20 Jaccard, nodes/features × method
  persistent_<kpi>_<kind>.png    per-window Shapley values of labels recurring in the top 10

Usage:
    PYTHONPATH=. python3 thesis_scripts/global_shapley_sample_stability.py
    PYTHONPATH=. python3 thesis_scripts/global_shapley_sample_stability.py --self-check
"""
import argparse
import itertools
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from thesis_scripts.global_shapley_methods_showcase import (
    KPIS, METHODS, METHOD_NAMES, UNITS, TOP_N, GRID_N, run_dirs, _jaccard)
from thesis_scripts.global_shapley_weighting_comparison import KINDS, _short

OUT_DIR = "files/explainer_outputs/global_shapley_sample_stability"
MAX_WINDOWS = 6
# Window-0 directory tag per KPI. Depart's original window-0 run decoded labels against
# LoadToVehicle's ocel.csv (only the graph tensors were swapped); _csvfix reruns it with
# Depart's own CSVs, the same way its other windows were collected.
WINDOW0_TAG = {"logistics_depart": "_csvfix"}
# Validated with the dataviz skill's palette checker (light surface: all checks pass).
COLORS = {"flat": "#D9763A", "norm_total": "#7457C4"}
INK, MUTED, GRID = "#1f1f1f", "#6b6b6b", "#e4e4e4"
KPI_NAMES = {k: v[3] for k, v in KPIS.items()}


def window_dirs(kpi):
    """{window: {method: dir}} for every window whose two method runs both exist."""
    database, _, task_id, _ = KPIS[kpi]
    found = {}
    for w in range(MAX_WINDOWS):
        dirs = run_dirs(database, task_id, w, WINDOW0_TAG.get(kpi, "") if w == 0 else "")
        if all(os.path.exists(os.path.join(d, KINDS[0][1])) for d in dirs.values()):
            found[w] = dirs
    return found


def load(d, kind):
    bars_csv, loo_csv = {k: (b, l) for k, b, l in KINDS}[kind]
    return pd.read_csv(os.path.join(d, bars_csv)), pd.read_csv(os.path.join(d, loo_csv))


def pair_metrics(a, b):
    """a, b: (bars, loo) for two windows of the same KPI × method × kind."""
    (bars_a, loo_a), (bars_b, loo_b) = a, b
    t20a, t20b = set(bars_a.label.head(TOP_N)), set(bars_b.label.head(TOP_N))
    t10a, t10b = set(bars_a.label.head(GRID_N)), set(bars_b.label.head(GRID_N))
    occ_a, occ_b = set(loo_a.label), set(loo_b.label)
    la, lb = loo_a.set_index("label").loo_mean, loo_b.set_index("label").loo_mean
    shared = sorted((t20a | t20b) & occ_a & occ_b)
    va, vb = la.reindex(shared).values, lb.reindex(shared).values
    rho = spearmanr(va, vb).statistic if len(shared) >= 3 else np.nan
    return {"jaccard_top20": _jaccard(t20a, t20b), "jaccard_top10": _jaccard(t10a, t10b),
            "present_elsewhere": (len(t20a & occ_b) + len(t20b & occ_a)) / (len(t20a) + len(t20b)),
            "spearman": rho,
            "sign_agreement": float(np.mean(np.sign(va) == np.sign(vb))) if shared else np.nan,
            "n_shared": len(shared)}


def per_label(runs):
    """Long table of each window's top-10 bars, for labels in >= 2 windows' top 10."""
    rows = [dict(window=w, label=r.label, mean=r.mean_signed_shift, std=r.std_shift, n=r["count"])
            for w, (bars, _) in runs.items() for _, r in bars.head(GRID_N).iterrows()]
    df = pd.DataFrame(rows)
    keep = df.groupby("label").window.nunique()
    return df[df.label.isin(keep[keep >= 2].index)]


def analyse(kpi, runs_by_method):
    """runs_by_method: {method: {kind: {window: (bars, loo)}}} -> (summary rows, pair rows)."""
    summary, pairs = [], []
    for method, by_kind in runs_by_method.items():
        for kind, runs in by_kind.items():
            ws = sorted(runs)
            prs = []
            for i, j in itertools.combinations(ws, 2):
                m = pair_metrics(runs[i], runs[j])
                prs.append(m)
                pairs.append(dict(kpi=kpi, method=method, kind=kind, window_a=i, window_b=j, **m))
            prs = pd.DataFrame(prs)
            rec = per_label(runs)
            # A single revisit has std 0 by construction; only n > 1 bars give a revisit std.
            within = rec[rec.n > 1].groupby("label")["std"].median()
            across = rec.groupby("label")["mean"].std()
            ratio = (across / within.reindex(across.index)).replace([np.inf], np.nan).dropna()
            persistent = set.intersection(*(set(runs[w][0].label.head(GRID_N)) for w in ws))
            row = dict(kpi=kpi, method=method, kind=kind, n_windows=len(ws))
            for col in ["jaccard_top20", "jaccard_top10", "present_elsewhere", "spearman",
                        "sign_agreement"]:
                row[col] = prs[col].mean()
                row[col + "_min"], row[col + "_max"] = prs[col].min(), prs[col].max()
            row.update(spread_ratio=ratio.median() if len(ratio) else np.nan,
                       n_recurring_top10=rec.label.nunique(), n_persistent=len(persistent),
                       persistent="; ".join(sorted(persistent)))
            summary.append(row)
    return summary, pairs


def _style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelcolor=INK, labelsize=8)
    ax.grid(True, axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def plot_overview(summary, path):
    metrics = [("jaccard_top20", "Top-20 Jaccard"), ("spearman", "Spearman ρ (pooled LOO)"),
               ("sign_agreement", "Sign agreement")]
    kinds = [k for k, _, _ in KINDS]
    kpis = [k for k in KPIS if k in set(summary.kpi)]
    fig, axes = plt.subplots(len(kinds), len(metrics), figsize=(13, 2.2 + 0.55 * len(kpis) * len(kinds)),
                             sharey=True, squeeze=False)
    offsets = {"flat": -0.14, "norm_total": 0.14}
    for r, kind in enumerate(kinds):
        for c, (col, name) in enumerate(metrics):
            ax = axes[r, c]
            _style(ax)
            for method, _ in METHODS:
                s = summary[(summary.kind == kind) & (summary.method == method)].set_index("kpi").reindex(kpis)
                y = np.arange(len(kpis)) + offsets[method]
                lo, hi = s[col] - s[col + "_min"], s[col + "_max"] - s[col]
                ax.errorbar(s[col], y, xerr=[lo, hi], fmt="o", ms=7, color=COLORS[method],
                            ecolor=COLORS[method], elinewidth=2, capsize=0,
                            markeredgecolor="white", markeredgewidth=1.5,
                            label=METHOD_NAMES[method] if (r, c) == (0, 0) else None)
                for xv, yv in zip(s[col], y):
                    if np.isfinite(xv):
                        ax.text(1.03, yv, f"{xv:.2f}", transform=ax.get_yaxis_transform(),
                                va="center", fontsize=7, color=MUTED, clip_on=False)
            ax.set_xlim(-1.05 if col == "spearman" else 0, 1.0)
            if col == "spearman":
                ax.axvline(0, color=MUTED, linewidth=0.8)
            ax.set_yticks(range(len(kpis)))
            ax.set_yticklabels([f"{KPI_NAMES[k]} ({int(summary[summary.kpi == k].n_windows.iloc[0])} win.)"
                                for k in kpis])
            ax.invert_yaxis() if c == 0 else None
            if r == 0:
                ax.set_title(name, fontsize=10, color=INK)
            if c == 0:
                ax.set_ylabel(kind, fontsize=10, color=INK)
    fig.legend(loc="upper center", ncol=2, frameon=False, fontsize=9, bbox_to_anchor=(0.5, 1.0))
    fig.suptitle("Global Shapley across disjoint 50-execution windows "
                 "(dot = mean over window pairs, bar = min–max)", fontsize=11, color=INK, y=1.04)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def plot_jaccard(kpi, runs_by_method, path):
    kinds = [k for k, _, _ in KINDS]
    fig, axes = plt.subplots(len(kinds), len(METHODS), figsize=(4.2 * len(METHODS), 3.6 * len(kinds)),
                             squeeze=False)
    for r, kind in enumerate(kinds):
        for c, (method, name) in enumerate(METHODS):
            runs = runs_by_method[method][kind]
            ws = sorted(runs)
            tops = {w: set(runs[w][0].label.head(TOP_N)) for w in ws}
            mat = np.array([[_jaccard(tops[a], tops[b]) for b in ws] for a in ws])
            ax = axes[r, c]
            ax.imshow(mat, cmap="Blues", vmin=0, vmax=1)
            for i, j in itertools.product(range(len(ws)), repeat=2):
                ax.text(j, i, f"{mat[i, j]:.2f}", ha="center", va="center", fontsize=9,
                        color="white" if mat[i, j] > 0.6 else INK)
            ax.set_xticks(range(len(ws)), [f"w{w}" for w in ws], fontsize=8)
            ax.set_yticks(range(len(ws)), [f"w{w}" for w in ws], fontsize=8)
            ax.set_title(f"{name} — {kind}", fontsize=9, color=INK)
            for s in ax.spines.values():
                s.set_visible(False)
    fig.suptitle(f"{KPIS[kpi][2]} — top-{TOP_N} Jaccard between execution windows",
                 fontsize=10, color=INK)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def plot_persistent(kpi, kind, runs_by_method, path):
    fig, axes = plt.subplots(1, len(METHODS), figsize=(7 * len(METHODS), 5.5), squeeze=False)
    for ax, (method, name) in zip(axes[0], METHODS):
        runs = runs_by_method[method][kind]
        n_w = len(runs)
        rec = per_label(runs)
        _style(ax)
        if rec.empty:
            ax.text(0.5, 0.5, "no label in the top 10 of ≥ 2 windows", ha="center",
                    transform=ax.transAxes, color=MUTED)
            ax.set_title(name, fontsize=10, color=INK)
            continue
        agg = rec.groupby("label")["mean"].agg(["mean", "std", "count"])
        agg = agg.reindex(agg["mean"].abs().sort_values(ascending=False).index).head(15)
        for i, (label, a) in enumerate(agg.iterrows()):
            v = rec[rec.label == label]["mean"].values
            ax.plot([v.min(), v.max()], [i, i], color=COLORS[method], alpha=0.35, linewidth=3,
                    solid_capstyle="round")
            ax.scatter(v, [i] * len(v), s=36, color=COLORS[method], edgecolor="white",
                       linewidth=1.2, zorder=3)
            ax.scatter([a["mean"]], [i], marker="|", s=160, color=INK, zorder=4)
            ax.text(1.01, i, f"{int(a['count'])}/{n_w}", transform=ax.get_yaxis_transform(),
                    va="center", fontsize=7, color=MUTED, clip_on=False)
        ax.axvline(0, color=MUTED, linewidth=0.8)
        ax.set_yticks(range(len(agg)), [_short(l, 34) for l in agg.index], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel(f"Shapley influence per window ({UNITS[method]})", fontsize=9, color=INK)
        ax.set_title(name, fontsize=10, color=INK)
    fig.suptitle(f"{KPIS[kpi][2]} — {kind} in the top {GRID_N} of ≥ 2 windows "
                 f"(dots = windows, tick = mean, right = windows it reached the top {GRID_N})",
                 fontsize=10, color=INK)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def write_tex(summary, path):
    lines = [r"\begin{tabular}{llrrrrrrr}", r"\toprule",
             r"KPI & Kind & Win. & \multicolumn{2}{c}{Top-20 Jaccard} & "
             r"\multicolumn{2}{c}{Spearman $\rho$} & \multicolumn{2}{c}{Persistent top-10} \\",
             r" & & & flat & $\div$ total & flat & $\div$ total & flat & $\div$ total \\",
             r"\midrule"]
    for kpi in [k for k in KPIS if k in set(summary.kpi)]:
        for kind in [k for k, _, _ in KINDS]:
            s = summary[(summary.kpi == kpi) & (summary.kind == kind)].set_index("method")
            f, t = s.loc["flat"], s.loc["norm_total"]
            lines.append(f"{KPI_NAMES[kpi]} & {kind} & {int(f.n_windows)} & "
                         f"{f.jaccard_top20:.2f} & {t.jaccard_top20:.2f} & "
                         f"{f.spearman:.2f} & {t.spearman:.2f} & "
                         f"{int(f.n_persistent)} & {int(t.n_persistent)} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


def self_check():
    """Window 0 against itself must give perfect agreement."""
    kpi = "om_payorder"
    w0 = window_dirs(kpi)[0]
    runs = {m: {k: {0: load(w0[m], k), 1: load(w0[m], k)} for k, _, _ in KINDS} for m, _ in METHODS}
    summary, _ = analyse(kpi, runs)
    for row in summary:
        for col in ["jaccard_top20", "jaccard_top10", "present_elsewhere", "spearman", "sign_agreement"]:
            assert abs(row[col] - 1.0) < 1e-12, (row["method"], row["kind"], col, row[col])
        assert row["spread_ratio"] == 0 or np.isnan(row["spread_ratio"]), row["spread_ratio"]
    print("self-check passed: window 0 vs itself gives Jaccard = ρ = sign agreement = 1")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    summary, pairs = [], []
    for kpi in KPIS:
        found = window_dirs(kpi)
        if len(found) < 2:
            print(f"{kpi}: only windows {sorted(found)} collected, skipped")
            continue
        runs = {m: {k: {w: load(d[m], k) for w, d in found.items()} for k, _, _ in KINDS}
                for m, _ in METHODS}
        s, p = analyse(kpi, runs)
        summary += s
        pairs += p
        plot_jaccard(kpi, runs, os.path.join(OUT_DIR, f"jaccard_{kpi}.png"))
        for kind, _, _ in KINDS:
            plot_persistent(kpi, kind, runs, os.path.join(OUT_DIR, f"persistent_{kpi}_{kind}.png"))
        print(f"{kpi}: windows {sorted(found)}")
    summary = pd.DataFrame(summary)
    summary.to_csv(os.path.join(OUT_DIR, "summary.csv"), index=False)
    pd.DataFrame(pairs).to_csv(os.path.join(OUT_DIR, "pairs.csv"), index=False)
    write_tex(summary, os.path.join(OUT_DIR, "summary.tex"))
    plot_overview(summary, os.path.join(OUT_DIR, "stability_overview.png"))
    with pd.option_context("display.width", 200, "display.max_columns", 30):
        print(summary[["kpi", "method", "kind", "n_windows", "jaccard_top20", "jaccard_top10",
                       "present_elsewhere", "spearman", "sign_agreement", "spread_ratio",
                       "n_persistent"]].round(2).to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()
    self_check() if args.self_check else main()
