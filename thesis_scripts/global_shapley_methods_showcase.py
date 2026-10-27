"""Side-by-side showcase of the six global Shapley variants, for the de Leoni meeting.

explain_aggregate_shapley() now has six ways to build the global ranking, all run on the
same N_TRACES test executions with seed=0 and revisit_n=8:
  last_event     only the last-event prefix of each execution
  exec_mean      every prefix; LOO shifts averaged within each execution first
  flat           every prefix; every prefix occurrence counts equally
  flat_distinct  flat selection, Shapley revisits in distinct executions
  norm_elapsed   flat_distinct, every shift ÷ elapsed time (floored at 1 h)
  norm_total     flat_distinct, every shift ÷ total execution duration
The all-prefix runs come from global_shapley_weighting_comparison.py --collect
--revisit-n 8 and global_shapley_normalization_comparison.py --collect; --collect here
adds the matching last-event run (a new directory, so the cited
aggregate_shapley_<task_id>/ figures are left alone).

Graphs, per dataset, in
files/explainer_outputs/<db>/aggregate_methods_showcase_<task_id>/:
  1. top10_grid_<kind>.png      each method's top-10 Shapley bars, ±1 std whiskers
                                across revisits (own x-axes: hours or unitless)
  2. focus_profile_<kind>.png   what each method's top 20 focuses on: where in the
                                process its labels occur, across how many executions,
                                how concentrated in one execution, which node types
  3. rank_bump_<kind>.png       rank of every label in any method's top 10, per method
  4. overlap_heatmap.png        6x6 top-20 Jaccard, nodes and features
plus a CSV per graph, and in files/explainer_outputs/:
  global_shapley_methods_table.{png,tex,csv}  how each method works, pros and cons,
                                              and measured OM / IoT numbers

Usage:
    PYTHONPATH=. python3 thesis_scripts/global_shapley_methods_showcase.py --collect   # minutes
    PYTHONPATH=. python3 thesis_scripts/global_shapley_methods_showcase.py             # plots only
"""
import argparse
import os
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from thesis_scripts.global_shapley_weighting_comparison import (
    DATASETS, N_TRACES, INCREASE, DECREASE, KINDS, _short, run_dir)
from thesis_scripts.global_shapley_normalization_comparison import method_dir

REVISIT_N = 8
TOP_N = 20
GRID_N = 10
METHODS = [("last_event", "last event only"),
           ("exec_mean", "all prefixes, per-execution mean"),
           ("flat", "all prefixes, flat"),
           ("flat_distinct", "flat, distinct-execution revisits"),
           ("norm_elapsed", "flat_distinct ÷ elapsed time"),
           ("norm_total", "flat_distinct ÷ total duration")]
METHOD_NAMES = dict(METHODS)
METHOD_COLORS = {"last_event": "#8C8C8C", "exec_mean": "#4C72B0", "flat": "#DD8452",
                 "flat_distinct": "#55A868", "norm_elapsed": "#C44E52", "norm_total": "#8172B2"}
UNITS = {m: "unitless" if m.startswith("norm_") else "hours" for m, _ in METHODS}
DB_SHORT = {"order_management": "OM", "cargo_pickup_iot": "IoT"}
TABLE_DIR = "files/explainer_outputs"

# How each method works, and its trade-offs; the measured columns are filled in from the runs.
DESCRIPTIONS = {
    "last_event": dict(
        prefixes="Last-event prefix of each execution",
        selection="Mean LOO shift per label over executions",
        revisits="≤8 executions containing the label",
        focus="What drove the final prediction of each execution",
        pros="Cheapest; one graph per execution, so no length weighting; matches the thesis figures",
        cons="Ignores in-flight prefixes, where predictions are actually used; elements that "
             "only matter early are invisible"),
    "exec_mean": dict(
        prefixes="Every prefix of the same executions",
        selection="Mean within each execution, then across executions",
        revisits="≤8 distinct executions, one random prefix each",
        focus="Elements that matter consistently across executions, at any stage",
        pros="Covers the whole process; each execution counts once regardless of its length",
        cons="An element decisive in only a few prefixes is diluted by its execution's other "
             "prefixes"),
    "flat": dict(
        prefixes="Every prefix of the same executions",
        selection="Mean over all prefix occurrences (flat)",
        revisits="≤8 occurrences sampled uniformly (may repeat an execution)",
        focus="Elements that recur in long executions",
        pros="Simplest pooling; reflects how often the model sees an element",
        cons="Long executions dominate the selection; revisits can cluster in one execution"),
    "flat_distinct": dict(
        prefixes="Every prefix of the same executions",
        selection="Flat (same as flat)",
        revisits="≤8 distinct executions, one random prefix each",
        focus="Flat's selection, with magnitudes not driven by one execution",
        pros="Separates the selection effect from the revisit-sampling effect",
        cons="Mainly diagnostic; mixes two weighting choices"),
    "norm_elapsed": dict(
        prefixes="Every prefix of the same executions",
        selection="Flat, each shift ÷ elapsed time at the prefix (floor 1 h)",
        revisits="As flat_distinct, same division",
        focus="Early-process elements (small denominators)",
        pros="Unitless; available at prediction time; de Leoni's proposal",
        cons="Over-corrects toward early prefixes; noisiest on IoT; depends on the floor"),
    "norm_total": dict(
        prefixes="Every prefix of the same executions",
        selection="Flat, each shift ÷ the execution's total duration",
        revisits="As flat_distinct, same division",
        focus="As flat_distinct, rescaled per execution",
        pros="Unitless; keeps the raw ranking",
        cons="Needs the true total duration (future information, offline only); ranking "
             "nearly identical to raw"),
}


def run_dirs(database, task_id):
    return {"last_event": f"files/explainer_outputs/{database}/aggregate_last_event_{task_id}_revisit{REVISIT_N}",
            "exec_mean": run_dir(database, task_id, "execution_mean", REVISIT_N),
            "flat": run_dir(database, task_id, "flat", REVISIT_N),
            "flat_distinct": run_dir(database, task_id, "flat_distinct", REVISIT_N),
            "norm_elapsed": method_dir(database, task_id, "elapsed"),
            "norm_total": method_dir(database, task_id, "total_duration")}


def collect():
    import torch
    import explainer as exp

    for database, cant, expected_task in DATASETS:
        e = exp.Explainer(database, cant)
        e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
        e.model.eval()
        if e.task_id != expected_task:
            raise SystemExit(f"{database}: config.yml points at {e.task_id}, expected {expected_task}")
        print(f"\n=== {database} / {e.task_id} / scope=last_event / revisit_n={REVISIT_N}")
        e.explain_aggregate_shapley(n_traces=N_TRACES, scope='last_event', revisit_n=REVISIT_N,
                                    seed=0, save_dir=run_dirs(database, e.task_id)["last_event"])


def _node_type(label, kind):
    return label.split("=")[0] if kind == "nodes" else label.split(".")[0]


def _jaccard(a, b):
    return len(a & b) / len(a | b)


def _plot_top_grid(bars, kind, title, path):
    fig, axes = plt.subplots(2, 3, figsize=(21, 9.5))
    for ax, (method, name) in zip(axes.flat, METHODS):
        df = bars[method].head(GRID_N)
        vals = df.mean_signed_shift.values
        # A single revisit has std 0 by construction, not "no spread": no whisker for it.
        ns = df["count"].values
        sds = np.where(ns > 1, df.std_shift.values, 0.0)
        lim = float((np.abs(vals) + sds).max()) * 2.3  # room for the "mean ± std (n)" text
        ax.barh(range(len(df)), vals, color=[INCREASE if v > 0 else DECREASE for v in vals],
                xerr=sds, error_kw=dict(ecolor="black", elinewidth=0.8, capsize=2.5, alpha=0.7))
        ax.set_yticks(range(len(df)))
        ax.set_yticklabels([_short(l, 32) for l in df.label], fontsize=8)
        ax.invert_yaxis()
        ax.axvline(0, color="black", linewidth=0.8)
        fmt = "{:+.1f}h" if UNITS[method] == "hours" else "{:+.3f}"
        sd_fmt = "{:.1f}h" if UNITS[method] == "hours" else "{:.3f}"
        for i, (v, sd, n) in enumerate(zip(vals, sds, ns)):
            text = fmt.format(v) + (f" ± {sd_fmt.format(sd)}" if n > 1 else "") + f" (n={n})"
            ax.text(v + (1 if v >= 0 else -1) * (sd + 0.02 * lim), i, text, va="center",
                    ha="left" if v >= 0 else "right", fontsize=7)
        ax.set_xlim(-lim, lim)
        ax.set_title(name, fontsize=11, color=METHOD_COLORS[method], weight="bold")
        ax.set_xlabel(f"Shapley influence ({UNITS[method]})", fontsize=9)
        ax.grid(True, axis="x", alpha=0.3)
    fig.suptitle(f"{title} — top {GRID_N} global Shapley {kind} per method "
                 f"({N_TRACES} executions, ≤{REVISIT_N} revisits per bar; "
                 f"whiskers = ±1 std across revisits)")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def _focus_rows(bars, loo, occ, kind):
    # One row per (method, top-20 label): where in the process and in which executions
    # the method found it.
    rows = []
    for method, _ in METHODS:
        o = occ[method]
        pos = (o.elapsed_h / o.total_h).groupby(o.label).mean()
        pooled = loo[method].set_index("label")
        for rank, label in enumerate(bars[method].head(TOP_N).label, start=1):
            rows.append({"method": method, "rank": rank, "label": label,
                         "node_type": _node_type(label, kind),
                         "position": pos.get(label, np.nan),
                         "n_executions": pooled.n_executions.get(label, np.nan),
                         "max_exec_share": pooled.max_exec_share.get(label, np.nan)})
    return pd.DataFrame(rows)


def _plot_focus(focus, kind, title, path):
    methods = [m for m, _ in METHODS]
    fig, axes = plt.subplots(1, 4, figsize=(22, 5.5), sharey=True,
                             gridspec_kw={"width_ratios": [1, 1, 1, 1.3]})
    panels = [("position", "Where in the process\n(elapsed ÷ total duration, 0 = start, 1 = end)", (0, 1)),
              ("n_executions", f"Across how many executions\n(of {N_TRACES})", (0, N_TRACES + 1)),
              ("max_exec_share", "Concentration\n(share of occurrences in its busiest execution)", (0, 1))]
    rng = np.random.default_rng(0)
    for ax, (col, xlabel, xlim) in zip(axes, panels):
        for i, m in enumerate(methods):
            v = focus[focus.method == m][col].dropna().values
            ax.boxplot([v], positions=[i], vert=False, widths=0.6, patch_artist=True, showfliers=False,
                       boxprops=dict(facecolor=METHOD_COLORS[m], alpha=0.35),
                       medianprops=dict(color="black"))
            ax.scatter(v, i + rng.uniform(-0.18, 0.18, len(v)), s=12, color=METHOD_COLORS[m], zorder=3)
        ax.set_xlim(*xlim)
        ax.set_xlabel(xlabel, fontsize=9)
        ax.grid(True, axis="x", alpha=0.25)
    axes[0].set_yticks(range(len(methods)))
    axes[0].set_yticklabels([METHOD_NAMES[m] for m in methods], fontsize=9)
    axes[0].invert_yaxis()

    mix = focus.groupby(["method", "node_type"]).size().unstack(fill_value=0).reindex(methods)
    mix = mix[mix.sum().sort_values(ascending=False).index]
    left = np.zeros(len(methods))
    cmap = plt.get_cmap("tab10")
    for j, nt in enumerate(mix.columns):
        axes[3].barh(range(len(methods)), mix[nt].values, left=left, color=cmap(j % 10), label=nt)
        left += mix[nt].values
    axes[3].set_xlabel(f"What kind of element\n(node type of the top {TOP_N} labels)", fontsize=9)
    axes[3].legend(fontsize=8, loc="center left", bbox_to_anchor=(1.01, 0.5))
    fig.suptitle(f"{title} — what each method's top {TOP_N} {kind} focus on "
                 f"(one dot per label)")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def _plot_rank_bump(bars, kind, title, path):
    methods = [m for m, _ in METHODS]
    ranks = {m: {l: i for i, l in enumerate(bars[m].head(TOP_N).label, start=1)} for m in methods}
    labels = []
    for m in methods:
        labels += [l for l in bars[m].head(GRID_N).label if l not in labels]
    off = TOP_N + 2  # "not in the top 20" row
    in_all = [l for l in labels if all(l in ranks[m] for m in methods)]
    fig, ax = plt.subplots(figsize=(13, max(7, 0.3 * len(labels) + 2)))
    cmap = plt.get_cmap("tab20")
    rows = []
    for j, l in enumerate(labels):
        y = [ranks[m].get(l, off) for m in methods]
        robust = l in in_all
        ax.plot(range(len(methods)), y, marker="o", linewidth=2.2 if robust else 1,
                alpha=0.95 if robust else 0.45, color=cmap(j % 20),
                label=_short(l, 34) + (" ●" if robust else ""))
        rows.append({"label": l, **{m: ranks[m].get(l) for m in methods}})
    ax.axhline(TOP_N + 1, color="black", linestyle=":", linewidth=0.8)
    ax.set_yticks(list(range(1, TOP_N + 1)) + [off])
    ax.set_yticklabels([str(r) for r in range(1, TOP_N + 1)] + [f">{TOP_N}"], fontsize=8)
    ax.invert_yaxis()
    ax.set_xticks(range(len(methods)))
    ax.set_xticklabels([METHOD_NAMES[m].replace(", ", ",\n") for m in methods], fontsize=8)
    ax.set_ylabel("Shapley rank")
    ax.grid(True, axis="x", alpha=0.25)
    ax.legend(fontsize=7, loc="center left", bbox_to_anchor=(1.01, 0.5),
              title="● = in every method's top 20", title_fontsize=7)
    ax.set_title(f"{title} — rank of every label in any method's top {GRID_N} {kind}", fontsize=10)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return pd.DataFrame(rows)


def _plot_overlap(tops_by_kind, title, path):
    methods = [m for m, _ in METHODS]
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    frames = []
    for ax, (kind, tops) in zip(axes, tops_by_kind.items()):
        mat = pd.DataFrame([[_jaccard(tops[a], tops[b]) for b in methods] for a in methods],
                           index=methods, columns=methods)
        frames.append(mat.assign(kind=kind))
        ax.imshow(mat.values, vmin=0, vmax=1, cmap="Blues")
        for i in range(len(methods)):
            for j in range(len(methods)):
                v = mat.values[i, j]
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=8,
                        color="white" if v > 0.6 else "black")
        ax.set_xticks(range(len(methods)))
        ax.set_xticklabels(methods, rotation=40, ha="right", fontsize=8)
        ax.set_yticks(range(len(methods)))
        ax.set_yticklabels(methods, fontsize=8)
        ax.set_title(f"{kind}", fontsize=10)
    fig.suptitle(f"{title} — top-{TOP_N} overlap between methods (Jaccard)")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return pd.concat(frames)


def _median_cv(bars):
    df = bars.head(TOP_N)
    df = df[df["count"] > 1]
    return float((df.std_shift / df.mean_signed_shift.abs()).median())


def plot():
    measured = {}  # method -> {column: {db: value}}
    for database, _, task_id in DATASETS:
        title = f"{database} / {task_id.replace('TimeFrom_', '').replace('_to_', ' → ')}"
        out = f"files/explainer_outputs/{database}/aggregate_methods_showcase_{task_id}"
        os.makedirs(out, exist_ok=True)
        dirs = run_dirs(database, task_id)
        missing = [d for d in dirs.values() if not os.path.isdir(d)]
        if missing:
            raise SystemExit(f"missing runs (collect them first): {missing}")

        tops_by_kind = {}
        # aggregate_metrics.csv ends with 'mean' and 'std' summary rows.
        n_graphs = {m: int(pd.read_csv(os.path.join(d, "aggregate_metrics.csv")).trace
                           .astype(str).str.isdigit().sum()) for m, d in dirs.items()}
        for kind, bars_csv, loo_csv in KINDS:
            bars = {m: pd.read_csv(os.path.join(d, bars_csv)) for m, d in dirs.items()}
            loo = {m: pd.read_csv(os.path.join(d, loo_csv)) for m, d in dirs.items()}
            # Raw occurrences are identical across the all-prefix runs; only the normalized
            # runs (and the new last-event run) wrote them.
            all_prefix_occ = pd.read_csv(os.path.join(dirs["norm_elapsed"], f"loo_occurrences_{kind}.csv"))
            occ = {m: all_prefix_occ for m, _ in METHODS}
            occ["last_event"] = pd.read_csv(os.path.join(dirs["last_event"], f"loo_occurrences_{kind}.csv"))
            assert set(occ["last_event"].order_id) == set(all_prefix_occ.order_id), \
                f"{database}/{kind}: last-event and all-prefix runs cover different executions"

            _plot_top_grid(bars, kind, title, os.path.join(out, f"top10_grid_{kind}.png"))
            pd.concat({m: b.head(GRID_N) for m, b in bars.items()}, names=["method"]).to_csv(
                os.path.join(out, f"top10_grid_{kind}.csv"))
            focus = _focus_rows(bars, loo, occ, kind)
            focus.to_csv(os.path.join(out, f"focus_profile_{kind}.csv"), index=False)
            _plot_focus(focus, kind, title, os.path.join(out, f"focus_profile_{kind}.png"))
            _plot_rank_bump(bars, kind, title, os.path.join(out, f"rank_bump_{kind}.png")).to_csv(
                os.path.join(out, f"rank_bump_{kind}.csv"), index=False)
            tops_by_kind[kind] = {m: set(b.head(TOP_N).label) for m, b in bars.items()}

            if kind == "nodes":
                med = focus.groupby("method")[["position", "n_executions"]].median()
                for m, _ in METHODS:
                    col = measured.setdefault(m, {})
                    col.setdefault("graphs", {})[database] = n_graphs[m]
                    col.setdefault("overlap", {})[database] = _jaccard(tops_by_kind[kind][m],
                                                                       tops_by_kind[kind]["last_event"])
                    col.setdefault("position", {})[database] = med.position[m]
                    col.setdefault("n_exec", {})[database] = med.n_executions[m]
                    col.setdefault("cv", {})[database] = _median_cv(bars[m])

        _plot_overlap(tops_by_kind, title, os.path.join(out, "overlap_heatmap.png")).to_csv(
            os.path.join(out, "overlap_heatmap.csv"))
        print(f"{title}  ->  {out}")

    _write_table(measured)


def _write_table(measured):
    dbs = [d for d, _, _ in DATASETS]
    rows = []
    for m, name in METHODS:
        d = DESCRIPTIONS[m]
        rows.append({"Method": f"{m}\n({name})", "Prefixes explained": d["prefixes"],
                     "LOO selection (top 20)": d["selection"], "Shapley revisits": d["revisits"],
                     "Units": UNITS[m], "Focuses on": d["focus"], "Pros": d["pros"], "Cons": d["cons"],
                     "Graphs": pair_fmt(measured, m, "graphs", "{:d}", dbs),
                     "Top-20 overlap w/ last_event": pair_fmt(measured, m, "overlap", "{:.2f}", dbs),
                     "Median position (0 start, 1 end)": pair_fmt(measured, m, "position", "{:.2f}", dbs),
                     "Median executions": pair_fmt(measured, m, "n_exec", "{:.0f}", dbs),
                     "Median CV": pair_fmt(measured, m, "cv", "{:.2f}", dbs)})
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(TABLE_DIR, "global_shapley_methods_table.csv"), index=False)

    widths = {"Method": 18, "Prefixes explained": 18, "LOO selection (top 20)": 20,
              "Shapley revisits": 18, "Units": 8, "Focuses on": 20, "Pros": 26, "Cons": 26,
              "Graphs": 10, "Top-20 overlap w/ last_event": 11, "Median position (0 start, 1 end)": 11,
              "Median executions": 10, "Median CV": 10}
    head = [textwrap.fill(c, widths[c]) for c in df.columns]
    cells = [[textwrap.fill(str(v), widths[c]) for c, v in r.items()] for _, r in df.iterrows()]
    fig, ax = plt.subplots(figsize=(30, 8))
    ax.axis("off")
    short = " / ".join(DB_SHORT[d] for d in dbs)
    ax.set_title(f"Global Shapley variants — how they work, trade-offs, and measured values "
                 f"({short}; nodes; {N_TRACES} executions, ≤{REVISIT_N} revisits)", fontsize=13, pad=40)
    table = ax.table(cellText=cells, colLabels=head, loc="center", cellLoc="left",
                     colWidths=[widths[c] / sum(widths.values()) for c in df.columns])
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    n_lines = max(max(s.count("\n") for s in row) for row in cells) + 1
    for (r, c), cell in table.get_celld().items():
        cell.set_height((max(h.count("\n") for h in head) + 1) * 0.03 if r == 0 else n_lines * 0.03)
        if r == 0:
            cell.set_facecolor("#EAEAEA")
            cell.set_text_props(weight="bold")
        else:
            cell.set_facecolor("#FFFFFF" if r % 2 else "#F5F5F5")
            if c == 0:
                cell.set_text_props(weight="bold", color=METHOD_COLORS[METHODS[r - 1][0]])
    plt.savefig(os.path.join(TABLE_DIR, "global_shapley_methods_table.png"), dpi=150, bbox_inches="tight")
    plt.close()

    esc = lambda s: (str(s).replace("\n", " ").replace("_", r"\_").replace("%", r"\%")
                     .replace("≤", r"$\leq$").replace("÷", r"$\div$"))
    spec = "p{2.2cm}" + "p{2.4cm}" * 7 + "p{1.3cm}" * 5
    lines = [r"\begin{tabular}{" + spec + "}", r"\hline",
             " & ".join(r"\textbf{" + esc(c) + "}" for c in df.columns) + r" \\", r"\hline"]
    lines += [" & ".join(esc(v) for v in r) + r" \\" for r in df.itertuples(index=False)]
    lines += [r"\hline", r"\end{tabular}"]
    with open(os.path.join(TABLE_DIR, "global_shapley_methods_table.tex"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print(df.drop(columns=["Prefixes explained", "LOO selection (top 20)", "Shapley revisits",
                           "Focuses on", "Pros", "Cons"]).to_string(index=False))


def pair_fmt(measured, method, col, fmt, dbs):
    return " / ".join(fmt.format(int(v) if fmt == "{:d}" else v) for v in
                      (measured[method][col][d] for d in dbs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true",
                        help="run the last-event scope per dataset first, then plot")
    args = parser.parse_args()
    if args.collect:
        collect()
    plot()
