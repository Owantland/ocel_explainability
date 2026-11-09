"""Shapley bar chart with ±1 std whiskers for the dashboard.

Ports the single-panel style of _plot_top_grid() in
thesis_scripts/global_shapley_methods_showcase.py (whiskers, "mean ± std (n)" labels,
symmetric axis, rows ranked by |mean|), with the sign colours of the thesis single-trace
figure loo_shapley_trace_showcase.png (red = pushes the prediction later, blue = earlier).
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

LATER, EARLIER = "#e15759", "#4e79a7"


def plot_shapley_bars(labels, means, stds, ns, path, title, xlabel="Signed Shapley shift (hours)",
                      show_n=True):
    """labels/means/stds/ns are parallel sequences, means and stds in hours. Rows are drawn
    ranked by |mean| from the top. A bar with n <= 1 gets no whisker: its std is 0 by
    construction, not "no spread". show_n=False drops the per-bar "(n=N)" (used when every
    bar shares the same n, which the title then states)."""
    order = np.argsort(-np.abs(np.asarray(means, dtype=float)), kind="stable")
    labels = [labels[i] for i in order]
    vals = np.asarray(means, dtype=float)[order]
    ns = np.asarray(ns)[order]
    sds = np.where(ns > 1, np.nan_to_num(np.asarray(stds, dtype=float)[order]), 0.0)

    fig, ax = plt.subplots(figsize=(9, 0.45 * len(labels) + 1.6))
    lim = float((np.abs(vals) + sds).max()) * 2.3 if len(vals) else 1.0
    lim = lim or 1.0
    ax.barh(range(len(vals)), vals, color=[LATER if v > 0 else EARLIER for v in vals],
            xerr=sds, error_kw=dict(ecolor="black", elinewidth=0.8, capsize=2.5, alpha=0.7))
    ax.set_yticks(range(len(vals)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.axvline(0, color="black", linewidth=0.8)
    for i, (v, sd, n) in enumerate(zip(vals, sds, ns)):
        text = f"{v:+.1f}h" + (f" ± {sd:.1f}h" if n > 1 else "") + (f" (n={n})" if show_n else "")
        ax.text(v + (1 if v >= 0 else -1) * (sd + 0.02 * lim), i, text, va="center",
                ha="left" if v >= 0 else "right", fontsize=8)
    ax.set_xlim(-lim, lim)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.grid(True, axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
