"""Explanation-subgraph showcase, one worked example per KPI (4 total) -- composes the
ALREADY-EXISTING explanation_subgraph.png images (a side effect of every
explain_trace_shapley() call this session) into one 4-panel figure, matching the style
of every other showcase this session. Pure image composition -- no new explainer
computation, no config switching, nothing recomputed.

Source images (same 4 traces used by every other showcase, generated 2026-07-26 during
the edge-showcase --collect runs):
  files/explainer_outputs/order_management/order_1801_shapley/explanation_subgraph.png
  files/explainer_outputs/order_management/order_1935_shapley/explanation_subgraph.png
  files/explainer_outputs/logistics/order_959_shapley/explanation_subgraph.png
  files/explainer_outputs/logistics/order_911_shapley/explanation_subgraph.png

Each image already has a generic baked-in title ("LOO Explanation Subgraph
(Regression)") and its own type-color legend positioned near the top -- checked
directly (non-white pixels extend through the whole top ~300px band on both node/edge
axes), so a naive top-pixel crop to remove the title risks clipping the legend too.
Rather than a fragile crop, this just adds the usual "[Dataset] / [Object] -> [KPI
Event]" + order_id/true/pred panel title ABOVE the full, uncropped image -- a small
redundancy with the embedded generic title, not a risk of cutting real content.

Usage: python3 loo_shapley_subgraph_showcase.py
"""
import json
import os
import shutil

import matplotlib.image as mpimg
import matplotlib.pyplot as plt

OUT_DIR = "thesis_parts/figures_tables"
BUILD_DIR = "thesis_parts/latex_template/figures"
NODE_DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_trace_showcase_data.json")
PNG_PATH = os.path.join(OUT_DIR, "loo_shapley_subgraph_showcase.png")
# Exact filename results.tex actually cites (figures/single_subgraph.png) -- this
# used to be a manual one-time rename with no re-sync path; promoted automatically
# now, same pattern as promote_aggregate_shapley_global_figures.py.
BUILD_PNG_PATH = os.path.join(BUILD_DIR, "single_subgraph.png")

# (kpi_label, database) -- order_id/true/pred come from loo_shapley_trace_showcase_data.json
KPI_SOURCES = [
    ("Order Management / Orders -> PayOrder", "order_management"),
    ("Order Management / Orders -> PackageDelivered", "order_management"),
    ("Logistics / CustomerOrder -> Depart", "logistics"),
    ("Logistics / CustomerOrder -> LoadToVehicle", "logistics"),
]


def main():
    with open(NODE_DATA_PATH) as f:
        node_data = json.load(f)

    fig, axes = plt.subplots(2, 2, figsize=(16, 13))
    axes = axes.flatten()
    for ax, (kpi_label, database) in zip(axes, KPI_SOURCES):
        entry = node_data[kpi_label]
        order_id = entry['order_id']
        img_path = f"files/explainer_outputs/{database}/order_{order_id}_shapley/explanation_subgraph.png"
        img = mpimg.imread(img_path)
        ax.imshow(img)
        ax.axis('off')
        ax.set_title(
            f"{kpi_label}\norder_id={order_id}  true={entry['true_hours']:.1f}h  "
            f"pred={entry['predicted_hours']:.1f}h",
            fontsize=11)

    fig.suptitle("LOO-identified, Shapley-quantified explanation subgraphs — one worked "
                 "example per KPI\n(node color = type, bold border = viewpoint node, "
                 "edges unweighted)", fontsize=13)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    os.makedirs(OUT_DIR, exist_ok=True)
    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {PNG_PATH}")

    os.makedirs(BUILD_DIR, exist_ok=True)
    shutil.copy(PNG_PATH, BUILD_PNG_PATH)
    print(f"Promoted -> {BUILD_PNG_PATH}")


if __name__ == '__main__':
    main()
