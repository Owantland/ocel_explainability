"""Single-trace EDGE-importance showcase, one worked example per KPI (4 total) --
the edge-focused sibling of loo_shapley_trace_showcase.py (which covers NODE
importance for the same 4 KPIs). Same panel layout/convention ("[Dataset] /
[Object] -> [KPI Event]", red = pushes prediction later, blue = pushes prediction
earlier), but tornado-bars edges (specific node-pair relations) instead of nodes.

Reuses the EXACT SAME 4 order_ids already picked for the node showcase (read from
loo_shapley_trace_showcase_data.json) rather than re-deriving a fresh "slow" trace
per KPI -- gives a coherent "same real example, two lenses" pairing with the
existing node figure.

explain_trace_shapley()'s returned dict already includes edge_importances (unused
by the node showcase) -- (edge_type, edge_idx, shift, large, signed_shift) tuples,
edge_type being PyG's (src_node_type, relation, dst_node_type) triple, edge_idx a
column index into that edge type's edge_index tensor. Unlike nodes (flat top_k
unioned with each node type's own top 3), edges have NO per-type expansion -- pass
top_k=8 directly (vs. the node showcase's top_k=5) to get 8 edges outright.

explain_trace_shapley() does not return the explain_subgraph/id_map needed to
decode an edge's two endpoints into a human-readable label -- rather than editing
that (heavily-used) method, this script just calls the same two already-public
helpers again itself: _locate_test_graph() (rebuilds the same subgraph) and
_decode_all_identifiers() (same id_map) -- both cheap lookups, no extra model
inference.

Usage:
    python3 loo_shapley_edge_trace_showcase.py --collect "Order Management / Orders -> PayOrder" --database order_management --cant 2000
    python3 loo_shapley_edge_trace_showcase.py --collect "Logistics / CustomerOrder -> LoadToVehicle" --database logistics --cant 1000
    # (switch config.yml, regenerate graphs for PackageDelivered/Depart as needed, then:)
    python3 loo_shapley_edge_trace_showcase.py --collect "Order Management / Orders -> PackageDelivered" --database order_management --cant 2000
    python3 loo_shapley_edge_trace_showcase.py --collect "Logistics / CustomerOrder -> Depart" --database logistics --cant 1000
    python3 loo_shapley_edge_trace_showcase.py --render
"""
import argparse
import json
import os

import matplotlib.pyplot as plt
import torch

import explainer as exp

OUT_DIR = "thesis_parts/figures_tables"
NODE_DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_trace_showcase_data.json")
DATA_PATH = os.path.join(OUT_DIR, "loo_shapley_edge_trace_showcase_data.json")
PNG_PATH = os.path.join(OUT_DIR, "loo_shapley_edge_trace_showcase.png")

KPI_ORDER = [
    "Order Management / Orders -> PayOrder",
    "Order Management / Orders -> PackageDelivered",
    "Logistics / CustomerOrder -> Depart",
    "Logistics / CustomerOrder -> LoadToVehicle",
]

TOP_N = 8


def _idx_label(id_map, node_type, idx):
    return id_map.get((node_type, idx), f"{node_type}#{idx}")


def collect(kpi_label, database, cant):
    with open(NODE_DATA_PATH) as f:
        node_data = json.load(f)
    if kpi_label not in node_data:
        raise SystemExit(f"{kpi_label!r} not found in {NODE_DATA_PATH} -- run the node "
                          f"showcase's --collect for this KPI first.")
    order_id = node_data[kpi_label]['order_id']
    print(f"Reusing order_id={order_id} from the node showcase for {kpi_label!r}")

    e = exp.Explainer(database, cant)
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    print(f"task_id: {e.task_id}")

    result = e.explain_trace_shapley(order_id, top_k=TOP_N, n_samples=100)

    explain_subgraph = e._locate_test_graph(order_id, None)
    id_map = e._decode_all_identifiers(explain_subgraph, order_id, None)

    rows = []
    for et, edge_idx, shift, large, signed_shift in result['edge_importances'][:TOP_N]:
        src, dst = explain_subgraph[et].edge_index[:, edge_idx].tolist()
        src_label = _idx_label(id_map, et[0], src)
        dst_label = _idx_label(id_map, et[2], dst)
        label = f"{src_label} -[{et[1]}]-> {dst_label}"
        rows.append({'label': label, 'signed_shift_hours': signed_shift / 3600.0})

    entry = {
        'kpi_label': kpi_label,
        'order_id': order_id,
        'true_hours': node_data[kpi_label]['true_hours'],
        'predicted_hours': result['predicted_hours'],
        'rows': rows,
    }

    all_data = {}
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH) as f:
            all_data = json.load(f)
    all_data[kpi_label] = entry
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(DATA_PATH, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"Saved collected result for {kpi_label!r} to {DATA_PATH}")


def render():
    with open(DATA_PATH) as f:
        all_data = json.load(f)
    missing = [k for k in KPI_ORDER if k not in all_data]
    if missing:
        raise SystemExit(f"Missing collected data for: {missing}. Run --collect for each first.")

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    axes = axes.flatten()
    for ax, kpi_label in zip(axes, KPI_ORDER):
        entry = all_data[kpi_label]
        rows = sorted(entry['rows'], key=lambda r: r['signed_shift_hours'])
        labels = [r['label'] for r in rows]
        values = [r['signed_shift_hours'] for r in rows]
        colors = ['#e15759' if v > 0 else '#4e79a7' for v in values]

        ax.barh(range(len(labels)), values, color=colors)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=8)
        ax.axvline(0, color='black', linewidth=0.8)
        ax.set_xlabel("Signed Shapley shift (hours)")
        ax.set_title(
            f"{kpi_label}\norder_id={entry['order_id']}  "
            f"true={entry['true_hours']:.1f}h  pred={entry['predicted_hours']:.1f}h",
            fontsize=10)
        ax.grid(True, axis='x', alpha=0.3)

    fig.suptitle("LOO-identified, Shapley-quantified single-trace EDGE explanation — one "
                 "worked example per KPI\n(red = pushes prediction later, blue = pushes "
                 "prediction earlier)", fontsize=12)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(PNG_PATH, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {PNG_PATH}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect', metavar='KPI_LABEL')
    parser.add_argument('--database', choices=['order_management', 'logistics'])
    parser.add_argument('--cant', type=int)
    parser.add_argument('--render', action='store_true')
    args = parser.parse_args()

    if args.collect:
        if not args.database or not args.cant:
            raise SystemExit("--collect requires --database and --cant")
        collect(args.collect, args.database, args.cant)
    elif args.render:
        render()
    else:
        raise SystemExit("Pass --collect \"<KPI label>\" --database ... --cant ... or --render")


if __name__ == '__main__':
    main()
