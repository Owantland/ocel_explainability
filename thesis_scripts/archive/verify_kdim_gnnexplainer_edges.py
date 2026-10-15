"""Verifies whether GNNExplainer can produce real edge importance on the k-dim GNN
baseline (to_hetero()-wrapped KDimGNN), where it structurally cannot on HGT.

Background (explainer.py:3191-3213, _get_gnn_explainer()'s docstring): GNNExplainer's
edge-mask injection (PyG's set_hetero_masks) walks model.modules() looking for an
nn.ModuleDict of per-relation MessagePassing submodules -- the layout a to_hetero()-
wrapped model has. HGTConv (model_classes/HGT.py) is a single fused MessagePassing
module instead, so set_hetero_masks finds nothing to patch and edge_mask_type='object'
raises "Could not compute gradients for edge masks" on HGT. Since the k-dim GNN baseline
is itself to_hetero()-wrapped (see model_classes/KDIM_GNN.py, training.KDim_Reg_Modelling),
it SHOULD produce the ModuleDict layout set_hetero_masks expects -- this script checks
whether that expectation actually holds, rather than assuming it.

This is additive verification, not a blocker for the rest of P4's baseline-comparison
work -- a fail here is itself a useful, documented result either way.

Usage: python3 verify_kdim_gnnexplainer_edges.py [database] [cant]
  (defaults to order_management/2000; pass e.g. logistics 1000 to test against
  whichever KPI files/config.yml currently points logistics at)
"""
import sys

import numpy as np
import torch
import torch_geometric.nn as pygnn
from torch_geometric.explain import Explainer as PyGExplainer, GNNExplainer

import training as t
from model_classes import KDIM_GNN

DATABASE = sys.argv[1] if len(sys.argv) > 1 else 'order_management'
CANT = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
N_TEST_GRAPHS = 5


class _ViewpointWrapper(torch.nn.Module):
    """Thin wrapper so the to_hetero()-wrapped k-dim GNN presents the same
    single-tensor-output interface HGT's own forward() already does
    (self.lin(x_dict[self.viewpoint])) -- matches the exact pattern
    _get_gnn_explainer() already works against for node masking, so this script
    isolates the one thing actually being tested (edge masks), not a second unknown
    (whether Explainer accepts dict-shaped model output)."""

    def __init__(self, kdim_hetero_model, viewpoint):
        super().__init__()
        self.kdim_hetero_model = kdim_hetero_model
        self.viewpoint = viewpoint

    def forward(self, x_dict, edge_index_dict):
        return self.kdim_hetero_model(x_dict, edge_index_dict)[self.viewpoint]


def load_kdim_model(m):
    vp = m.kpi_viewpoint
    kdim_model_path = m.model_path.replace(".pth", "_kdim.pth")

    base_model = KDIM_GNN.KDimGNN(
        hidden_channels=m.params.get('hidden_channels', 32),
        out_channels=1,
        num_layers=m.params.get('num_layers', 2),
    )
    kdim_model = pygnn.to_hetero(base_model, m.train_data[0].metadata()).to(m.device)
    with torch.no_grad():
        g0 = m.test_data[0].to(m.device)
        kdim_model(g0.x_dict, g0.edge_index_dict)
    kdim_model.load_state_dict(torch.load(kdim_model_path, weights_only=False))
    kdim_model.eval()
    return _ViewpointWrapper(kdim_model, vp)


def pick_test_graphs(m, n):
    """Skip graphs with an empty edge type -- same guard as
    explainer.py's _check_gnn_explainer_edges, since GNNExplainer's
    _initialize_masks() crashes on an empty relation regardless of architecture,
    which would be a false negative for what this script is actually testing."""
    picked = []
    for g in m.test_data:
        if any(g[et].edge_index.size(1) == 0 for et in g.edge_types):
            continue
        picked.append(g)
        if len(picked) >= n:
            break
    return picked


def main():
    m = t.Modelling(DATABASE, CANT)

    def fresh_explainer():
        # Rebuild BOTH the model and the explainer fresh per graph -- set_hetero_masks
        # attaches its edge-mask parameters directly onto the model's own conv
        # submodules (not the explainer object), and PyG only tears them back down via
        # clear_masks() on a successful completion. A failed call (e.g. graph 0's
        # ValueError, raised mid-training) leaves those graph-0-sized masks attached,
        # which then breaks graph 1's differently-sized edge_index with an unrelated
        # AssertionError -- confirmed empirically: a fresh *explainer* alone wasn't
        # enough, the underlying model object needed to be fresh too.
        wrapper = load_kdim_model(m)
        return wrapper, PyGExplainer(
            model=wrapper,
            algorithm=GNNExplainer(epochs=200, lr=0.01),
            explanation_type='model',
            node_mask_type='attributes',
            edge_mask_type='object',
            model_config=dict(mode='regression', task_level='node', return_type='raw'),
        )

    graphs = pick_test_graphs(m, N_TEST_GRAPHS)
    print(f"Testing edge_mask_type='object' against the k-dim GNN on {len(graphs)} "
          f"test graphs ({DATABASE}/{m.task_id}, num_layers={m.params.get('num_layers')})...\n")

    n_pass, n_fail_known, n_fail_other = 0, 0, 0
    for i, g in enumerate(graphs):
        g = g.to(m.device)
        sizes = {et: g[et].edge_index.size(1) for et in g.edge_types}
        print(f"[graph {i}] edge type sizes: {sizes}")
        torch.manual_seed(42)
        _, gnn_explainer = fresh_explainer()
        try:
            explanation = gnn_explainer(g.x_dict, g.edge_index_dict, index=0)
        except (RuntimeError, ValueError) as e:
            if "Could not compute gradients for edge masks" in str(e):
                print(f"[graph {i}] FAIL (same known HGT failure mode): {e}")
                n_fail_known += 1
            else:
                print(f"[graph {i}] FAIL (different, unexpected error): {e}")
                n_fail_other += 1
            continue

        try:
            raw_edge_mask_dict = explanation.edge_mask_dict
        except KeyError:
            print(f"[graph {i}] FAIL (no edge_mask_dict on the returned explanation "
                  f"-- edge_mask_type='object' silently produced nothing)")
            n_fail_other += 1
            continue

        n_pass += 1
        print(f"[graph {i}] PASS -- per-relation edge mask stats:")
        for et, mask in raw_edge_mask_dict.items():
            mask_np = mask.detach().cpu().numpy()
            nonzero_frac = float((np.abs(mask_np) > 1e-6).mean()) if mask_np.size else float('nan')
            print(f"    {et}: n={mask_np.size:4d}  mean={mask_np.mean():.4f}  "
                  f"std={mask_np.std():.4f}  nonzero_frac={nonzero_frac:.3f}")

    print(f"\nSummary: {n_pass} pass / {n_fail_known} fail (known HGT-style error) / "
          f"{n_fail_other} fail (other) out of {len(graphs)} graphs tested.")
    if n_pass == len(graphs) and len(graphs) > 0:
        print("CONCLUSION: the k-dim GNN's to_hetero() layout DOES let GNNExplainer "
              "produce real edge importance, unlike HGT.")
    elif n_fail_known == len(graphs) and len(graphs) > 0:
        print("CONCLUSION: the k-dim GNN hits the SAME edge-mask failure as HGT -- "
              "to_hetero() wrapping alone was not sufficient here.")
    else:
        print("CONCLUSION: mixed/inconclusive result -- inspect the per-graph output above "
              "before drawing a conclusion either way.")


if __name__ == '__main__':
    main()
