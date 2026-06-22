import torch
from torch_geometric.explain import Explainer, Explanation
from torch_geometric.explain import GNNExplainer
from torch_geometric.explain.metric import (
    fidelity,
    characterization_score,
    fidelity_curve_auc,
    unfaithfulness
)
import sup_funcs as sf
import os
import pandas as pd
from model_classes import REG_GAT, REG_GNN

class ModelExplainer:
    def __init__(self, database, cant, std, mean):
        self.database = database
        self.cant = cant
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.std = std
        self.mean = mean

    def decode_epoch(self, epoch_val):
        timestamp = pd.Timestamp(epoch_val, unit='s')
        return timestamp

    @torch.no_grad()
    def _masked_prediction(self, data, model, target_std, target_mean, device=torch.device('cpu'),
                           node_mask=None, edge_mask=None):
        """
        Run the model with the node/edge masks applied continuously (soft
        masking) rather than hard-removing nodes/edges. This avoids having to
        renumber edge_index or rebuild a subgraph -- we just scale each node's
        features and each edge's contribution by its importance in [0, 1].
        """
        data = data.to(device)
        x = data.x
        if node_mask is not None:
            x = x * node_mask
        edge_weight = edge_mask if edge_mask is not None else None
        batch = torch.zeros(data.num_nodes, dtype=torch.long, device=device)
        out = model(x, data.edge_index, batch, edge_weight=edge_weight)
        return out * target_std.to(device) + target_mean.to(device)

    def evaluate_explanation_quality(self, data, explanation, model, verbose):
        """
        Quantify how good a GNNExplainer explanation actually is.

        PyG ships built-in `fidelity()` / `unfaithfulness()` metrics in
        `torch_geometric.explain.metric`, but as of this writing they only
        support classification models internally (they rely on predicted class
        labels) and raise `ValueError: Fidelity not defined for 'regression'
        models` if you call them on a regression model like ours. So instead we
        reimplement the same underlying idea ourselves, directly on raw
        (de-normalized) predictions, using soft masking:

          - Fidelity+ (higher = better): zero out the important nodes/edges and
            keep everything else. If the explanation is right, the prediction
            should change A LOT, since the model actually relied on that part.
          - Fidelity- (lower = better): keep ONLY the important nodes/edges and
            zero out the rest. If the explanation is sufficient on its own, the
            prediction should stay close to the original.

        Both are reported in the target's original units (e.g. Debye for dipole
        moment), so they're directly interpretable -- "removing what the
        explanation flagged shifted the prediction by 0.8 Debye."
        """
        target_std = self.std
        target_mean = self.mean
        node_mask = explanation.node_mask  # [num_nodes, num_features], in [0, 1]
        edge_mask = explanation.edge_mask  # [num_edges], in [0, 1]

        y_original = self._masked_prediction(data, model, target_std, target_mean)  # no masking = full graph
        # Complement: remove (zero out) the important parts, keep the rest.
        y_complement = self._masked_prediction(data, model, target_std, target_mean,
                                               node_mask=1 - node_mask, edge_mask=1 - edge_mask)
        # Subgraph: keep ONLY the important parts, zero out the rest.
        y_subgraph = self._masked_prediction(data, model, target_std, target_mean,
                                             node_mask=node_mask, edge_mask=edge_mask)

        fidelity_plus = (y_original - y_complement).abs().item()
        fidelity_minus = (y_original - y_subgraph).abs().item()

        # A simple bounded combined score: what share of the "damage from
        # removing important stuff" is NOT also caused by "damage from removing
        # unimportant stuff"? 1.0 = ideal (all damage comes from the important
        # part); 0.0 = the explanation is no better than random.
        denom = fidelity_plus + fidelity_minus
        characterization_score = fidelity_plus / denom if denom > 1e-8 else 0.0

        # Sparsity: what fraction of nodes/edges were NOT flagged as important
        # (mask value below a 0.5 threshold)? Higher = more compact explanation.
        edge_sparsity = (explanation.edge_mask < 0.5).float().mean().item()
        node_sparsity = (explanation.node_mask.sum(dim=-1) < 0.5).float().mean().item()

        metrics = {
            "fidelity_plus": fidelity_plus,
            "fidelity_minus": fidelity_minus,
            "characterization_score": characterization_score,
            "edge_sparsity": edge_sparsity,
            "node_sparsity": node_sparsity,
        }

        if verbose:
            print("\n--- Explanation quality metrics ---")
            print(
                f"  Fidelity+        : {fidelity_plus:.4f}  (higher is better -- removing the important part should hurt)")
            print(
                f"  Fidelity-        : {fidelity_minus:.4f}  (lower is better -- the important part alone should be enough)")
            print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
            print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of edges marked unimportant)")
            print(f"  Node sparsity    : {node_sparsity:.2%}  (share of nodes marked unimportant)")

        return metrics

    def explain_regression(self, model, index=15, topk=5):
        # Load test data for the explainer
        file_path = self.path_dict['pytorch_path']
        test_data = torch.load(f"{file_path}/test_graphs_hom.pt", weights_only=False)
        device = torch.device('cpu')

        data = test_data[index]
        print(data.vwpnt_id.item())
        print(self.decode_epoch((data.timestamp).item()))
        batch = torch.zeros(data.num_nodes, dtype=torch.long, device=device)

        model_explainer = Explainer(
            model=model,
            algorithm=GNNExplainer(epochs=200),
            explanation_type="model",  # explain the model's own prediction
            node_mask_type="attributes",  # learn importance per node feature
            edge_mask_type="object",  # learn importance per edge
            model_config=dict(
                mode="regression",
                task_level="graph",
                return_type="raw",
            ),
        )
        model_explanation = model_explainer(data.x, data.edge_index, batch=batch)
        subgraph = model_explanation.get_explanation_subgraph()
        complement_subgraph = model_explanation.get_complement_subgraph()

        explainer_path = f"{self.path_dict['explainer_path']}TimeUntil_{self.path_dict['kpi_event']}"
        model_explanation.visualize_feature_importance(f"{explainer_path}_top{topk}.png", top_k=topk)
        model_explanation.visualize_graph(f'{explainer_path}_graph.png', backend="graphviz")

        self.evaluate_explanation_quality(data, model_explanation, model, verbose=True)

    def explain_model(self, index=15, topk=5):
        """
        Main function for explaining a given model. Identifies if the model is a regression or a classification and
        runs the appropriate function to generate the explanation and give some results.
        :return:
        """

        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        num_node_features = 11 if self.database == 'order_management' else 14

        if self.path_dict['kpi_type'] == 0:
            model_path = f"{model_path}/TimeUntil_{kpi_event}.pth"
            model = REG_GNN.REG_GNN(in_channels=num_node_features, hidden_channels=64, num_layers=3)
            # model = REG_GAT.REG_GAT(in_channels=num_node_features, hidden_channels=64, num_layers=3)
            model.load_state_dict(torch.load(model_path))
            self.explain_regression(model, index=index, topk=topk)

        elif self.path_dict['kpi_type'] == 1:
            model_path = f"{model_path}/BinaryClass_{kpi_event}.pth"

