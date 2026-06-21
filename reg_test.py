"""
Graph Regression with PyTorch Geometric (PyG)
==============================================

This example shows how to train a Graph Neural Network (GNN) to perform
*graph-level regression* on a homogeneous graph dataset.

We use the QM9 dataset (small molecules), which is a classic graph
regression benchmark: each graph (molecule) has a single continuous
target value to predict (here, we pick one of QM9's quantum-chemical
properties).

Pipeline:
    1. Load the dataset and normalize the regression target.
    2. Split into train / val / test sets and wrap them in DataLoaders.
    3. Define a GNN (GCN layers + global pooling + MLP head).
    4. Train with MSE loss, validate, and report test MAE.

Install requirements (if not already installed):
    pip install torch torch_geometric --break-system-packages
"""

import torch
import torch.nn.functional as F
from torch.nn import Linear
from torch_geometric.datasets import QM9
from torch_geometric.loader import DataLoader
from torch_geometric.nn import GCNConv, global_mean_pool
from torch_geometric.explain import Explainer, GNNExplainer
from torch_geometric.explain.metric import (
   fidelity,
   characterization_score,
   fidelity_curve_auc,
)

# ---------------------------------------------------------------------------
# 1. Load dataset
# ---------------------------------------------------------------------------
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

dataset = QM9(root="data/QM9")

# QM9 graphs have 19 regression targets in `data.y`. We'll predict a single
# property — index 0, which is "dipole moment" (mu) — by slicing it out.
TARGET_IDX = 0

# Normalize the target (helps training stability). Compute mean/std once.
ys = torch.cat([d.y[:, TARGET_IDX] for d in dataset])
target_mean, target_std = ys.mean(), ys.std()


def normalize_target(data):
    data.y = (data.y[:, TARGET_IDX] - target_mean) / target_std
    return data


dataset = [normalize_target(d) for d in dataset]

# ---------------------------------------------------------------------------
# 2. Train / val / test split
# ---------------------------------------------------------------------------
torch.manual_seed(42)
dataset = dataset[: 20000]  # subsample for a quick example; remove for full run
perm = torch.randperm(len(dataset))
dataset = [dataset[i] for i in perm]

n_train = int(0.8 * len(dataset))
n_val = int(0.1 * len(dataset))

train_dataset = dataset[:n_train]
val_dataset = dataset[n_train: n_train + n_val]
test_dataset = dataset[n_train + n_val:]

train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=64)
test_loader = DataLoader(test_dataset, batch_size=64)


# ---------------------------------------------------------------------------
# 3. Define the GNN model
# ---------------------------------------------------------------------------
class GNNRegressor(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels=64, num_layers=3):
        super().__init__()
        self.convs = torch.nn.ModuleList()
        self.convs.append(GCNConv(in_channels, hidden_channels))
        for _ in range(num_layers - 1):
            self.convs.append(GCNConv(hidden_channels, hidden_channels))

        self.lin1 = Linear(hidden_channels, hidden_channels)
        self.lin2 = Linear(hidden_channels, 1)  # single scalar output

    def forward(self, x, edge_index, batch):
        # Node-level message passing
        for conv in self.convs:
            x = conv(x, edge_index)
            x = F.relu(x)

        # Pool node embeddings -> one vector per graph
        x = global_mean_pool(x, batch)

        # Graph-level MLP head -> scalar regression target
        x = F.relu(self.lin1(x))
        x = self.lin2(x)
        return x.squeeze(-1)


model = GNNRegressor(in_channels=dataset[0].num_node_features).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)


# ---------------------------------------------------------------------------
# 4. Train / evaluate
# ---------------------------------------------------------------------------
def train_one_epoch():
    model.train()
    total_loss = 0
    for data in train_loader:
        data = data.to(device)
        optimizer.zero_grad()
        out = model(data.x, data.edge_index, data.batch)
        loss = F.mse_loss(out, data.y)
        loss.backward()
        optimizer.step()
        total_loss += loss.item() * data.num_graphs
    return total_loss / len(train_dataset)


@torch.no_grad()
def evaluate(loader):
    model.eval()
    total_mae = 0
    for data in loader:
        data = data.to(device)
        out = model(data.x, data.edge_index, data.batch)
        # De-normalize before computing MAE so it's in original units
        pred = out * target_std.to(device) + target_mean.to(device)
        true = data.y * target_std.to(device) + target_mean.to(device)
        total_mae += (pred - true).abs().sum().item()
    return total_mae / len(loader.dataset)


@torch.no_grad()
def _predict_denormalized(data):
    """Helper: run the model on a single graph and return the de-normalized
    prediction (matches the units used during evaluation)."""
    data = data.to(device)
    out = model(data.x, data.edge_index, data.batch)
    return (out * target_std.to(device) + target_mean.to(device)).item()


def explain_single_graph(data, save_path=None):
    """
    Use PyG's Explainer + GNNExplainer to find which nodes, edges, and node
    features were most responsible for the model's prediction on a single
    graph.

    This trains a small per-graph mask-optimization (no need to retrain the
    GNN) that learns soft masks over edges and node features which, when
    applied, best preserve the model's original output. High mask values =
    high importance.
    """
    model.eval()
    data = data.to(device)

    # Single-graph Data objects don't come with a `batch` vector by default;
    # the model's global_mean_pool needs one (all zeros = "one graph").
    batch = torch.zeros(data.num_nodes, dtype=torch.long, device=device)

    explainer = Explainer(
        model=model,
        algorithm=GNNExplainer(epochs=200),
        explanation_type="model",      # explain the model's own prediction
        node_mask_type="attributes",   # learn importance per node feature
        edge_mask_type="object",       # learn importance per edge
        model_config=dict(
            mode="regression",
            task_level="graph",
            return_type="raw",
        ),
    )

    explanation = explainer(data.x, data.edge_index, batch=batch)

    pred = _predict_denormalized(data)
    print(f"\nExplaining prediction for test graph: {pred:.4f}")

    # Per-node feature-importance summary (sum over the feature dimension)
    node_importance = explanation.node_mask.sum(dim=-1)
    top_nodes = torch.argsort(node_importance, descending=True)[:5]
    print("Top 5 most important atoms (node indices):", top_nodes.tolist())
    print("Their importance scores:", node_importance[top_nodes].tolist())

    # Per-edge importance summary
    edge_importance = explanation.edge_mask
    top_edges = torch.argsort(edge_importance, descending=True)[:5]
    print("Top 5 most important bonds (edge indices):", top_edges.tolist())
    print("Their importance scores:", edge_importance[top_edges].tolist())

    if save_path is not None:
        # PyG ships a convenience visualizer built on matplotlib/networkx
        explanation.visualize_graph(save_path, backend="graphviz")
        print(f"Saved explanation visualization to {save_path}")

    is_valid = explanation.validate()
    print(f"Test is valid: {is_valid}")

    explanation.visualize_feature_importance("model_topk.png", top_k=10)

    return explanation


if __name__ == "__main__":
    n_epochs = 30
    best_val_mae = float("inf")

    for epoch in range(1, n_epochs + 1):
        train_loss = train_one_epoch()
        val_mae = evaluate(val_loader)

        if val_mae < best_val_mae:
            best_val_mae = val_mae

        print(f"Epoch {epoch:02d} | Train Loss: {train_loss:.4f} | Val MAE: {val_mae:.4f}")

    test_mae = evaluate(test_loader)
    print(f"\nFinal Test MAE: {test_mae:.4f} (best Val MAE: {best_val_mae:.4f})")

    # -----------------------------------------------------------------------
    # 5. Explainability: which nodes/edges/features drive a prediction?
    # -----------------------------------------------------------------------
    explain_single_graph(test_dataset[0], save_path="explanation_graph0.png")