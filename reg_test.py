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

    def forward(self, x, edge_index, batch, edge_weight=None):
        # Node-level message passing. `edge_weight` lets us softly scale
        # each edge's contribution (used later for explanation fidelity --
        # not needed for normal training/inference, where it stays None).
        for conv in self.convs:
            x = conv(x, edge_index, edge_weight=edge_weight)
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


@torch.no_grad()
def _masked_prediction(data, node_mask=None, edge_mask=None):
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


def evaluate_explanation_quality(data, explanation, verbose=True):
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
    node_mask = explanation.node_mask          # [num_nodes, num_features], in [0, 1]
    edge_mask = explanation.edge_mask          # [num_edges], in [0, 1]

    y_original = _masked_prediction(data)  # no masking = full graph
    # Complement: remove (zero out) the important parts, keep the rest.
    y_complement = _masked_prediction(data, node_mask=1 - node_mask, edge_mask=1 - edge_mask)
    # Subgraph: keep ONLY the important parts, zero out the rest.
    y_subgraph = _masked_prediction(data, node_mask=node_mask, edge_mask=edge_mask)

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
        print(f"  Fidelity+        : {fidelity_plus:.4f}  (higher is better -- removing the important part should hurt)")
        print(f"  Fidelity-        : {fidelity_minus:.4f}  (lower is better -- the important part alone should be enough)")
        print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
        print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of edges marked unimportant)")
        print(f"  Node sparsity    : {node_sparsity:.2%}  (share of nodes marked unimportant)")

    return metrics

def explain_single_graph(data, save_path):
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
        explanation.visualize_graph(save_path, backend="networkx")
        print(f"Saved explanation visualization to {save_path}")

    # Don't just trust the explanation -- check how faithful it actually is.
    quality_metrics = evaluate_explanation_quality(data, explanation)

    return explanation, quality_metrics


# QM9's node features start with a one-hot atom type, and edge_attr is a
# one-hot bond type. We use these to turn per-graph importance scores into
# interpretable, aggregate statistics ("on average, how important is a
# nitrogen atom?" rather than "node #7 in this one molecule").
QM9_ATOM_TYPES = ["H", "C", "N", "O", "F"]
QM9_BOND_TYPES = ["single", "double", "triple", "aromatic"]


def explain_multiple_graphs(graphs, n_graphs=50, epochs_per_graph=100):
    """
    Run GNNExplainer over many graphs and aggregate importance by atom type
    and bond type, instead of inspecting explanations one graph at a time.

    Note: GNNExplainer's mask optimization is inherently per-graph (it learns
    a mask sized to that graph's own nodes/edges), so "batching" here means
    looping efficiently and pooling results into a model-wide summary, rather
    than optimizing one giant mask across a literal `Batch` of graphs at once.
    """
    # Build the Explainer once and reuse it across all graphs (the model
    # being explained doesn't change, so there's no need to recreate it).
    explainer = Explainer(
        model=model,
        algorithm=GNNExplainer(epochs=epochs_per_graph),
        explanation_type="model",
        node_mask_type="attributes",
        edge_mask_type="object",
        model_config=dict(mode="regression", task_level="graph", return_type="raw"),
    )

    atom_importance_sum = {atom: 0.0 for atom in QM9_ATOM_TYPES}
    atom_importance_count = {atom: 0 for atom in QM9_ATOM_TYPES}
    bond_importance_sum = {bond: 0.0 for bond in QM9_BOND_TYPES}
    bond_importance_count = {bond: 0 for bond in QM9_BOND_TYPES}

    # Track explanation quality across the whole batch, not just per-graph,
    # so we know whether the explanations are *generally* trustworthy.
    quality_totals = {
        "fidelity_plus": 0.0,
        "fidelity_minus": 0.0,
        "characterization_score": 0.0,
    }

    graphs_to_explain = graphs[:n_graphs]
    for i, data in enumerate(graphs_to_explain):
        data = data.to(device)
        batch = torch.zeros(data.num_nodes, dtype=torch.long, device=device)
        explanation = explainer(data.x, data.edge_index, batch=batch)

        # --- aggregate node (atom) importance by atom type ---
        node_importance = explanation.node_mask.sum(dim=-1)
        atom_type_idx = data.x[:, : len(QM9_ATOM_TYPES)].argmax(dim=-1)
        for atom_idx, score in zip(atom_type_idx.tolist(), node_importance.tolist()):
            atom_name = QM9_ATOM_TYPES[atom_idx]
            atom_importance_sum[atom_name] += score
            atom_importance_count[atom_name] += 1

        # --- aggregate edge (bond) importance by bond type ---
        if data.edge_attr is not None and data.edge_attr.size(-1) >= len(QM9_BOND_TYPES):
            bond_type_idx = data.edge_attr[:, : len(QM9_BOND_TYPES)].argmax(dim=-1)
            edge_importance = explanation.edge_mask
            for bond_idx, score in zip(bond_type_idx.tolist(), edge_importance.tolist()):
                bond_name = QM9_BOND_TYPES[bond_idx]
                bond_importance_sum[bond_name] += score
                bond_importance_count[bond_name] += 1

        # --- track this graph's explanation quality (no print per-graph) ---
        graph_metrics = evaluate_explanation_quality(data, explanation, verbose=False)
        for key in quality_totals:
            quality_totals[key] += graph_metrics[key]

        if (i + 1) % 10 == 0:
            print(f"Explained {i + 1}/{len(graphs_to_explain)} graphs...")

    print(f"\n=== Aggregated atom-type importance over {len(graphs_to_explain)} graphs ===")
    atom_summary = {}
    for atom in QM9_ATOM_TYPES:
        count = atom_importance_count[atom]
        avg = atom_importance_sum[atom] / count if count > 0 else float("nan")
        atom_summary[atom] = avg
        print(f"  {atom:>2s}: avg importance = {avg:.4f}  (seen {count} times)")

    print(f"\n=== Aggregated bond-type importance over {len(graphs_to_explain)} graphs ===")
    bond_summary = {}
    for bond in QM9_BOND_TYPES:
        count = bond_importance_count[bond]
        avg = bond_importance_sum[bond] / count if count > 0 else float("nan")
        bond_summary[bond] = avg
        print(f"  {bond:>8s}: avg importance = {avg:.4f}  (seen {count} times)")

    n = len(graphs_to_explain)
    quality_summary = {key: total / n for key, total in quality_totals.items()}
    print(f"\n=== Average explanation quality over {n} graphs ===")
    print(f"  Fidelity+        : {quality_summary['fidelity_plus']:.4f}  (higher is better)")
    print(f"  Fidelity-        : {quality_summary['fidelity_minus']:.4f}  (lower is better)")
    print(f"  Characterization : {quality_summary['characterization_score']:.4f}  (higher is better, in [0, 1])")
    print("  -> If these look poor across many graphs, GNNExplainer's masks aren't")
    print("     reliably capturing what the model uses -- consider more epochs,")
    print("     a different explainer algorithm, or treating per-graph explanations")
    print("     with more skepticism.")

    return {
        "atom_importance": atom_summary,
        "bond_importance": bond_summary,
        "explanation_quality": quality_summary,
    }


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
    explanation, quality_metrics = explain_single_graph(test_dataset[0], save_path="explanation_graph0.png")

    # -----------------------------------------------------------------------
    # 6. Batch explanation: aggregate importance across many graphs
    # -----------------------------------------------------------------------
    explain_multiple_graphs(test_dataset, n_graphs=50)