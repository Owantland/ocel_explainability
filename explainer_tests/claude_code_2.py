# Claude Explanation with Subgraph

"""
Heterogeneous Node Classification on DBLP with PyTorch Geometric (PyG)
========================================================================

This example uses PyG's bundled `DBLP` dataset -- a real heterogeneous
academic graph (NOT synthetic, unlike the earlier examples in this series)
-- together with a mini-batch `NeighborLoader` that avoids depending on
`torch-sparse`.

IMPORTANT SCOPE NOTE: DBLP ships only classification labels (4-class
research-area labels for `author` nodes), not a continuous regression
target. So this example is *node classification*, not regression -- the
model/training code is otherwise structurally identical to the node
regression example (same per-relation message passing, same train/val/test
mask pattern), just with a softmax head and cross-entropy loss instead of
a scalar head and MSE/L1 loss.

Dataset structure (loaded automatically by PyG):
    - Node types : 'author', 'paper', 'term', 'conference'
    - Edge types : ('author','to','paper') + reverse
                   ('paper','to','term') + reverse
                   ('paper','to','conference') + reverse
    - Labels     : 'author' nodes only, 4 classes, with train/val/test
                   masks already provided by the dataset.
    - Caveat     : 'conference' nodes have no raw features in the original
                   data. We add a constant placeholder feature via
                   `T.Constant` so every node type has *something* to
                   project into the model's hidden space.

ON THE LOADER (the actual point of this example):
    PyG's `NeighborLoader` samples a fixed-size neighborhood around a batch
    of "seed" nodes, for mini-batch training on graphs too large (or, for
    practice, just inconvenient) to run full-batch. Historically this
    required `torch-sparse`'s compiled sampling routines. As of recent PyG
    versions, `NeighborLoader` instead prefers `pyg-lib`'s sampler when
    it's installed, and `pyg-lib` has NO dependency on `torch-sparse` at
    all. So: install `pyg-lib`, and this loader works without ever
    touching `torch-sparse`.

Install requirements:
    pip install torch torch_geometric pyg-lib --break-system-packages
    (pyg-lib wheels are version-matched to your torch/CUDA build --
    see https://data.pyg.org/whl/ if the plain pip install doesn't find one)
"""

import torch
import torch.nn.functional as F
from torch.nn import Linear
import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import HGTConv

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ---------------------------------------------------------------------------
# 1. Load the DBLP dataset
# ---------------------------------------------------------------------------
# `T.Constant(node_types='conference')` gives 'conference' nodes a constant
# placeholder feature (a single column of 1s), since the raw dataset has no
# features for that node type at all.
dataset = DBLP(root="data/DBLP", transform=T.Constant(node_types="conference"))
data = dataset[0]  # DBLP is a single heterogeneous graph, not a list of graphs
data = data.to(device)

metadata = data.metadata()
num_classes = int(data["author"].y.max().item()) + 1


# ---------------------------------------------------------------------------
# 2. Mini-batch loaders via NeighborLoader (no torch-sparse dependency)
#
# `input_nodes=('author', data['author'].train_mask)` tells the loader
# which nodes are the actual prediction targets for this split -- it then
# samples a neighborhood AROUND each batch of authors, pulling in whatever
# papers/terms/conferences are needed to compute their embeddings.
# `num_neighbors=[10, 10]` caps the fan-out at 10 neighbors per hop, for 2
# hops (matching the 2-layer model below).
# ---------------------------------------------------------------------------
train_loader = NeighborLoader(
    data,
    num_neighbors=[10, 10],
    batch_size=128,
    input_nodes=("author", data["author"].train_mask),
    shuffle=True,
)

val_loader = NeighborLoader(
    data,
    num_neighbors=[10, 10],
    batch_size=128,
    input_nodes=("author", data["author"].val_mask),
    shuffle=False,
)

test_loader = NeighborLoader(
    data,
    num_neighbors=[10, 10],
    batch_size=128,
    input_nodes=("author", data["author"].test_mask),
    shuffle=False,
)


# ---------------------------------------------------------------------------
# 3. Define the GNN (same HGTConv-based design as the earlier node
#    regression example, swapped to a classification head)
# ---------------------------------------------------------------------------
class HeteroNodeClassifier(torch.nn.Module):
    def __init__(self, metadata, hidden_channels=64, num_layers=2,
                 heads=4, num_classes=4, target_node_type="author"):
        super().__init__()
        self.target_node_type = target_node_type

        # Lazy per-type input projection, same as the HGTConv regression
        # model -- handles 'author'/'paper'/'term' real features and
        # 'conference' nodes' constant placeholder feature uniformly.
        self.convs = torch.nn.ModuleList(
            [
                HGTConv(-1, hidden_channels, metadata, heads=heads)
                for _ in range(num_layers)
            ]
        )

        self.out_lin = Linear(hidden_channels, num_classes)

    def forward(self, x_dict, edge_index_dict):
        for conv in self.convs:
            x_dict = {nt: x.relu() for nt, x in conv(x_dict, edge_index_dict).items()}
        return self.out_lin(x_dict[self.target_node_type])  # raw logits


model = HeteroNodeClassifier(
    metadata, hidden_channels=64, num_layers=2, heads=4,
    num_classes=num_classes, target_node_type="author",
).to(device)

# Materialize HGTConv's lazy linear layers with one real forward pass
# before constructing the optimizer (same reason as the regression example).
with torch.no_grad():
    batch = next(iter(train_loader))
    model(batch.x_dict, batch.edge_index_dict)

optimizer = torch.optim.Adam(model.parameters(), lr=5e-3, weight_decay=1e-5)


# ---------------------------------------------------------------------------
# 4. Train / evaluate
#
# Each `batch` from NeighborLoader is its own small HeteroData object: a
# sampled subgraph containing the seed authors plus their neighborhood.
# `batch['author'].batch_size` tells you how many of `batch['author'].x`'s
# rows are the actual seed nodes for this step -- NeighborLoader always
# places them FIRST, with sampled neighbor nodes appended after. So we
# only compute loss/accuracy on `out[:batch_size]`, not the whole subgraph.
# ---------------------------------------------------------------------------
def train_one_epoch():
    model.train()
    total_loss = total_correct = total_examples = 0

    for batch in train_loader:
        batch = batch.to(device)
        optimizer.zero_grad()

        out = model(batch.x_dict, batch.edge_index_dict)
        batch_size = batch["author"].batch_size
        seed_out = out[:batch_size]
        seed_y = batch["author"].y[:batch_size]

        loss = F.cross_entropy(seed_out, seed_y)
        loss.backward()
        optimizer.step()

        total_loss += loss.item() * batch_size
        total_correct += (seed_out.argmax(dim=-1) == seed_y).sum().item()
        total_examples += batch_size

    return total_loss / total_examples, total_correct / total_examples


@torch.no_grad()
def evaluate(loader):
    model.eval()
    total_correct = total_examples = 0

    for batch in loader:
        batch = batch.to(device)
        out = model(batch.x_dict, batch.edge_index_dict)
        batch_size = batch["author"].batch_size
        seed_out = out[:batch_size]
        seed_y = batch["author"].y[:batch_size]

        total_correct += (seed_out.argmax(dim=-1) == seed_y).sum().item()
        total_examples += batch_size

    return total_correct / total_examples


def get_single_node_subgraph(author_idx, num_neighbors=[10, 10]):
    """Sample a fresh neighborhood subgraph around exactly ONE author node,
    for fine-grained counterfactual analysis (separate from the training
    loaders, which batch many seed nodes together)."""
    loader = NeighborLoader(
        data,
        num_neighbors=num_neighbors,
        batch_size=1,
        input_nodes=("author", torch.tensor([author_idx], dtype=torch.long, device=data["author"].x.device)),
        shuffle=False,
    )
    return next(iter(loader))


@torch.no_grad()
def _predict_proba(batch):
    batch = batch.to(device)
    out = model(batch.x_dict, batch.edge_index_dict)
    # Correct way is to instantiate the loader and then obtain the length in case
    # we want to use a larger length example
    seed_out = out[: 1]
    return F.softmax(seed_out, dim=-1)[0]


def feature_importance_for_node(batch, node_type, node_idx, baseline_confidence,
                                 predicted_class, top_k=10):
    """
    Leave-one-out at the FEATURE level: for one specific node, zero out
    each feature dimension individually and measure the resulting drop in
    confidence for the original predicted class. This refines the
    node-level analysis above -- "paper[12] matters a lot" becomes
    "...specifically because of features 3, 7, and 19" (e.g. particular
    words in a bag-of-words representation).
    """
    x = batch[node_type].x[node_idx]
    num_features = x.size(0)
    feature_importances = []  # (feature_idx, confidence_drop, flips_prediction)

    for f in range(num_features):
        if x[f].item() == 0.0:
            continue  # already zero -- nothing to remove, skip for speed
        perturbed = batch.clone()
        perturbed[node_type].x[node_idx, f] = 0.0
        proba = _predict_proba(perturbed)
        confidence_drop = baseline_confidence - proba[predicted_class].item()
        flips = proba.argmax().item() != predicted_class
        feature_importances.append((f, confidence_drop, flips))

    feature_importances.sort(key=lambda t: t[1], reverse=True)
    return feature_importances[:top_k]


def counterfactual_explain_author(author_idx, top_k=5, num_neighbors=[10, 10]):
    """
    Counterfactual ("what if this weren't here?") explanation for ONE
    author's prediction. We remove each neighboring node (zero its
    features) or each edge (drop it from edge_index) one at a time, and
    measure how much the model's confidence in its ORIGINAL predicted
    class shifts as a result. We then drill one level deeper and do the
    same leave-one-out idea on individual FEATURES of the most relevant
    nodes -- "which node" refined into "which specific input signal".

    - Big confidence drop when removed  -> the model's decision counter-
      factually depends heavily on that node/edge/feature.
    - If removing a single node/edge/feature actually flips the predicted
      class, that's a genuine counterfactual: the smallest possible change
      that changes the model's decision.

    This is a leave-one-out occlusion method, not gradient-based, so it
    works identically regardless of model architecture or task type --
    useful here since PyG's `Explainer`/`GNNExplainer` heterogeneous-graph
    support is less mature than its homogeneous-graph support.
    """
    batch = get_single_node_subgraph(author_idx, num_neighbors).to(device)
    baseline_proba = _predict_proba(batch)
    predicted_class = baseline_proba.argmax().item()
    baseline_confidence = baseline_proba[predicted_class].item()

    print(f"\nExplaining author node #{author_idx}")
    print(f"  Predicted class: {predicted_class} (confidence {baseline_confidence:.4f})")
    print(f"  Sampled neighborhood: " +
          ", ".join(f"{nt}={batch[nt].num_nodes}" for nt in batch.node_types))

    # --- leave-one-out over every NODE in the sampled neighborhood ---
    node_importances = []  # (node_type, local_idx, confidence_drop, flips_prediction)
    for node_type in batch.node_types:
        n = batch[node_type].x.size(0)
        # Skip the seed author itself (always index 0 for 'author') -- we
        # want to know what its prediction depends ON, not what removing
        # the node being predicted does (that's trivially destructive).
        start = 1 if node_type == "author" else 0
        for i in range(start, n):
            perturbed = batch.clone()
            perturbed[node_type].x[i] = 0.0  # "remove" this node's signal
            proba = _predict_proba(perturbed)
            confidence_drop = baseline_confidence - proba[predicted_class].item()
            flips = proba.argmax().item() != predicted_class
            node_importances.append((node_type, i, confidence_drop, flips))

    # --- leave-one-out over every EDGE in the sampled neighborhood ---
    edge_importances = []  # (edge_type, position, confidence_drop, flips_prediction)
    for edge_type in batch.edge_types:
        edge_index = batch[edge_type].edge_index
        num_edges = edge_index.size(1)
        for e in range(num_edges):
            perturbed = batch.clone()
            keep = torch.ones(num_edges, dtype=torch.bool, device=device)
            keep[e] = False
            perturbed[edge_type].edge_index = edge_index[:, keep]
            proba = _predict_proba(perturbed)
            confidence_drop = baseline_confidence - proba[predicted_class].item()
            flips = proba.argmax().item() != predicted_class
            edge_importances.append((edge_type, e, confidence_drop, flips))

    node_importances.sort(key=lambda t: t[2], reverse=True)
    edge_importances.sort(key=lambda t: t[2], reverse=True)

    # --- feature-level counterfactual on the SEED author's own input ---
    seed_feature_importances = feature_importance_for_node(
        batch, "author", 0, baseline_confidence, predicted_class, top_k=top_k
    )

    # --- feature-level counterfactual on the single most influential
    #     NEIGHBOR node, since "this node matters" is coarser than
    #     "these specific features of this node matter" ---
    if node_importances:
        top_node_type, top_node_idx, _, _ = node_importances[0]
        top_node_feature_importances = feature_importance_for_node(
            batch, top_node_type, top_node_idx, baseline_confidence, predicted_class, top_k=top_k
        )
    else:
        top_node_type, top_node_idx = None, None
        top_node_feature_importances = []

    print(f"\n  Top {top_k} most important NODES (confidence drop if removed):")
    for node_type, i, drop, flips in node_importances[:top_k]:
        flag = "  <-- FLIPS PREDICTION" if flips else ""
        print(f"    {node_type}[{i}]: confidence drop = {drop:+.4f}{flag}")

    print(f"\n  Top {top_k} most important EDGES (confidence drop if removed):")
    for edge_type, e, drop, flips in edge_importances[:top_k]:
        src, dst = batch[edge_type].edge_index[:, e].tolist()
        flag = "  <-- FLIPS PREDICTION" if flips else ""
        print(f"    {edge_type} edge ({src} -> {dst}): confidence drop = {drop:+.4f}{flag}")

    print(f"\n  Top {top_k} most important FEATURES on the seed author itself:")
    for f, drop, flips in seed_feature_importances:
        flag = "  <-- FLIPS PREDICTION" if flips else ""
        print(f"    author[0].x[{f}]: confidence drop = {drop:+.4f}{flag}")

    if top_node_type is not None:
        print(f"\n  Top {top_k} most important FEATURES on the most influential neighbor "
              f"({top_node_type}[{top_node_idx}]):")
        for f, drop, flips in top_node_feature_importances:
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {top_node_type}[{top_node_idx}].x[{f}]: confidence drop = {drop:+.4f}{flag}")

    any_flips = (
        any(f for *_, f in node_importances)
        or any(f for *_, f in edge_importances)
        or any(f for *_, f in seed_feature_importances)
        or any(f for *_, f in top_node_feature_importances)
    )
    if any_flips:
        print("\n  >>> A genuine counterfactual exists above: removing that single")
        print("      node, edge, or feature changes the model's predicted class entirely.")
    else:
        print("\n  >>> No single node/edge/feature removal flips the prediction -- the")
        print("      model's decision here is robust to any one removal (it relies on")
        print("      combined, redundant evidence rather than one critical piece).")

    return batch, node_importances, edge_importances, seed_feature_importances, top_node_feature_importances


def build_explanation_subgraph(batch, node_importances, edge_importances,
                                node_top_k=10, edge_top_k=15):
    """
    Turn the counterfactual node/edge importance scores into an actual
    NetworkX subgraph -- the "explanation subgraph": the seed author plus
    only the most important surrounding nodes and edges, rather than the
    entire (much larger) sampled neighborhood. Conceptually the same idea
    as GNNExplainer's `get_explanation_subgraph()`, just built from our
    leave-one-out scores instead of learned soft masks.
    """
    import networkx as nx

    G = nx.MultiDiGraph()

    # Always include the seed author node.
    G.add_node(("author", 0), node_type="author", importance=1.0, is_seed=True, flips=False)

    # Keep only the top-k most important neighbor NODES (by confidence drop).
    top_nodes = node_importances[:node_top_k]
    for nt, i, drop, flips in top_nodes:
        G.add_node((nt, i), node_type=nt, importance=drop, is_seed=False, flips=flips)

    # Keep only the top-k most important EDGES, adding their endpoints if
    # not already present -- an edge can matter even if one endpoint
    # individually didn't make the node top-k cut.
    top_edges = edge_importances[:edge_top_k]
    for edge_type, e, drop, flips in top_edges:
        src_type, _, dst_type = edge_type
        edge_index = batch[edge_type].edge_index
        src, dst = edge_index[:, e].tolist()
        src_key, dst_key = (src_type, src), (dst_type, dst)

        for key, ntype in [(src_key, src_type), (dst_key, dst_type)]:
            if key not in G.nodes:
                is_seed = (ntype == "author" and key[1] == 0)
                G.add_node(key, node_type=ntype, importance=0.0, is_seed=is_seed, flips=False)

        G.add_edge(src_key, dst_key, edge_type=edge_type[1], importance=drop, flips=flips)

    return G


def visualize_explanation_subgraph(G, save_path="explanation_subgraph.png"):
    """Draw the explanation subgraph: node color = type, node size =
    importance, red edges/outlines = "removing this flips the prediction"."""
    import matplotlib.pyplot as plt
    import networkx as nx

    type_colors = {"author": "#4C72B0", "paper": "#DD8452", "term": "#55A868", "conference": "#C44E52"}

    pos = nx.spring_layout(G, seed=42, k=0.9)

    node_colors, node_sizes, edge_colors_outline = [], [], []
    for node, attrs in G.nodes(data=True):
        node_colors.append(type_colors.get(attrs["node_type"], "gray"))
        base_size = 900 if attrs.get("is_seed") else 250
        node_sizes.append(base_size + max(attrs.get("importance", 0), 0) * 2500)
        edge_colors_outline.append("red" if attrs.get("flips") else "black")

    edge_colors, edge_widths = [], []
    for _, _, attrs in G.edges(data=True):
        edge_colors.append("red" if attrs.get("flips") else "gray")
        edge_widths.append(1 + max(attrs.get("importance", 0), 0) * 12)

    plt.figure(figsize=(10, 8))
    nx.draw_networkx_nodes(
        G, pos, node_color=node_colors, node_size=node_sizes,
        edgecolors=edge_colors_outline, linewidths=1.5, alpha=0.9,
    )
    nx.draw_networkx_edges(
        G, pos, edge_color=edge_colors, width=edge_widths,
        arrows=True, connectionstyle="arc3,rad=0.1", alpha=0.7,
    )
    labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes}
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=7)

    legend_handles = [
        plt.Line2D([0], [0], marker="o", color="w", label=nt,
                   markerfacecolor=color, markersize=10)
        for nt, color in type_colors.items()
    ]
    legend_handles.append(plt.Line2D([0], [0], color="red", lw=2, label="flips prediction"))
    plt.legend(handles=legend_handles, loc="best", fontsize=8)

    plt.title("Counterfactual Explanation Subgraph")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"Saved explanation subgraph visualization to {save_path}")



if __name__ == "__main__":
#     n_epochs = 50
#     best_val_acc = 0.0
#
#     for epoch in range(1, n_epochs + 1):
#         train_loss, train_acc = train_one_epoch()
#         val_acc = evaluate(val_loader)
#
#         if val_acc > best_val_acc:
#             best_val_acc = val_acc
#
#         if epoch % 5 == 0 or epoch == 1:
#             print(
#                 f"Epoch {epoch:02d} | Train Loss: {train_loss:.4f} | "
#                 f"Train Acc: {train_acc:.4f} | Val Acc: {val_acc:.4f}"
#             )
#
#     test_acc = evaluate(test_loader)
#     print(f"\nFinal Test Accuracy: {test_acc:.4f} (best Val Accuracy: {best_val_acc:.4f})")
#     torch.save(model.state_dict(), f"dblap_test.pth")

    # -----------------------------------------------------------------------
    # 5. Counterfactual evaluation: which nodes/edges matter most, and is
    #    the model's decision fragile (a single removal flips it) or robust?
    # -----------------------------------------------------------------------
    # example_author_idx = data["author"].test_mask.nonzero(as_tuple=True)[0][100].item()

    model.load_state_dict(torch.load('dblap_test.pth', weights_only=False))

    example_author_idx = 111

    explained_batch, node_imp, edge_imp, seed_feat_imp, top_node_feat_imp = (
        counterfactual_explain_author(example_author_idx, top_k=5)
    )
    #
    # # -----------------------------------------------------------------------
    # # 6. Draw the explanation subgraph from the counterfactual scores above
    # # -----------------------------------------------------------------------
    # explanation_graph = build_explanation_subgraph(
    #     explained_batch, node_imp, edge_imp, node_top_k=10, edge_top_k=15
    # )
    # visualize_explanation_subgraph(explanation_graph, save_path="explanation_subgraph.png")