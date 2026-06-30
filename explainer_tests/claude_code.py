# Regression explanation
"""
Node Regression on a Heterogeneous Graph with PyTorch Geometric (PyG)
======================================================================

This example shows how to train a GNN to perform *node-level regression*
on a heterogeneous graph -- i.e. one graph with multiple node types and
multiple edge (relation) types, where we predict a continuous value for
every node of ONE of those types.

Setup (synthetic "academic graph", inspired by datasets like OGB-MAG, but
built from scratch here since most bundled heterogeneous datasets in PyG
are labeled for node *classification*, not regression):

    - Node types : 'author', 'paper', 'venue'
    - Edge types : ('author', 'writes', 'paper')      + reverse
                   ('paper', 'published_in', 'venue')  + reverse
    - Target     : a continuous "impact score" for every PAPER node,
                   generated as a function of its features, its number of
                   authors, and its venue -- stands in for something like
                   citation count or downstream-use frequency in a real
                   dataset.

Unlike graph regression (one prediction per graph, many small graphs),
node regression here means: ONE graph, many nodes, predict a value per
node -- so we split with train/val/test MASKS over paper-node indices
rather than splitting whole graphs into separate sets.

Install requirements (if not already installed):
    pip install torch torch_geometric --break-system-packages
"""

import copy

import numpy as np
import torch
import torch.nn.functional as F
from torch.nn import Linear
from torch_geometric.data import HeteroData
from torch_geometric.nn import HGTConv

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ---------------------------------------------------------------------------
# 1. Build a synthetic heterogeneous graph
# ---------------------------------------------------------------------------
def build_synthetic_hetero_graph(
    num_authors=2000,
    num_papers=5000,
    num_venues=50,
    author_dim=16,
    paper_dim=24,
    venue_dim=8,
    seed=42,
):
    rng = np.random.default_rng(seed)
    data = HeteroData()

    # --- node features per type ---
    data["author"].x = torch.tensor(rng.normal(size=(num_authors, author_dim)), dtype=torch.float)
    data["paper"].x = torch.tensor(rng.normal(size=(num_papers, paper_dim)), dtype=torch.float)
    data["venue"].x = torch.tensor(rng.normal(size=(num_venues, venue_dim)), dtype=torch.float)

    # --- (author, writes, paper) edges: each paper has 1-4 random authors ---
    src, dst = [], []
    for p in range(num_papers):
        n_auth = rng.integers(1, 5)
        authors = rng.choice(num_authors, size=n_auth, replace=False)
        src.extend(authors.tolist())
        dst.extend([p] * n_auth)
    writes_edge_index = torch.tensor([src, dst], dtype=torch.long)
    data["author", "writes", "paper"].edge_index = writes_edge_index
    data["paper", "written_by", "author"].edge_index = writes_edge_index.flip(0)

    # --- (paper, published_in, venue) edges: each paper has exactly 1 venue ---
    venues = rng.integers(0, num_venues, size=num_papers)
    pub_edge_index = torch.tensor(
        np.stack([np.arange(num_papers), venues]), dtype=torch.long
    )
    data["paper", "published_in", "venue"].edge_index = pub_edge_index
    data["venue", "publishes", "paper"].edge_index = pub_edge_index.flip(0)

    # --- synthetic continuous regression target for every PAPER node ---
    dst_tensor = torch.tensor(dst, dtype=torch.long)
    num_authors_per_paper = torch.bincount(dst_tensor, minlength=num_papers).float()
    venue_idx = torch.tensor(venues, dtype=torch.long)
    venue_effect = data["venue"].x[:, 0][venue_idx]

    noise = torch.tensor(rng.normal(0, 0.2, size=num_papers), dtype=torch.float)
    target = (
        2.0 * data["paper"].x[:, 0]
        + 0.3 * num_authors_per_paper
        + 0.5 * venue_effect
        + noise
    )
    data["paper"].y = target

    # --- train / val / test split over PAPER node indices ---
    perm = torch.randperm(num_papers)
    n_train = int(0.7 * num_papers)
    n_val = int(0.15 * num_papers)

    train_mask = torch.zeros(num_papers, dtype=torch.bool)
    val_mask = torch.zeros(num_papers, dtype=torch.bool)
    test_mask = torch.zeros(num_papers, dtype=torch.bool)
    train_mask[perm[:n_train]] = True
    val_mask[perm[n_train : n_train + n_val]] = True
    test_mask[perm[n_train + n_val :]] = True

    data["paper"].train_mask = train_mask
    data["paper"].val_mask = val_mask
    data["paper"].test_mask = test_mask

    return data


data = build_synthetic_hetero_graph()

# Normalize the target using train-set statistics only (avoid leaking
# val/test information into the normalization).
train_y = data["paper"].y[data["paper"].train_mask]
target_mean, target_std = train_y.mean(), train_y.std()
data["paper"].y = (data["paper"].y - target_mean) / target_std

data = data.to(device)
target_mean, target_std = target_mean.to(device), target_std.to(device)

# ---------------------------------------------------------------------------
# 2. Define the GNN
#
# We use HGTConv (Heterogeneous Graph Transformer) instead of a plain conv
# duplicated via `to_hetero`. The key difference: `to_hetero` applies the
# SAME fixed aggregation (e.g. "sum") to every relation, treating
# author->paper and paper->venue as equally important by construction.
# HGTConv instead learns ATTENTION WEIGHTS per relation type, so the model
# decides for itself how much to trust each relation -- which tends to
# matter a lot once some relations are denser, noisier, or simply more
# predictive than others.
# ---------------------------------------------------------------------------
class HeteroNodeRegressor(torch.nn.Module):
    def __init__(self, metadata, hidden_channels=64, num_layers=2,
                 heads=4, target_node_type="paper"):
        super().__init__()
        self.target_node_type = target_node_type

        # HGTConv handles the per-node-type input projection internally
        # (via lazy-initialized linear layers, in_channels=-1), so we don't
        # need a separate lin_dict like the to_hetero version did -- each
        # layer figures out the right input dimension for every type the
        # first time it sees real data.
        self.convs = torch.nn.ModuleList(
            [
                HGTConv(-1, hidden_channels, metadata, heads=heads)
                for _ in range(num_layers)
            ]
        )

        # Regression head -- only applied to the node type we care about.
        self.out_lin = Linear(hidden_channels, 1)

    def forward(self, x_dict, edge_index_dict):
        for conv in self.convs:
            x_dict = {nt: x.relu() for nt, x in conv(x_dict, edge_index_dict).items()}
        out = self.out_lin(x_dict[self.target_node_type])
        return out.squeeze(-1)


metadata = data.metadata()

model = HeteroNodeRegressor(
    metadata, hidden_channels=64, num_layers=2, heads=4, target_node_type="paper"
).to(device)

# HGTConv's lazy linear layers need one real forward pass before their
# parameters exist, so optimizer construction must come AFTER this.
with torch.no_grad():
    model(data.x_dict, data.edge_index_dict)

optimizer = torch.optim.Adam(model.parameters(), lr=5e-3, weight_decay=1e-5)


# ---------------------------------------------------------------------------
# 3. Train / evaluate
#
# This is a single graph, so training is "full-batch": every forward pass
# uses the entire graph, and the train/val/test masks select which PAPER
# nodes' losses/metrics actually count. For graphs too large to fit in
# memory this way, swap in `torch_geometric.loader.NeighborLoader` to
# sample mini-batches of neighborhoods instead -- the model code is
# unchanged either way.
# ---------------------------------------------------------------------------
def train_one_epoch():
    model.train()
    optimizer.zero_grad()
    out = model(data.x_dict, data.edge_index_dict)
    mask = data["paper"].train_mask
    loss = F.mse_loss(out[mask], data["paper"].y[mask])
    loss.backward()
    optimizer.step()
    return loss.item()


@torch.no_grad()
def evaluate(mask_name):
    model.eval()
    out = model(data.x_dict, data.edge_index_dict)
    mask = data["paper"][mask_name]

    # De-normalize before computing MAE so it's in original target units.
    pred = out[mask] * target_std + target_mean
    true = data["paper"].y[mask] * target_std + target_mean
    return (pred - true).abs().mean().item()


def get_k_hop_predecessor_set(target_type, target_idx, num_hops):
    """
    Find every node within `num_hops` PRECEDING the target in the message-
    passing direction (each edge type's `edge_index` is src -> dst) -- i.e.
    every node that could possibly influence the target's final embedding
    after that many GNN layers.

    Unlike the DBLP classification example (which used NeighborLoader's
    randomly-SAMPLED neighborhood, since that model trains via mini-batches
    with a fixed fan-out), this regression model trains full-batch over the
    WHOLE graph -- so this is the model's EXACT receptive field for that
    node, with nothing approximated away. Anything outside this set is
    provably irrelevant to the target's prediction and not worth testing.
    """
    frontier = {nt: set() for nt in data.node_types}
    frontier[target_type].add(target_idx)
    visited_nodes = {nt: set(s) for nt, s in frontier.items()}
    visited_edges = {et: set() for et in data.edge_types}

    for _ in range(num_hops):
        new_frontier = {nt: set() for nt in data.node_types}
        for edge_type in data.edge_types:
            src_type, _, dst_type = edge_type
            dst_frontier = frontier[dst_type]
            if not dst_frontier:
                continue
            edge_index = data[edge_type].edge_index
            src_list = edge_index[0].tolist()
            dst_list = edge_index[1].tolist()
            for pos, (s, d) in enumerate(zip(src_list, dst_list)):
                if d in dst_frontier:
                    visited_edges[edge_type].add(pos)
                    if s not in visited_nodes[src_type]:
                        new_frontier[src_type].add(s)
        for nt in data.node_types:
            visited_nodes[nt] |= new_frontier[nt]
        frontier = new_frontier

    return visited_nodes, visited_edges


@torch.no_grad()
def _predict_value(perturbed_data=None, paper_idx=None):
    """Run the model on (a possibly perturbed copy of) the FULL graph and
    return the de-normalized prediction for one specific paper -- the
    regression analogue of `_predict_proba` in the classification version.
    No subgraph sampling here; the model always sees the whole graph,
    matching how it was actually trained."""
    d = perturbed_data if perturbed_data is not None else data
    out = model(d.x_dict, d.edge_index_dict)
    denorm = out * target_std + target_mean
    return denorm[paper_idx].item() if paper_idx is not None else denorm


def feature_importance_for_node_regression(node_type, node_idx, baseline_value,
                                            target_paper_idx, top_k=10):
    """
    Leave-one-out at the FEATURE level, regression version: zero out each
    feature dimension of one specific node individually, and measure the
    resulting SHIFT in the TARGET paper's predicted value. The regression
    analogue of `feature_importance_for_node`'s confidence-drop measure --
    here there's no probability to drop, just a continuous prediction that
    moves up or down.
    """
    x = data[node_type].x[node_idx]
    num_features = x.size(0)
    feature_importances = []  # (feature_idx, value_shift, large_shift)

    for f in range(num_features):
        if x[f].item() == 0.0:
            continue  # already zero -- nothing to remove, skip for speed
        perturbed = data.clone()
        perturbed[node_type].x[node_idx, f] = 0.0
        pred = _predict_value(perturbed, target_paper_idx)
        shift = abs(baseline_value - pred)
        large_shift = shift > target_std.item()  # see note in main function
        feature_importances.append((f, shift, large_shift))

    feature_importances.sort(key=lambda t: t[1], reverse=True)
    return feature_importances[:top_k]


def counterfactual_explain_paper(paper_idx, top_k=5, num_hops=2):
    """
    Counterfactual ("what if this weren't here?") explanation for ONE
    paper's REGRESSION prediction (its predicted impact score) -- adapted
    from `counterfactual_explain_author` in the DBLP classification example.

    What changes from the classification version:
      - "Confidence drop" becomes raw VALUE SHIFT (de-normalized units --
        e.g. "removing this shifted the predicted impact score by 0.8").
        There's no probability to read off a softmax head.
      - "Flips the predicted class" has no regression equivalent (no
        discrete decision boundary to cross), so instead we flag any
        single removal whose shift exceeds ONE TARGET STANDARD DEVIATION
        as a "large shift" -- i.e. a change comparable in size to the
        natural spread of the target itself, the closest regression
        analogue of "this one thing single-handedly changed the verdict".
      - The candidate nodes/edges to test come from the EXACT k-hop
        receptive field (see `get_k_hop_predecessor_set`) rather than a
        NeighborLoader-sampled approximate neighborhood, since this model
        trains full-batch with no sampling involved.

    Everything else -- leave-one-out over nodes, then edges, then feature-
    level drill-down on the seed node and the top neighbor -- mirrors the
    classification version structurally.
    """
    candidate_nodes, candidate_edges = get_k_hop_predecessor_set("paper", paper_idx, num_hops)

    print("candidate_nodes", candidate_nodes)
    print("candidate_edges", candidate_edges)

    baseline_value = _predict_value(paper_idx=paper_idx)

    print(f"\nExplaining paper node #{paper_idx}")
    print(f"  Predicted impact score: {baseline_value:.4f}")
    print(f"  Exact {num_hops}-hop receptive field: " +
          ", ".join(f"{nt}={len(idxs)}" for nt, idxs in candidate_nodes.items() if idxs))

    # --- leave-one-out over every NODE in the exact receptive field ---
    node_importances = []  # (node_type, idx, value_shift, large_shift)
    for node_type, idx_set in candidate_nodes.items():
        for idx in idx_set:
            if node_type == "paper" and idx == paper_idx:
                continue  # skip the target itself -- same reasoning as the
                          # classification version: we want what its
                          # prediction depends ON, not the trivial effect
                          # of removing the node being predicted.
            perturbed = data.clone()
            perturbed[node_type].x[idx] = 0.0
            pred = _predict_value(perturbed, paper_idx)
            shift = abs(baseline_value - pred)
            large_shift = shift > target_std.item()
            node_importances.append((node_type, idx, shift, large_shift))

    # --- leave-one-out over every EDGE in the exact receptive field ---
    edge_importances = []  # (edge_type, position, value_shift, large_shift)
    for edge_type, pos_set in candidate_edges.items():
        if not pos_set:
            continue
        edge_index = data[edge_type].edge_index
        num_edges = edge_index.size(1)
        for e in pos_set:
            perturbed = data.clone()
            keep = torch.ones(num_edges, dtype=torch.bool, device=device)
            keep[e] = False
            perturbed[edge_type].edge_index = edge_index[:, keep]
            pred = _predict_value(perturbed, paper_idx)
            shift = abs(baseline_value - pred)
            large_shift = shift > target_std.item()
            edge_importances.append((edge_type, e, shift, large_shift))

    node_importances.sort(key=lambda t: t[2], reverse=True)
    edge_importances.sort(key=lambda t: t[2], reverse=True)

    # --- feature-level counterfactual on the SEED paper's own input ---
    seed_feature_importances = feature_importance_for_node_regression(
        "paper", paper_idx, baseline_value, paper_idx, top_k=top_k
    )

    # --- feature-level counterfactual on the single most influential
    #     NEIGHBOR node ---
    if node_importances:
        top_node_type, top_node_idx, _, _ = node_importances[0]
        top_node_feature_importances = feature_importance_for_node_regression(
            top_node_type, top_node_idx, baseline_value, paper_idx, top_k=top_k
        )
    else:
        top_node_type, top_node_idx = None, None
        top_node_feature_importances = []

    print(f"\n  Top {top_k} most important NODES (predicted value shift if removed):")
    for node_type, i, shift, large in node_importances[:top_k]:
        flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
        print(f"    {node_type}[{i}]: value shift = {shift:.4f}{flag}")

    print(f"\n  Top {top_k} most important EDGES (predicted value shift if removed):")
    for edge_type, e, shift, large in edge_importances[:top_k]:
        src, dst = data[edge_type].edge_index[:, e].tolist()
        flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
        print(f"    {edge_type} edge ({src} -> {dst}): value shift = {shift:.4f}{flag}")

    print(f"\n  Top {top_k} most important FEATURES on the seed paper itself:")
    for f, shift, large in seed_feature_importances:
        flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
        print(f"    paper[{paper_idx}].x[{f}]: value shift = {shift:.4f}{flag}")

    if top_node_type is not None:
        print(f"\n  Top {top_k} most important FEATURES on the most influential neighbor "
              f"({top_node_type}[{top_node_idx}]):")
        for f, shift, large in top_node_feature_importances:
            flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
            print(f"    {top_node_type}[{top_node_idx}].x[{f}]: value shift = {shift:.4f}{flag}")

    any_large = (
        any(l for *_, l in node_importances)
        or any(l for *_, l in edge_importances)
        or any(l for *_, l in seed_feature_importances)
        or any(l for *_, l in top_node_feature_importances)
    )
    if any_large:
        print("\n  >>> At least one single node, edge, or feature removal above shifts the")
        print("      prediction by more than one target standard deviation -- the regression")
        print("      analogue of a single piece of evidence dominating the model's output.")
    else:
        print("\n  >>> No single removal shifts the prediction by more than one target standard")
        print("      deviation -- the model's prediction here is robust, built from combined,")
        print("      redundant evidence rather than one dominant factor.")

    return node_importances, edge_importances, seed_feature_importances, top_node_feature_importances



# # ReduceLROnPlateau watches val MAE and shrinks the LR when progress
# # stalls -- full-batch training on one graph tends to plateau well
# # before a fixed number of epochs is up, so a static LR wastes the
# # back half of training.
# scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
#     optimizer, mode="min", factor=0.5, patience=10
# )
#
# max_epochs = 300
# early_stop_patience = 30  # stop if val MAE hasn't improved in this many epochs
#
# best_val_mae = float("inf")
# best_state = None
# epochs_without_improvement = 0
#
# for epoch in range(1, max_epochs + 1):
#     train_loss = train_one_epoch()
#     val_mae = evaluate("val_mask")
#     scheduler.step(val_mae)
#
#     if val_mae < best_val_mae:
#         best_val_mae = val_mae
#         best_state = copy.deepcopy(model.state_dict())
#         epochs_without_improvement = 0
#     else:
#         epochs_without_improvement += 1
#
#     if epoch % 10 == 0 or epoch == 1:
#         current_lr = optimizer.param_groups[0]["lr"]
#         print(
#             f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
#             f"Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}"
#         )
#
#     if epochs_without_improvement >= early_stop_patience:
#         print(f"\nEarly stopping at epoch {epoch} "
#               f"(no val improvement for {early_stop_patience} epochs)")
#         break
#
# # Reload the best-performing checkpoint before final evaluation --
# # the LAST epoch's weights aren't necessarily the BEST epoch's weights,
# # especially once early stopping is in play.
# if best_state is not None:
#     model.load_state_dict(best_state)
#
# test_mae = evaluate("test_mask")
# print(f"\nFinal Test MAE: {test_mae:.4f} (best Val MAE: {best_val_mae:.4f})")
# torch.save(model.state_dict(), f"reg_test.pth")

model.load_state_dict(torch.load(f"reg_test.pth", weights_only=False))
# -----------------------------------------------------------------------
# 4. Counterfactual explanation, adapted for regression
# -----------------------------------------------------------------------
example_paper_idx = data["paper"].test_mask.nonzero(as_tuple=True)[0][0].item()
counterfactual_explain_paper(example_paper_idx, top_k=5, num_hops=2)