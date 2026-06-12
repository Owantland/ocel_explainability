"""
Heterogeneous Graph Node Classification with PyTorch Geometric
==============================================================

Dataset : DBLP (built-in PyG dataset)
  - Node types  : 'author' (target), 'paper', 'term', 'conference'
  - Edge types  : author–paper, paper–term, paper–conference (+ reverses)
  - Task        : Classify authors into one of 4 research areas

Model   : HGT  (Heterogeneous Graph Transformer)
Explain : HeteroExplainer wrapping GNNExplainer
          → per-node feature importance + edge-mask importance
"""

import torch
import torch.nn.functional as F
from torch import Tensor

import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP
from torch_geometric.nn import HGTConv, Linear
from torch_geometric.explain import Explainer, GNNExplainer


# ─────────────────────────────────────────────
# 1. Load the DBLP dataset
# ─────────────────────────────────────────────

def load_dblp(root: str = '/tmp/DBLP'):
    """
    Downloads and preprocesses the DBLP dataset.

    DBLP schema
    ───────────
      author      (4 057 nodes, 334-dim bag-of-words)   ← classification target
      paper       (14 328 nodes, 4 231-dim BoW)
      term        (7 723 nodes, 50-dim  word-embedding)
      conference  (20 nodes,    no features → will be one-hot after transform)

    Relations (undirected after ToUndirected):
      (author,    to, paper)
      (paper,     to, term)
      (paper,     to, conference)

    Labels: 4 research areas  (0=DB, 1=DM, 2=IR, 3=ML)
    """
    # NoPad fills conference nodes with a zero vector; Constant gives them a
    # learnable baseline. We use Constant(value=1) so every type has features.
    transform = T.Compose([
        T.Constant(node_types=['conference']),   # adds x = ones for conference
        T.ToUndirected(),                        # adds reverse edge types
    ])

    dataset = DBLP(root=root, transform=transform)
    data    = dataset[0]

    print("DBLP graph loaded:")
    print(f"  Node types : {data.node_types}")
    print(f"  Edge types : {[str(e) for e in data.edge_types]}")
    for nt in data.node_types:
        n = data[nt].num_nodes
        has_y = hasattr(data[nt], 'y') and data[nt].y is not None
        print(f"    {nt:12s}  nodes={n}"
              + (f"  classes={data[nt].y.max().item()+1}" if has_y else ""))
    print()

    return data


# ─────────────────────────────────────────────
# 2. HGT model
# ─────────────────────────────────────────────

class HGT(torch.nn.Module):
    """
    Heterogeneous Graph Transformer for node classification.

    Architecture
    ────────────
    1. Per-type Linear projection  →  shared hidden_channels
    2. num_layers × HGTConv        (type-aware multi-head attention)
    3. Linear classifier           (applied only to the target node type)
    """

    def __init__(self, metadata, hidden_channels: int, out_channels: int,
                 num_heads: int = 4, num_layers: int = 2,
                 target_type: str = 'author'):
        super().__init__()
        self.target_type = target_type
        node_types, _ = metadata

        # Lazy projections (input dim inferred on first forward pass)
        self.lin_dict = torch.nn.ModuleDict({
            nt: Linear(-1, hidden_channels) for nt in node_types
        })

        self.convs = torch.nn.ModuleList([
            HGTConv(hidden_channels, hidden_channels,
                    metadata=metadata, heads=num_heads)
            for _ in range(num_layers)
        ])

        self.classifier = Linear(hidden_channels, out_channels)

    def forward(self, x_dict: dict[str, Tensor],
                edge_index_dict: dict) -> Tensor:

        # 1. Project to shared space
        h = {nt: F.gelu(lin(x_dict[nt]))
             for nt, lin in self.lin_dict.items()}

        # 2. Message passing
        for conv in self.convs:
            h = conv(h, edge_index_dict)
            h = {k: F.gelu(v) for k, v in h.items()}

        # 3. Classify target nodes
        return self.classifier(h[self.target_type])


# ─────────────────────────────────────────────
# 3. Training & evaluation
# ─────────────────────────────────────────────

def train_step(model, data, optimizer, target: str = 'author'):
    model.train()
    optimizer.zero_grad()
    out  = model(data.x_dict, data.edge_index_dict)
    mask = data[target].train_mask
    loss = F.cross_entropy(out[mask], data[target].y[mask])
    loss.backward()
    optimizer.step()
    return loss.item()


@torch.no_grad()
def evaluate(model, data, target: str = 'author'):
    model.eval()
    out  = model(data.x_dict, data.edge_index_dict)
    pred = out.argmax(dim=-1)
    results = {}
    for split in ['train', 'val', 'test']:
        mask    = data[target][f'{split}_mask']
        correct = (pred[mask] == data[target].y[mask]).sum().item()
        results[split] = correct / mask.sum().item()
    return results


# ─────────────────────────────────────────────
# 4. Explainability with HeteroExplainer
# ─────────────────────────────────────────────

def explain_nodes(model, data, node_indices: list[int],
                  target: str = 'author', num_classes: int = 4):
    """
    Runs GNNExplainer (via PyG's Explainer framework) on a list of
    author node indices and prints:
      • Predicted class
      • Top-5 most important input features (by gradient × activation mask)
      • Per-relation edge-mask statistics (mean importance per edge type)

    GNNExplainer optimises a soft edge-mask and a feature-mask so that
    the masked sub-graph produces the same prediction as the full graph,
    isolating the most relevant neighbourhood structure and features.
    """

    # ── Build the Explainer ───────────────────────────────────────────────────
    explainer = Explainer(
        model=model,
        algorithm=GNNExplainer(epochs=200, lr=0.01),
        explanation_type='model',          # explain the model's own prediction
        node_mask_type='attributes',       # produce per-feature importance mask
        edge_mask_type='object',           # produce per-edge importance mask
        model_config=dict(
            mode='multiclass_classification',
            task_level='node',
            return_type='raw',             # model returns raw logits
        ),
    )

    model.eval()
    CLASS_NAMES = {0: 'DB', 1: 'DM', 2: 'IR', 3: 'ML'}

    print("=" * 60)
    print("EXPLAINABILITY  (GNNExplainer)")
    print("=" * 60)

    for node_idx in node_indices:

        # ── Forward pass for predicted label ─────────────────────────────────
        with torch.no_grad():
            logits = model(data.x_dict, data.edge_index_dict)
            pred   = logits[node_idx].argmax().item()
            true   = data[target].y[node_idx].item()

        print(f"\nAuthor node {node_idx:4d}  |  "
              f"true={CLASS_NAMES[true]}  pred={CLASS_NAMES[pred]}"
              f"  {'✓' if pred == true else '✗'}")

        # ── Run explainer ────────────────────────────────────────────────────
        # For heterogeneous graphs we pass x_dict / edge_index_dict;
        # the index refers to the position in the target node type.
        explanation = explainer(
            x=data.x_dict,
            edge_index=data.edge_index_dict,
            index=node_idx,
            target=logits,                 # supply cached logits as target
        )

        # ── Feature importance ────────────────────────────────────────────────
        # node_mask is a dict {node_type: Tensor[num_nodes, num_features]}
        # We look at the row corresponding to our target node.
        if explanation.node_mask is not None:
            author_mask = explanation.node_mask  # shape [N_author, F]
            feat_imp    = author_mask[node_idx]  # shape [F]
            top5_vals, top5_idx = feat_imp.topk(5)
            print("  Top-5 feature dims (author BoW):")
            for rank, (fi, fv) in enumerate(
                    zip(top5_idx.tolist(), top5_vals.tolist()), 1):
                print(f"    {rank}. dim={fi:4d}  importance={fv:.4f}")
        elif hasattr(explanation, 'node_feat_mask'):
            # Older PyG API fallback
            feat_imp = explanation.node_feat_mask
            top5_vals, top5_idx = feat_imp.topk(5)
            print("  Top-5 feature dims (author BoW):")
            for rank, (fi, fv) in enumerate(
                    zip(top5_idx.tolist(), top5_vals.tolist()), 1):
                print(f"    {rank}. dim={fi:4d}  importance={fv:.4f}")

        # ── Edge importance by relation type ──────────────────────────────────
        # edge_mask is a dict {edge_type: Tensor[num_edges]} with values in [0,1]
        if explanation.edge_mask is not None:
            print("  Edge-mask stats (mean importance per relation):")
            for etype, emask in explanation.edge_mask.items():
                if emask.numel() == 0:
                    continue
                print(f"    {str(etype):50s}  "
                      f"mean={emask.mean():.4f}  "
                      f"max={emask.max():.4f}  "
                      f"nnz>{0.5:.1f}={(emask > 0.5).sum().item()}")

    print("\n" + "=" * 60)


# ─────────────────────────────────────────────
# 5. Main
# ─────────────────────────────────────────────

def main():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}\n")

    # ── Data ──────────────────────────────────────────────────────────────────
    data = load_dblp().to(device)

    TARGET      = 'author'
    NUM_CLASSES = int(data[TARGET].y.max().item()) + 1   # 4

    # ── Model ─────────────────────────────────────────────────────────────────
    model = HGT(
        metadata=data.metadata(),
        hidden_channels=64,
        out_channels=NUM_CLASSES,
        num_heads=4,
        num_layers=2,
        target_type=TARGET,
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-3, weight_decay=1e-4)

    # ── Training loop ─────────────────────────────────────────────────────────
    print(f"{'Epoch':>6}  {'Loss':>8}  {'Train':>7}  {'Val':>7}  {'Test':>7}")
    print("─" * 47)

    for epoch in range(1, 101):
        loss = train_step(model, data, optimizer, TARGET)
        acc  = evaluate(model, data, TARGET)
        if epoch % 10 == 0:
            print(f"{epoch:>6}  {loss:>8.4f}  "
                  f"{acc['train']:>7.2%}  {acc['val']:>7.2%}  {acc['test']:>7.2%}")

    # ── Explainability ────────────────────────────────────────────────────────
    # Pick a few test-set authors to explain
    test_mask   = data[TARGET].test_mask.cpu()
    test_nodes  = test_mask.nonzero(as_tuple=True)[0][:5].tolist()   # first 5

    print(f"\nRunning GNNExplainer on {len(test_nodes)} test-set author nodes …\n")
    explain_nodes(model, data, node_indices=test_nodes,
                  target=TARGET, num_classes=NUM_CLASSES)


if __name__ == '__main__':
    main()