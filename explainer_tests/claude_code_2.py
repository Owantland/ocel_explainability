"""
Heterogeneous Graph Node Classification with PyTorch Geometric
==============================================================

Dataset : DBLP (built-in PyG dataset)
  - Node types  : 'author' (target), 'paper', 'term', 'conference'
  - Edge types  : author–paper, paper–term, paper–conference (+ reverses)
  - Task        : Classify authors into one of 4 research areas

Model   : HGT  (Heterogeneous Graph Transformer)

Explainability — two complementary methods:
  1. GNNExplainer  (feature-mask only — avoids the edge-gradient issue on
                    heterogeneous graphs with discrete edge indices)
  2. HGT Attention Weights  (reads the alpha scores stored by HGTConv during
                    the forward pass — gives per-edge, per-relation importance
                    without any gradient requirement)
"""

import torch
import torch.nn.functional as F
from torch import Tensor
from collections import defaultdict

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
      conference  (20 nodes,    no features → ones after Constant transform)

    Relations (undirected after ToUndirected):
      (author, to, paper), (paper, to, term), (paper, to, conference)

    Labels: 4 research areas  (0=DB, 1=DM, 2=IR, 3=ML)
    """
    transform = T.Compose([
        T.Constant(node_types=['conference']),
        T.ToUndirected(),
    ])
    dataset = DBLP(root=root, transform=transform)
    data    = dataset[0]

    print("DBLP graph loaded:")
    print(f"  Node types : {data.node_types}")
    print(f"  Edge types : {[str(e) for e in data.edge_types]}")
    for nt in data.node_types:
        n     = data[nt].num_nodes
        has_y = hasattr(data[nt], 'y') and data[nt].y is not None
        print(f"    {nt:12s}  nodes={n}"
              + (f"  classes={data[nt].y.max().item()+1}" if has_y else ""))
    print()
    return data


# ─────────────────────────────────────────────
# 2. HGT model  (attention-storing variant)
# ─────────────────────────────────────────────

class HGT(torch.nn.Module):
    """
    Heterogeneous Graph Transformer.

    Key addition for explainability:
      Each HGTConv layer is created with `return_attention_weights` support.
      After every forward pass `self.attention_weights` holds a list of dicts:
        { edge_type_str : (edge_index, alpha) }
      where alpha has shape [num_edges, num_heads].
    """

    def __init__(self, metadata, hidden_channels: int, out_channels: int,
                 num_heads: int = 4, num_layers: int = 2,
                 target_type: str = 'author'):
        super().__init__()
        self.target_type      = target_type
        self.num_heads        = num_heads
        self.attention_weights: list[dict] = []   # populated in forward()

        node_types, _ = metadata
        self.lin_dict  = torch.nn.ModuleDict({
            nt: Linear(-1, hidden_channels) for nt in node_types
        })
        self.convs = torch.nn.ModuleList([
            HGTConv(hidden_channels, hidden_channels,
                    metadata=metadata, heads=num_heads)
            for _ in range(num_layers)
        ])
        self.classifier = Linear(hidden_channels, out_channels)

    def forward(self, x_dict: dict[str, Tensor],
                edge_index_dict: dict,
                return_attention: bool = False) -> Tensor:

        h = {nt: F.gelu(lin(x_dict[nt]))
             for nt, lin in self.lin_dict.items()}

        self.attention_weights = []
        for conv in self.convs:
            if return_attention:
                # return_attention_weights=True makes HGTConv return
                # (out_dict, {edge_type: (edge_index, alpha)})
                h, attn = conv(h, edge_index_dict,
                               return_attention_weights=True)
                self.attention_weights.append(attn)
            else:
                h = conv(h, edge_index_dict)
            h = {k: F.gelu(v) for k, v in h.items()}

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
# 4a. Explainer — GNNExplainer (feature masks)
# ─────────────────────────────────────────────

def build_gnn_explainer(model) -> Explainer:
    """
    Uses GNNExplainer with ONLY feature (node attribute) masks.

    Why not edge masks?
    ───────────────────
    PyG's GNNExplainer computes edge importance by injecting a learnable
    scalar into the aggregation path. For homogeneous graphs this is done
    by multiplying edge_index values — but for heterogeneous graphs the
    edge indices are stored in a Python dict and PyG cannot propagate
    gradients through the dict look-up. This raises:

        ValueError: Could not compute gradients for edge masks of type ...

    The correct workaround is to set edge_mask_type=None and instead read
    the HGT attention weights directly (see explain_attention_weights below),
    which gives richer per-edge, per-head importance without any gradient issue.
    """
    return Explainer(
        model=model,
        algorithm=GNNExplainer(epochs=200, lr=0.01),
        explanation_type='model',
        node_mask_type='attributes',   # feature-importance mask ✓
        edge_mask_type=None,           # disabled — use attention weights instead
        model_config=dict(
            mode='multiclass_classification',
            task_level='node',
            return_type='raw',
        ),
    )


# ─────────────────────────────────────────────
# 4b. Explainer — HGT attention weights
# ─────────────────────────────────────────────

@torch.no_grad()
def explain_attention_weights(model, data, node_idx: int,
                               target: str = 'author'):
    """
    Extracts attention-based edge importance for a single target node.

    HGTConv computes a multi-head attention score alpha ∈ [0,1] for every
    edge. We:
      1. Run a forward pass with return_attention=True to populate
         model.attention_weights (one dict per layer).
      2. For each layer and each edge type, find all edges whose
         *destination* is `node_idx` (relevant for the target node).
      3. Average alpha across heads → one scalar per incoming edge.
      4. Report the top-k most attended neighbours per relation type.

    This is gradient-free and works correctly for heterogeneous graphs.
    """
    model.eval()
    _ = model(data.x_dict, data.edge_index_dict, return_attention=True)

    # edge types that point TO the target node type
    incoming_etypes = [et for et in data.edge_types if et[2] == target]

    results = defaultdict(list)   # edge_type_str → [(src_id, mean_alpha), ...]

    for layer_idx, attn_dict in enumerate(model.attention_weights):
        for etype, (edge_index, alpha) in attn_dict.items():
            if etype[2] != target:
                continue

            src_nodes = edge_index[0]   # source node ids
            dst_nodes = edge_index[1]   # destination node ids

            # Keep only edges arriving at our node of interest
            mask        = (dst_nodes == node_idx)
            src_focused = src_nodes[mask]
            alpha_mean  = alpha[mask].mean(dim=-1)   # average over heads

            for src, a in zip(src_focused.tolist(), alpha_mean.tolist()):
                results[f"L{layer_idx+1} {etype}"].append((src, a))

    return results


# ─────────────────────────────────────────────
# 4c. Combined explain routine
# ─────────────────────────────────────────────

def explain_nodes(model, data, node_indices: list[int],
                  target: str = 'author'):
    """
    For each node prints:
      A) Top-5 most important input features  (GNNExplainer feature mask)
      B) Top-5 most attended neighbours       (HGT attention weights, per layer
                                               and per incoming relation type)
    """
    CLASS_NAMES = {0: 'DB', 1: 'DM', 2: 'IR', 3: 'ML'}
    explainer   = build_gnn_explainer(model)
    model.eval()

    print("=" * 65)
    print("EXPLAINABILITY")
    print("  Method A — GNNExplainer  : per-feature importance (gradient)")
    print("  Method B — Attention     : per-edge importance (attention α)")
    print("=" * 65)

    # Cache logits once — reused as the 'target' tensor for the explainer
    with torch.no_grad():
        logits = model(data.x_dict, data.edge_index_dict)

    for node_idx in node_indices:
        pred = logits[node_idx].argmax().item()
        true = data[target].y[node_idx].item()

        print(f"\n{'─'*65}")
        print(f"Author node {node_idx:4d}  |  "
              f"true={CLASS_NAMES[true]}  "
              f"pred={CLASS_NAMES[pred]}  "
              f"{'✓' if pred == true else '✗'}")

        # ── A: GNNExplainer feature mask ─────────────────────────────────────
        print("\n  [A] Feature importance (GNNExplainer, author BoW dims):")
        try:
            explanation = explainer(
                x=data.x_dict,
                edge_index=data.edge_index_dict,
                index=node_idx,
                target=logits,
            )

            # node_mask shape: [N_author, F]  or stored as node_feat_mask
            feat_mask = (explanation.node_mask[node_idx]
                         if explanation.node_mask is not None
                         else explanation.node_feat_mask)

            top_vals, top_idx = feat_mask.topk(5)
            for rank, (fi, fv) in enumerate(
                    zip(top_idx.tolist(), top_vals.tolist()), 1):
                print(f"    {rank}. dim={fi:4d}  score={fv:.4f}")

        except Exception as exc:
            print(f"    (GNNExplainer error: {exc})")

        # ── B: Attention weights ──────────────────────────────────────────────
        print("\n  [B] Most attended neighbours (HGT attention α):")
        attn_results = explain_attention_weights(model, data, node_idx, target)

        if not attn_results:
            print("    (no incoming attention edges found)")
        else:
            for rel_label, pairs in attn_results.items():
                if not pairs:
                    continue
                # Sort by attention score descending, keep top 5
                top = sorted(pairs, key=lambda x: x[1], reverse=True)[:5]
                print(f"\n    Relation: {rel_label}")
                for rank, (src, a) in enumerate(top, 1):
                    print(f"      {rank}. neighbour_id={src:5d}  α={a:.4f}")

    print(f"\n{'='*65}\n")


# ─────────────────────────────────────────────
# 5. Main
# ─────────────────────────────────────────────

def main():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}\n")

    data        = load_dblp().to(device)
    TARGET      = 'author'
    NUM_CLASSES = int(data[TARGET].y.max().item()) + 1   # 4

    model = HGT(
        metadata=data.metadata(),
        hidden_channels=64,
        out_channels=NUM_CLASSES,
        num_heads=4,
        num_layers=2,
        target_type=TARGET,
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-3, weight_decay=1e-4)

    # ── Train ──────────────────────────────────────────────────────────────────
    print(f"{'Epoch':>6}  {'Loss':>8}  {'Train':>7}  {'Val':>7}  {'Test':>7}")
    print("─" * 47)
    for epoch in range(1, 101):
        loss = train_step(model, data, optimizer, TARGET)
        acc  = evaluate(model, data, TARGET)
        if epoch % 10 == 0:
            print(f"{epoch:>6}  {loss:>8.4f}  "
                  f"{acc['train']:>7.2%}  {acc['val']:>7.2%}  {acc['test']:>7.2%}")

    # ── Explain ────────────────────────────────────────────────────────────────
    test_mask  = data[TARGET].test_mask.cpu()
    test_nodes = test_mask.nonzero(as_tuple=True)[0][:3].tolist()  # first 3
    print(f"\nRunning explainability on {len(test_nodes)} test-set authors …\n")
    explain_nodes(model, data, node_indices=test_nodes, target=TARGET)


if __name__ == '__main__':
    main()