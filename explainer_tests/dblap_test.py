import os.path as osp

import torch
import torch.nn.functional as F

import torch_geometric
import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP, OGB_MAG
from torch_geometric.nn import HANConv, Linear, HeteroConv, GCNConv, SAGEConv, GATConv, Linear, to_hetero
from torch_geometric.explain import CaptumExplainer, Explainer, GNNExplainer

from tqdm import tqdm

class HAN(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, metadata, heads=8):
        """
        Initializes the HAN model.

        Args:
            in_channels (int or dict): Size of input features. If int, assumes all node types
                                        have the same feature size. If dict, maps node types
                                        to their feature sizes.
            hidden_channels (int): Size of the hidden embeddings.
            out_channels (int): Number of output classes.
            metadata (tuple): Metadata tuple containing node types and edge types,
                              obtained from data.metadata().
            heads (int, optional): Number of attention heads. Defaults to 8.
        """
        super().__init__()
        # HANConv layer automatically handles multiple meta-paths based on metadata
        # It performs node-level and semantic-level attention.
        # We specify '-1' for in_channels to let HANConv infer input sizes
        # per node type from the metadata and input data.
        self.conv1 = HANConv(in_channels=-1, out_channels=hidden_channels,
                             metadata=metadata, heads=heads, dropout=0.6)
        self.conv2 = HANConv(in_channels=hidden_channels, out_channels=out_channels,
                             metadata=metadata, heads=1, dropout=0.6) # Usually 1 head for final layer

    def forward(self, x_dict, edge_index_dict):
        """
        Forward pass of the HAN model.

        Args:
            x_dict (dict): Dictionary mapping node types to their feature tensors.
            edge_index_dict (dict): Dictionary mapping edge types to their edge index tensors.

        Returns:
            torch.Tensor: Output logits for the target node type ('author').
        """
        # Note: HANConv returns a dictionary of embeddings for all node types reached
        #       through the defined meta-paths originating from the source nodes.
        x_dict = self.conv1(x_dict, edge_index_dict)
        # Apply activation (optional, depends on layer implementation details)
        # x_dict = {key: F.elu(x) for key, x in x_dict.items()} # Example activation

        x_dict = self.conv2(x_dict, edge_index_dict)

        # We only need the output for the 'author' node type for our classification task
        return x_dict['author']

class HeteroGNN(torch.nn.Module):
    def __init__(self, data, hidden_channels, out_channels, num_layers):
        super().__init__()

        self.convs = torch.nn.ModuleList()
        for _ in range(num_layers):
            conv_dict = {}
            for edge in data.edge_types:
                conv_dict[edge] = GATConv((-1, -1), hidden_channels, add_self_loops=False)
            print(conv_dict)
            conv = HeteroConv(conv_dict, aggr='sum')
            self.convs.append(conv)

        self.lin = Linear(hidden_channels, out_channels)

    def forward(self, x_dict, edge_index_dict):
        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict)
            x_dict = {key: x.relu() for key, x in x_dict.items()}
        return self.lin(x_dict['author'])

class GAT(torch.nn.Module):
    def __init__(self, hidden_channels, out_channels):
        super().__init__()
        self.conv1 = GATConv((-1, -1), hidden_channels, add_self_loops=False)
        self.lin1 = Linear(-1, hidden_channels)
        self.conv2 = GATConv((-1, -1), out_channels, add_self_loops=False)
        self.lin2 = Linear(-1, out_channels)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index) + self.lin1(x)
        x = x.relu()
        x = self.conv2(x, edge_index) + self.lin2(x)
        return x

class Model(torch.nn.Module):
    def __init__(self, hidden_channels, out_channels):
        super().__init__()
        self.encoder = GAT(hidden_channels, out_channels)
        self.encoder = to_hetero(self.encoder, data.metadata(), aggr='sum')

    def forward(self, x_dict, edge_index_dict):
        z_dict = self.encoder(x_dict, edge_index_dict)
        return z_dict['author']

def train():
    model.train()
    optimizer.zero_grad()
    out = model(data.x_dict, data.edge_index_dict)
    mask = data['author'].train_mask
    loss = F.cross_entropy(out[mask], data['author'].y[mask])
    loss.backward()
    optimizer.step()
    return float(loss)


@torch.no_grad()
def test():
    model.eval()
    pred = model(data.x_dict, data.edge_index_dict).argmax(dim=-1)

    accs = []
    for split in ['train_mask', 'val_mask', 'test_mask']:
        mask = data['author'][split]
        acc = (pred[mask] == data['author'].y[mask]).sum() / mask.sum()
        accs.append(float(acc))
    return accs

# # Import the dataset
path = osp.join(osp.dirname(osp.realpath(__file__)), '../../data/DBLP')
dataset = DBLP(path, transform=T.Constant(node_types='conference'))

# dataset = OGB_MAG(root='./data', preprocess='metapath2vec')
data = dataset[0]
print(data)
transform = T.Compose([
    T.NormalizeFeatures(),
    T.ToUndirected() # Ensure graph is undirected for simpler relation handling
])
data = transform(data)
print(data.edge_types)
print(data.edge_index_dict.keys())

# # Define the model
# # model = HAN(in_channels=-1, hidden_channels=128,
# #             out_channels=4, metadata=data.metadata(), heads=8)
#
# # model = HeteroGNN(data, hidden_channels=64, out_channels=4, num_layers=2)
# model = Model(hidden_channels=64, out_channels=4)
#
# device = torch.device('mps')
# data, model = data.to(device), model.to(device)
#
# # Optimizer
# optimizer = torch.optim.Adam(model.parameters(), lr=0.005, weight_decay=0.001)
#
# pbar = tqdm(range(1, 101))
# for epoch in pbar:
#     loss = train()
#     train_acc, val_acc, test_acc = test()
#     print(f'Epoch: {epoch:03d}, Loss: {loss:.4f}, Train: {train_acc:.4f}, '
#           f'Val: {val_acc:.4f}, Test: {test_acc:.4f}')
# pbar.close()
#
# explainer = Explainer(
#     model,  # It is assumed that model outputs a single tensor.
#     algorithm=CaptumExplainer('IntegratedGradients'),
#     explanation_type='model',
#     node_mask_type='attributes',
#     edge_mask_type='object',
#     model_config = dict(
#         mode='multiclass_classification',
#         task_level='node',
#         return_type='probs',  # Model returns probabilities.
#     ),
# )

# # Generate batch-wise heterogeneous explanations for
# # the nodes at index `1` and `3`:
# hetero_explanation = explainer(
#     data.x_dict,
#     data.edge_index_dict,
#     index=torch.tensor([1, 3]),
# )
# print(hetero_explanation.edge_mask_dict)
# print(hetero_explanation.node_mask_dict)

# path = 'feature_importance.png'
# explanation.visualize_feature_importance(path, top_k=10)
# print(f"Feature importance plot has been saved to '{path}'")
#
# path = 'subgraph.pdf'
# explanation.visualize_graph(path)
# print(f"Subgraph visualization plot has been saved to '{path}'")
