"""
    A Graph Classification example

    Graph classification refers to the problem of classifiying entire graphs (in contrast to nodes),
    given a dataset of graphs, based on some structural graph properties Here, we want to embed entire graphs, and we
    want to embed those graphs in such a way so that they are linearly separable given a task at hand.
"""

import torch
from torch_geometric.datasets import TUDataset
from torch_geometric.loader import DataLoader

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool

from tqdm import tqdm

"""
    In this model Here, we make use of the GCNConv with ReLU(x)=max(x,0) activation for obtaining localized node 
    embeddings, before we apply our final classifier on top of a graph readout layer.
"""
class GCN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GCN, self).__init__()
        torch.manual_seed(12345)
        self.conv1 = GCNConv(num_node_features, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, hidden_channels)
        self.conv3 = GCNConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index, batch):
        # 1. Obtain node embeddings
        x = self.conv1(x, edge_index)
        x = x.relu()
        x = self.conv2(x, edge_index)
        x = x.relu()
        x = self.conv3(x, edge_index)

        # 2. Readout layer
        x = global_mean_pool(x, batch)  # [hidden_channels, batch_size]

        # 3. Apply a final classifier
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin(x)

        return x

class GNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GNN, self).__init__()
        torch.manual_seed(12345)
        self.conv1 = GraphConv(num_node_features, hidden_channels)
        self.conv2 = GraphConv(hidden_channels, hidden_channels)
        self.conv3 = GraphConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index, batch):
        x = self.conv1(x, edge_index)
        x = x.relu()
        x = self.conv2(x, edge_index)
        x = x.relu()
        x = self.conv3(x, edge_index)

        x = global_mean_pool(x, batch)

        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin(x)

        return x

def train():
    model.train()

    for data in train_loader:  # Iterate in batches over the training dataset.
         out = model(data.x, data.edge_index, data.batch)  # Perform a single forward pass.
         loss = criterion(out, data.y)  # Compute the loss.
         loss.backward()  # Derive gradients.
         optimizer.step()  # Update parameters based on gradients.
         optimizer.zero_grad()  # Clear gradients.

def test(loader):
     model.eval()

     correct = 0
     for data in loader:  # Iterate in batches over the training/test dataset.
         out = model(data.x, data.edge_index, data.batch)
         pred = out.argmax(dim=1)  # Use the class with highest probability.
         correct += int((pred == data.y).sum())  # Check against ground-truth labels.
     return correct / len(loader.dataset)  # Derive ratio of correct predictions.


train_loader = torch.load(f"files/hetero_structures/order_management/PayOrder/deliveryOnTime/train_graphs_hom.pt", weights_only=False)
val_loader = torch.load(f"files/hetero_structures/order_management/PayOrder/deliveryOnTime/val_graphs_hom.pt", weights_only=False)
test_loader = torch.load(f"files/hetero_structures/order_management/PayOrder/deliveryOnTime/test_graphs_hom.pt", weights_only=False)

for step, data in enumerate(train_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {data.num_graphs}')
    print(data)
    print()

# Define some variables for the models
num_node_features = 11
num_classes = 4

"""
    Improving Accuracy

    Can we do better than this? As multiple papers pointed out (Xu et al. (2018), Morris et al. (2018)), applying
    neighborhood normalization decreases the expressivity of GNNs in distinguishing certain graph structures.
    An alternative formulation (Morris et al. (2018)) omits neighborhood normalization completely and adds a simple
    skip-connection to the GNN layer in order to preserve central node information. This layer is implemented under the
    name GraphConv in PyTorch Geometric.
    This new implementation gets us up to an 82% accuracy
"""
model = GNN(hidden_channels=64)
print(model)

optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
criterion = torch.nn.CrossEntropyLoss()
# pbar = tqdm(range(1, 301))
# min_val = 0.5
# for epoch in pbar:
#     train()
#     train_acc = test(train_loader)
#     val_acc = test(val_loader)
#     print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Test Acc: {val_acc:.4f}')
#     if val_acc > min_val:
#         print("New best!")
#         min_val = val_acc
#         torch.save(model.state_dict(), f"./GNN_hom.pth")
# pbar.close()

"""
    Load the best saved model so we can make some explainer tests
"""
state_dict_path = ("./GNN_hom.pth")
model.load_state_dict(torch.load(state_dict_path))
test_acc = test(test_loader)
print(f'Final ACCs: Test Acc: {test_acc:.4f}')

