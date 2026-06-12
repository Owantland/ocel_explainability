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

"""
    In this model Here, we make use of the GCNConv with ReLU(x)=max(x,0) activation for obtaining localized node 
    embeddings, before we apply our final classifier on top of a graph readout layer.
"""
class GCN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GCN, self).__init__()
        torch.manual_seed(12345)
        self.conv1 = GCNConv(dataset.num_node_features, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, hidden_channels)
        self.conv3 = GCNConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, dataset.num_classes)

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
        self.conv1 = GraphConv(dataset.num_node_features, hidden_channels)
        self.conv2 = GraphConv(hidden_channels, hidden_channels)
        self.conv3 = GraphConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, dataset.num_classes)

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


dataset = TUDataset(root='data/TUDataset', name='MUTAG')
print()
print(f'Dataset: {dataset}:')
print('====================')
print(f'Number of graphs: {len(dataset)}')
print(f'Number of features: {dataset.num_features}')
print(f'Number of classes: {dataset.num_classes}')

"""
    This dataset provides 188 different graphs, and the task is to classify each graph into one out of two classes.
    By inspecting the first graph object of the dataset, we can see that it comes with 17 nodes 
    (with 7-dimensional feature vectors) and 38 edges (leading to an average node degree of 2.24). It also comes with 
    exactly one graph label (y=[1]) and some edge attributes which we won't take into account for now.
"""
data = dataset[0]
print(f"\n{data}")
print('=============================================================')
# Gather some statistics about the first graph.
print(f'Number of nodes: {data.num_nodes}')
print(f'Number of edges: {data.num_edges}')
print(f'Average node degree: {data.num_edges / data.num_nodes:.2f}')
print(f'Has isolated nodes: {data.has_isolated_nodes()}')
print(f'Has self-loops: {data.has_self_loops()}')
print(f'Is undirected: {data.is_undirected()}')

"""
    We can shuffle the data in the dataset and perform a simple train/test split.
"""
torch.manual_seed(12345)
dataset = dataset.shuffle()

train_dataset = dataset[:150]
test_dataset = dataset[150:]

print(f'Number of training graphs: {len(train_dataset)}')
print(f'Number of test graphs: {len(test_dataset)}\n')

"""
    Mini-batching of Graphs
    
    Since graphs in graph classification datasets are usually small, a good idea is to batch the graphs before 
    inputting them into a Graph Neural Network to guarantee full GPU utilization. PyTorch Geometric opts for another 
    approach to achieve parallelization across a number of examples. Here, adjacency matrices are stacked in a diagonal 
    fashion (creating a giant graph that holds multiple isolated subgraphs), and node and target features are simply 
    concatenated in the node dimension.
    PyTorch Geometric automatically takes care of batching multiple graphs into a single giant graph with the help of 
    the torch_geometric.data.DataLoader class.
    
    Here, we opt for a batch_size of 64, leading to 3 (randomly shuffled) mini-batches, containing all 2⋅64+22=150 graphs.
    Furthermore, each Batch object is equipped with a batch vector, which maps each node to its respective graph in 
    the batch:
            batch=[0,…,0,1,…,1,2,…]
"""
train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=64, shuffle=False)

for step, data in enumerate(train_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {data.num_graphs}')
    print(data)
    print()

"""
    Training a GNN
    
    Training a GNN for graph classification usually follows a simple recipe:
        1) Embed each node by performing multiple rounds of message passing
        2) Aggregate node embeddings into a unified graph embedding (readout layer)
        3) Train a final classifier on the graph embedding
    There exists multiple readout layers in literature, but the most common one is to simply take the 
    average of node embeddings.
    
    PyTorch Geometric provides this functionality via torch_geometric.nn.global_mean_pool, which takes in the node 
    embeddings of all nodes in the mini-batch and the assignment vector batch to compute a graph embedding of size 
    [batch_size, hidden_channels] for each graph in the batch.
"""
model = GCN(hidden_channels=64)
print(model)


"""
    We define the model training characteristics in the optimizer and the loss criterion, and train the model for a 
    few epochs.
    
    As one can see, our model reaches around 76% test accuracy. Reasons for the fluctations in accuracy can be explained 
    by the rather small dataset (only 38 test graphs), and usually disappear once one applies GNNs to larger datasets.
"""
model = GCN(hidden_channels=64)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
criterion = torch.nn.CrossEntropyLoss()

for epoch in range(1, 171):
    train()
    train_acc = test(train_loader)
    test_acc = test(test_loader)
    print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Test Acc: {test_acc:.4f}')

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

optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
for epoch in range(1, 201):
    train()
    train_acc = test(train_loader)
    test_acc = test(test_loader)
    print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Test Acc: {test_acc:.4f}')
