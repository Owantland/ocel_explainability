"""
    A simple integreation of explainability to one of our models

    Based on graph_class.py, follows a dataset as it trains a model and then is used for explainability.
"""

import torch

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader

from torch_geometric.explain import Explainer, Explanation
from torch_geometric.explain import GNNExplainer
from torch_geometric.explain.metric import (
   fidelity,
   characterization_score,
   fidelity_curve_auc,
)

from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas as pd

"""
    In this model Here, we make use of the GCNConv with ReLU(x)=max(x,0) activation for obtaining localized node 
    embeddings, before we apply our final classifier on top of a graph readout layer.
"""
class GCN(torch.nn.Module):
    def __init__(self, hidden_channels):
        # Init
        super(GCN, self).__init__()

        # GCN Layers
        self.conv1 = GCNConv(num_node_features, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, hidden_channels)
        self.conv3 = GCNConv(hidden_channels, hidden_channels)
        self.conv4 = GCNConv(hidden_channels, hidden_channels)

        # Output Layer
        self.lin = Linear(hidden_channels, 11)

    def forward(self, x, edge_index, batch):
        # 1. Obtain node embeddings
        x = self.conv1(x, edge_index)
        x = F.tanh(x)
        x = self.conv2(x, edge_index)
        x = F.tanh(x)
        x = self.conv3(x, edge_index)
        x = F.tanh(x)
        x = self.conv4(x, edge_index)
        x = F.tanh(x)

        # 2. Readout layer A.K.A. Global pooling for graph classification
        x = global_mean_pool(x, batch)  # [hidden_channels, batch_size]

        # 3. Apply a final classifier
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin(x)
        return x

class GNN(torch.nn.Module):
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

def train():
    model.train()
    total_loss = 0
    for data in train_loader:  # Iterate in batches over the training dataset.
        # Move batch to device
        data = data.to(device)
        # Forward pass
        optimizer.zero_grad()  # Clear gradients.
        out = model(data.x, data.edge_index, data.batch)
        # Loss and backpropagation
        loss = criterion(out, data.y)  # Compute the loss.
        loss.backward()  # Derive gradients.
        optimizer.step()  # Update parameters based on gradients.
        # Loss calculation
        total_loss += loss.item() * data.num_graphs
    return total_loss / len(train_data)

@torch.no_grad()
def test(loader):
     model.eval()
     total_mae = 0
     for data in loader:
         data.to(device)
         # Forward Pass
         out = model(data.x, data.edge_index, data.batch)
         loss = criterion(out, data.y)
         # De-normalize before computing MAE so it's in original units
         pred = out * target_std.to(device) + target_mean.to(device)
         true = data.y #* target_std.to(device) + target_mean.to(device)
         total_mae += (pred - true).abs().sum().item()
     return total_mae / len(loader.dataset)

def decode_epoch(epoch_val):
    timestamp = pd.Timestamp(epoch_val, unit='s')
    return timestamp

def decode_time(total_secs):
    timestamp = pd.Timedelta(round(total_secs, 2), unit='s')
    return timestamp

"""
    Step 1: Loading the dataset

    Load the list of data structures saved from the previous step and preprocess them:
        * Standardize the Y value for ease of use in the GNN architecture
        * Create a dataloader to more efficiently handle the large swat of data
"""

train_data = torch.load(f"files/hetero_structures/order_management/train_graphs_hom.pt", weights_only=False)
val_data = torch.load(f"files/hetero_structures/order_management/val_graphs_hom.pt", weights_only=False)
test_data = torch.load(f"files/hetero_structures/order_management/test_graphs_hom.pt", weights_only=False)


def normalize_target(data):
    data.y = (data.y - target_mean) / target_std
    return data

train_data = [normalize_target(d) for d in train_data]
ys = torch.cat([d.y for d in train_data])

# Create appropriate loaders
train_loader = DataLoader(train_data, batch_size=64, shuffle=True)
val_loader = DataLoader(val_data, batch_size=64)
test_loader = DataLoader(test_data, batch_size=64)

for step, data in enumerate(train_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {data.num_graphs}')
    print(data)

for step, data in enumerate(val_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {data.num_graphs}')
    print(data)

for step, data in enumerate(test_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {data.num_graphs}')
    print(data)

"""
    Step 2: Creating the model

    Begin by creating the model object with the appropriate values as well as an optimizer function
"""
# Define some variables for the models
num_node_features = 11
model = GNN(in_channels=num_node_features, hidden_channels=64, num_layers=3)
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)
print(model)

"""
    Step 3: Training and validation
"""
criterion = F.mse_loss
device = torch.device("cpu")

# Train the model
pbar = tqdm(range(1, 51))
best_val_mae = float("inf")
for epoch in pbar:
    train_loss = train()
    val_mae = test(val_loader)
    print(f'Epoch: {epoch:03d}, Train Loss: {train_loss:.4f}, Val MAE: {val_mae:.4f}')

    if val_mae < best_val_mae:
        print("New best!")
        best_val_mae = val_mae
        torch.save(model.state_dict(), f"./GNN_Delivery_Reg.pth")
pbar.close()

test_mae = test(test_loader)
print(f"\nFinal Test MAE: {test_mae:.4f} (best Val MAE: {best_val_mae:.4f})")

"""
    Load the best saved model so we can make some explainer tests
"""
train_data = torch.load(f"files/hetero_structures/order_management/train_graphs_hom.pt", weights_only=False)
val_data = torch.load(f"files/hetero_structures/order_management/val_graphs_hom.pt", weights_only=False)
test_data = torch.load(f"files/hetero_structures/order_management/test_graphs_hom.pt", weights_only=False)
test_loader = DataLoader(test_data, batch_size=64)
device = torch.device('cpu')
criterion = F.mse_loss
ys = torch.cat([d.y for d in train_data])
target_mean, target_std = ys.mean(), ys.std()

state_dict_path = ("files/models/order_management/TimeUntil_PackageDelivered.pth")
model = GNN(in_channels=11, hidden_channels=64, num_layers=3)
model.load_state_dict(torch.load(state_dict_path))
test_mae = test(test_loader)
test_mae = decode_time(test_mae)

# Create another loader to make batches for explainability
# explain_loader = DataLoader(train_data, batch_size=64)
# batch = next(iter(explain_loader)).to(device)
index = 15

data = test_data[index]
print(data.vwpnt_id.item())
print(decode_epoch((data.timestamp).item()))
batch = torch.zeros(data.num_nodes, dtype=torch.long, device=device)

model_explainer = Explainer(
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
model_explanation = model_explainer(data.x, data.edge_index, batch=batch)
subgraph = model_explanation.get_explanation_subgraph()
complement_subgraph = model_explanation.get_complement_subgraph()
model_explanation.visualize_feature_importance("model_topk.png", top_k=10)
model_explanation.visualize_graph('red_graph.png', backend="graphviz")
