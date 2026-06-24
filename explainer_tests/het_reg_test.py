import os.path as osp

import torch
import torch.nn.functional as F

import torch_geometric
import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP
from torch_geometric.nn import HeteroConv, Linear, SAGEConv, GATConv
from torch_geometric.loader import DataLoader

from torchmetrics import F1Score
from tqdm import tqdm
import pandas as pd

class HeteroGNN(torch.nn.Module):
    def __init__(self, hidden_channels, out_channels, num_layers, num_heads, data, viewpoint):
        super().__init__()

        self.viewpoint = viewpoint
        self.lin_dict = torch.nn.ModuleDict()
        for node_type in data.node_types:
            self.lin_dict[node_type] = Linear(-1, hidden_channels[0]*num_heads)

        self.convs = torch.nn.ModuleList()
        for i in range(num_layers):
            conv_dict = {}
            for edge in data.edge_index_dict.keys():
                edge_id = (edge[0], edge[1], edge[2])
                if edge_id == ('Events', 'to', 'Events'):
                    conv_dict[edge_id] = GATConv(-1, hidden_channels[i], add_self_loops=False,
                                                 heads=num_heads)
                else:
                    conv_dict[edge_id] = GATConv((-1,-1), hidden_channels[i], add_self_loops=False,
                                                            heads = num_heads)
            conv = HeteroConv(conv_dict, aggr='sum')
            self.convs.append(conv)

        self.lin_out = Linear(hidden_channels[-1] * num_heads, out_channels)

    def forward(self, x_dict, edge_index_dict):
        for node_type, x in x_dict.items():
            x_dict[node_type] = self.lin_dict[node_type](x)

        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict)
            x_dict = {key: F.relu(x) for key, x in x_dict.items()}

        # Only use the embeddings for 'order' nodes
        order_out = self.lin_out(x_dict[self.viewpoint])

        return order_out

def normalize_target(data, mean, std):
    data[viewpoint_object].y = (data[viewpoint_object].y - mean) / std
    return data

def decode_time(total_secs):
    timestamp = pd.Timedelta(round(total_secs, 2), unit='s')
    return timestamp

def train():
    model.train()

    total_examples = total_loss = 0
    for batch in train_loader:
        optimizer.zero_grad()
        batch = batch.to(device)
        batch_size = len(batch[viewpoint_object].batch)
        out = model(batch.x_dict, batch.edge_index_dict)
        loss = criterion(out[:batch_size], batch[viewpoint_object].y[:batch_size])
        loss.backward()
        optimizer.step()

        total_examples += batch_size
        total_loss += float(loss) * batch_size
    return total_loss / total_examples

@torch.no_grad()
def test(loader):
    model.eval()
    total_examples = total_loss = 0
    # total_mae = 0
    for data in loader:
        data = data.to(device)
        out = model(data.x_dict, data.edge_index_dict)
        batch_size = len(data[viewpoint_object].batch)

        # pred = out[:batch_size] * std.to(device) + mean.to(device)
        # true = data[viewpoint_object].y[:batch_size] * std.to(device) + mean.to(device)
        loss = criterion(out[:batch_size], data[viewpoint_object].y[:batch_size])

        # total_mae += (pred - true).abs().sum().item()
        total_examples += batch_size
        total_loss += float(loss) * batch_size
    return total_loss / total_examples
    # return total_mae / len(loader.dataset)

viewpoint_object = 'Orders'
training_data = torch.load('../files/hetero_structures/order_management/train_graphs_sg.pt', weights_only=False)
val_data = torch.load('../files/hetero_structures/order_management/val_graphs_sg.pt', weights_only=False)
test_data = torch.load('../files/hetero_structures/order_management/test_graphs_sg.pt', weights_only=False)

# Standardize the Y value for ease of use in the GNN architecture
ys = torch.cat([d[viewpoint_object].y for d in training_data])
mean, std = ys.mean(), ys.std()

training_data = [normalize_target(d, mean, std) for d in training_data]
val_data = [normalize_target(d, mean, std) for d in val_data]
test_data = [normalize_target(d, mean, std) for d in test_data]

train_loader = DataLoader(training_data, batch_size=128, shuffle=True)
val_loader = DataLoader(val_data, batch_size=64)
test_loader = DataLoader(test_data, batch_size=64)

# for step, data in enumerate(train_loader):
#     print(f'Step {step + 1}:')
#     print('=======')
#     print(f'Number of graphs in the current batch: {len(data['Orders'].batch)}')
#     print(data)

# for step, data in enumerate(val_loader):
#     print(f'Step {step + 1}:')
#     print('=======')
#     print(f'Number of graphs in the current batch: {len(data['Orders'].batch)}')
#     print(data)

# Model values
num_layers = 5
width_layers = 8
num_heads = 3
hidden_channels = [width_layers] * num_layers
model = HeteroGNN(hidden_channels=hidden_channels, out_channels=1, num_layers=num_layers, num_heads=num_heads,
                  data=training_data[0], viewpoint=viewpoint_object)
optimizer = torch.optim.Adam(model.parameters(), lr=0.005)
# device = torch.device('mps' if torch.backends.mps.is_available() else 'cpu')
device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
model = model.to(device)
criterion = torch.nn.L1Loss()
data = next(iter(train_loader))
with torch.no_grad():  # Initialize lazy modules.
    out = model(data.x_dict, data.edge_index_dict)

learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 +  [0.001] * 10 + [0.0005] * 26
patience = 5
epochs_sg = 10
to_train = [i for i in range(1,6)]
flag = True
while flag:
    for i in to_train:
        model = HeteroGNN(hidden_channels=hidden_channels, out_channels=1, num_layers=num_layers, num_heads=num_heads,
                          data=training_data[0], viewpoint=viewpoint_object)
        best_val = 10e7
        counter = 0
        for epoch, lr in enumerate(learning_rates):
            optimizer = torch.optim.Adam(model.parameters(), lr=lr)
            loss = train()
            val_loss = test(val_loader)
            print(f"{i} - Epoch: {epoch:03d}, LR: {lr}, Loss: {loss:.4f}, Val Loss: {val_loss}")
            if val_loss < best_val:
                print('New Best')
                best_val = val_loss
                torch.save(model.state_dict(), f"../files/models/order_management/het_Regression_PayOrder_{i}.pth")
                counter = 0
            else:
                counter += 1

            if counter > patience:
                print('---')
                break
        if epoch + 1 >= epochs_sg:
            to_train.remove(i)
    if len(to_train) == 0:
        flag = False

# pbar = tqdm(range(1, 101))
# for epoch in pbar:
#     loss = train()
#     val_mae = test(val_loader)
#     print(f"Epoch: {epoch:03d}, Loss: {loss:.4f}, Val MAE: {decode_time(val_mae)}")
#
#     if val_mae < best_val:
#         best_val = val_mae
#         print("Best value found!")
#         torch.save(model.state_dict(), f"../files/models/order_management/het_Regression.pt")
# pbar.close()
#
test_loss = test(test_loader)
# test_mae = decode_time(test_mae)
print(f'Final MAE: {test_loss} \nMean: {mean.item()}\nSTD: {std.item()}')

# We have a simple test setup. We now have to try regression