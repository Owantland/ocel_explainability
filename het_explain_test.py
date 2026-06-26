import torch
import torch.nn.functional as F
from torch_geometric.loader import DataLoader
from torch_geometric.nn import HGTConv, Linear
import pandas as pd

class HGT(torch.nn.Module):
    def __init__(self, hidden_channels, out_channels, num_layers, num_heads, data, viewpoint):
        super().__init__()

        self.viewpoint = viewpoint
        self.lin_dict = torch.nn.ModuleDict()
        for node_type in data.node_types:
            self.lin_dict[node_type] = Linear(-1, hidden_channels)

        self.convs = torch.nn.ModuleList()
        for _ in range(num_layers):
            conv = HGTConv(hidden_channels, hidden_channels, data.metadata(),
                           num_heads)
            self.convs.append(conv)

        self.lin = Linear(hidden_channels, out_channels)

    def forward(self, x_dict, edge_index_dict):
        x_dict = {
            node_type: self.lin_dict[node_type](x).relu_()
            for node_type, x in x_dict.items()
        }

        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict)

        return self.lin(x_dict[self.viewpoint])

def train():
    model.train()
    total_loss = 0
    for batch in train_loader:
        batch = batch.to(device)
        optimizer.zero_grad()
        out = model(batch.x_dict, batch.edge_index_dict)
        loss = F.mse_loss(out[batch['node'].train_mask], batch['node'].y[batch['node'].train_mask].float())
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    return total_loss / len(train_loader)


train_data = torch.load(f"files/hetero_structures/order_management/train_graphs_sg.pt", weights_only=False)
val_data = torch.load(f"files/hetero_structures/order_management/val_graphs_sg.pt", weights_only=False)
test_data = torch.load(f"files/hetero_structures/order_management/test_graphs_sg.pt", weights_only=False)

# Create appropriate loaders
train_loader = DataLoader(train_data, batch_size=64, shuffle=True)
val_loader = DataLoader(val_data, batch_size=64)
test_loader = DataLoader(test_data, batch_size=64)
data = train_data[0]

model_name = "HGT"
model = HGT(hidden_channels=128, out_channels=6, num_heads=2, num_layers=2, data=data, viewpoint='Orders')
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
data, model = data.to(device), model.to(device)
optimizer = torch.optim.AdamW(model.parameters(), lr=0.002)
model.load_state_dict(torch.load('models/outer/HGT_HGTLoader.pth', weights_only=False))


with torch.no_grad():  # Initialize lazy modules.
    out = model(data.x_dict, data.edge_index_dict)


