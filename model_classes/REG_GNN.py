import torch

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.nn import global_mean_pool

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