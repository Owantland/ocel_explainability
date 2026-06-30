import torch
from torch_geometric.nn import HGTConv, Linear

class HGT(torch.nn.Module):
    def __init__(self, hidden_channels, out_channels, num_layers, num_heads, data, viewpoint):
        super().__init__()

        self.viewpoint = viewpoint
        self.lin_dict = torch.nn.ModuleDict()
        for node_type in data.node_types:
            self.lin_dict[node_type] = Linear(-1, hidden_channels)

        self.convs = torch.nn.ModuleList()
        for _ in range(num_layers):
            conv = HGTConv(hidden_channels, hidden_channels, data.metadata(), num_heads)
            self.convs.append(conv)

        self.lin = Linear(hidden_channels, out_channels)

    def forward(self, x_dict, edge_index_dict):
        x_dict = {
            node_type: self.lin_dict[node_type](x).relu_()
            for node_type, x in x_dict.items()
        }

        for conv in self.convs:
            new_x_dict = conv(x_dict, edge_index_dict)
            # Residual connection: add previous embeddings before activation
            x_dict = {nt: (new_x_dict[nt] + x_dict[nt]).relu_() for nt in new_x_dict}

        return self.lin(x_dict[self.viewpoint])