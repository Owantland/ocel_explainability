import torch
from torch_geometric.nn import HGTConv, Linear

class HGT(torch.nn.Module):
    def __init__(self, hidden_channels, out_channels, num_layers, num_heads, data, viewpoint):
        super().__init__()

        self.viewpoint = viewpoint
        self.lin_dict = torch.nn.ModuleDict()
        for node_type in data.node_types:
            self.lin_dict[node_type] = Linear(-1, hidden_channels)
        self.pre_act = torch.nn.PReLU()

        self.convs = torch.nn.ModuleList()
        self.acts = torch.nn.ModuleList()
        for _ in range(num_layers):
            conv = HGTConv(hidden_channels, hidden_channels, data.metadata(),
                           num_heads)
            self.convs.append(conv)
            self.acts.append(torch.nn.PReLU())

        self.lin = Linear(hidden_channels, out_channels)

    def forward(self, x_dict, edge_index_dict):
        # Matches HOEG (Smit et al. 2024): PReLU activation throughout the network,
        # including between stacked message-passing layers (previously there was none).
        x_dict = {
            node_type: self.pre_act(self.lin_dict[node_type](x))
            for node_type, x in x_dict.items()
        }

        for conv, act in zip(self.convs, self.acts):
            x_dict = conv(x_dict, edge_index_dict)
            x_dict = {node_type: act(x) for node_type, x in x_dict.items()}

        return self.lin(x_dict[self.viewpoint])