import torch
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, Linear, GATConv

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