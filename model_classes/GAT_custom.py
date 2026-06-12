import torch
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, GATConv, Linear

class GATCustom(torch.nn.Module):
    def __init__(self, data, hidden_channels, out_channels, num_heads, num_layers):
        super().__init__()

        # Initialize linear layers for each node type
        self.lin_dict = torch.nn.ModuleDict()
        for node_type in data.node_types:
            # Initialize linear layers for each node type
            self.lin_dict[node_type] = Linear(-1, hidden_channels[0] * num_heads)

        # Define the Heterogeneous GAT layers
        self.convs = torch.nn.ModuleList()
        for i in range(num_layers):
            conv_dict = {}
            for edge_type in data.edge_types:
                if edge_type == ('Events', 'to', 'Events'):
                    conv_dict[edge_type] = GATConv(-1, hidden_channels[i], heads=num_heads, add_self_loops = False)
                else:
                    conv_dict[edge_type] = GATConv((-1, -1), hidden_channels[i],
                                                   heads=num_heads, add_self_loops=False)
            conv = HeteroConv(conv_dict, aggr='sum')
            self.convs.append(conv)

        # Linear layer for final prediction specific to viewpoint nodes
        self.lin_out = Linear(hidden_channels[-1] * num_heads, out_channels)

    def forward(self, x_dict, edge_index_dict, vwpnt):
        # Apply linear transformations to the initial features of each node type
        x_dict = {key: self.lin_dict[key](x) for key, x in x_dict.items()}
        edge_index_dict = edge_index_dict

        # Apply each HeteroConv layer with GATConv
        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict)
            # Apply ReLU activation after each convolution layer
            x_dict = {key: F.relu(x) for key, x in x_dict.items()}

        # Only use the embeddings for 'order' nodes for final prediction
        order_out = self.lin_out(x_dict[vwpnt])

        return order_out