import torch
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, GATConv, Linear


class OrderPredictionHeteroGNN_2(torch.nn.Module):
    def __init__(self, hidden_channels_list, out_channels, num_layers, num_heads, viewpoint, tensor_dict):
        super().__init__()

        node_dict = {}
        edges = {}
        edges_dict = {}
        self.convs = torch.nn.ModuleList()

        for key in tensor_dict:
            if '_to_' in key:
                edges[key] = tensor_dict[key]
            else:
                # Initialize linear layers for each node type
                node_dict[key] = Linear(tensor_dict[key], hidden_channels_list[0] * num_heads)
        self.lin_dict = torch.nn.ModuleDict(node_dict)

        for i in range(num_layers):
            for key in edges:
                if key == 'event_to_event':
                    name = tuple(key.split('_'))
                    val = GATConv(-1, hidden_channels_list[i], heads=num_heads, add_self_loops=False)
                    edges_dict[name] = val

                else:
                    name = tuple(key.split('_'))
                    val = GATConv((-1, -1), hidden_channels_list[i], heads=num_heads, add_self_loops=False)
                    edges_dict[name] = val

                    name = key.replace('_to_', '--rev_to--')
                    name = name.split('--')
                    name = tuple(reversed(name))
                    val = GATConv((-1, -1), hidden_channels_list[i], heads=num_heads, add_self_loops=False)
                    edges_dict[name] = val
            conv = HeteroConv(edges_dict, aggr='sum')
            self.convs.append(conv)

        # Linear layer for final prediction specific to "order" nodes
        self.lin_out = Linear(hidden_channels_list[-1] * num_heads, out_channels)

    # def forward(self, batch):
    #     # Apply linear transformations to the initial features of each node type
    #     x_dict = {key: self.lin_dict[key](x) for key, x in batch.x_dict.items()}
    #     edge_index_dict = batch.edge_index_dict
    #
    #     # Apply each HeteroConv layer with GATConv
    #     for conv in self.convs:
    #         x_dict = conv(x_dict, edge_index_dict)
    #         # Apply ReLU activation after each convolution layer
    #         x_dict = {key: F.relu(x) for key, x in x_dict.items()}
    #
    #     # Only use the embeddings for 'order' nodes for final prediction
    #     order_out = self.lin_out(x_dict[self.viewpoint])
    #
    #     return order_out

