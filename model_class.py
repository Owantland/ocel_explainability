import torch
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, GATConv, Linear


class OrderPredictionHeteroGNN_2(torch.nn.Module):
    def __init__(self, hidden_channels_list, out_channels, num_layers, num_heads, viewpoint):
        super().__init__()

        # Initialize linear layers for each node type
        self.lin_dict = torch.nn.ModuleDict({
            'item': Linear(2, hidden_channels_list[0] * num_heads),
            'package': Linear(1, hidden_channels_list[0] * num_heads),
            'employee': Linear(18, hidden_channels_list[0] * num_heads),
            'product': Linear(2, hidden_channels_list[0] * num_heads),
            'order': Linear(1, hidden_channels_list[0] * num_heads),
            'event': Linear(11, hidden_channels_list[0 * num_heads]),
            'customer': Linear(15, hidden_channels_list[0 * num_heads])
        })
        self.viewpoint = viewpoint
        # Define the Heterogeneous GAT layers
        self.convs = torch.nn.ModuleList()
        for i in range(num_layers):
            conv = HeteroConv({
                ('event', 'to', 'event'): GATConv(-1, hidden_channels_list[i], heads=num_heads, add_self_loops=False),
                ('order', 'to', 'item'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                 add_self_loops=False),
                ('order', 'to', 'event'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                  add_self_loops=False),
                ('item', 'to', 'event'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                 add_self_loops=False),
                ('employee', 'to', 'event'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                     add_self_loops=False),
                ('package', 'to', 'event'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                    add_self_loops=False),
                ('package', 'to', 'item'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                   add_self_loops=False),
                ('event', 'rev_to', 'package'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                        add_self_loops=False),
                ('item', 'rev_to', 'order'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                     add_self_loops=False),
                ('event', 'rev_to', 'order'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                      add_self_loops=False),
                ('event', 'rev_to', 'item'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                     add_self_loops=False),
                ('event', 'rev_to', 'employee'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                         add_self_loops=False),
                ('item', 'rev_to', 'package'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                       add_self_loops=False),

                ('product', 'to', 'event'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                    add_self_loops=False),
                ('event', 'rev_to', 'product'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                        add_self_loops=False),
                ('product', 'to', 'item'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                   add_self_loops=False),
                ('item', 'rev_to', 'product'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                       add_self_loops=False),

                ('customer', 'to', 'order'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                     add_self_loops=False),
                ('customer', 'to', 'event'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                     add_self_loops=False),

                ('employee', 'to', 'package'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                       add_self_loops=False),
                ('package', 'rev_to', 'employee'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                           add_self_loops=False),

                ('order', 'rev_to', 'customers'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                          add_self_loops=False),
                ('event', 'rev_to', 'customer'): GATConv((-1, -1), hidden_channels_list[i], heads=num_heads,
                                                         add_self_loops=False)
            }, aggr='sum')
            self.convs.append(conv)

        # Linear layer for final prediction specific to "order" nodes
        self.lin_out = Linear(hidden_channels_list[-1] * num_heads, out_channels)

    def forward(self, batch):
        # Apply linear transformations to the initial features of each node type
        x_dict = {key: self.lin_dict[key](x) for key, x in batch.x_dict.items()}
        edge_index_dict = batch.edge_index_dict

        # Apply each HeteroConv layer with GATConv
        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict)
            # Apply ReLU activation after each convolution layer
            x_dict = {key: F.relu(x) for key, x in x_dict.items()}

        # Only use the embeddings for 'order' nodes for final prediction
        order_out = self.lin_out(x_dict[self.viewpoint])

        return order_out

