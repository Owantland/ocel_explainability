import torch
import torch.nn as nn
import torch_geometric.nn as pygnn


class KDimGNN(torch.nn.Module):
    """Port of HOEG's (Smit et al. 2024) own k-dimensional GNN baseline
    (Morris et al. 2019), from OCPPM-master/models/definitions/geometric_models.py's
    HigherOrderGNN (lines 188-264 there). Stacked lazy-dim GraphConv + PReLU message
    passing, matching HOEG's own recipe exactly, plus a lazy-dim Linear head.

    Unlike OCPPM's own class this drops `graph_level_prediction`/`gpool` and
    `no_preprocessing_layers` entirely -- this project always calls it with
    `graph_level_prediction=False` (node-level output) and no pre-processing
    layers, so keeping those knobs would just be dead configuration surface.
    `forward()` is written for use BEFORE `to_hetero()` wraps this module -- once
    wrapped, the model operates on the full heterogeneous graph (x_dict/edge_index_dict)
    exactly like HGT, not the Events-only stripped-down graph HomoGNN (REG_GNN) uses.

    No `squeeze` here (unlike OCPPM's OTC config, which sets `squeeze=True`): HGT's own
    forward() (model_classes/HGT.py) doesn't squeeze either, and `baselines.py`'s
    `out[0].item()` convention already handles the resulting (N, 1)-shaped output fine.
    Keeping this identical to HGT's output convention (not OCPPM's) means
    kdim_predictions() in baselines.py can mirror hgt_predictions() directly.
    """

    def __init__(self, hidden_channels=32, out_channels=1, num_layers=2,
                 no_postprocessing_layers=1):
        super().__init__()
        self.convs = nn.ModuleList()
        self.acts = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(pygnn.GraphConv(-1, hidden_channels))
            self.acts.append(nn.PReLU())

        self.post_layers = nn.ModuleList()
        for i in range(no_postprocessing_layers):
            if i != (no_postprocessing_layers - 1):
                self.post_layers.append(pygnn.Linear(-1, hidden_channels))
            else:
                self.post_layers.append(pygnn.Linear(-1, out_channels))

    def forward(self, x, edge_index, batch=None):
        for conv, act in zip(self.convs, self.acts):
            x = conv(x, edge_index)
            x = act(x)
        for post_processing in self.post_layers:
            x = post_processing(x)
        return x
