import os.path as osp

import torch
import torch.nn.functional as F

import torch_geometric
import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP
from torch_geometric.nn import HeteroConv, Linear, SAGEConv
from torch_geometric.loader import DataLoader

from torchmetrics import F1Score
from tqdm import tqdm

class HeteroGNN(torch.nn.Module):
    def __init__(self, metadata, hidden_channels, out_channels, num_layers):
        super().__init__()

        self.convs = torch.nn.ModuleList()
        for _ in range(num_layers):
            conv = HeteroConv({
                edge_type: SAGEConv((-1, -1), hidden_channels)
                for edge_type in metadata[1]
            })
            self.convs.append(conv)

        self.lin = Linear(hidden_channels, out_channels)

    def forward(self, x_dict, edge_index_dict):
        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict)
            x_dict = {key: F.leaky_relu(x) for key, x in x_dict.items()}
        return self.lin(x_dict[viewpoint_object])

def train():
    model.train()

    total_examples = total_loss = 0
    for batch in train_loader:
        optimizer.zero_grad()
        batch = batch.to(device)
        batch_size = len(batch[viewpoint_object].batch)
        out = model(batch.x_dict, batch.edge_index_dict)
        loss = F.cross_entropy(out[:batch_size],
                               batch[viewpoint_object].y[:batch_size])
        loss.backward()
        optimizer.step()

        total_examples += batch_size
        total_loss += float(loss) * batch_size
    return total_loss / total_examples

@torch.no_grad()
def test(loader):
    model.eval()
    f1 = F1Score("binary")
    total_correct = 0
    for data in loader:
        data = data.to(device)
        batch_size = len(data[viewpoint_object].batch)
        out = model(data.x_dict, data.edge_index_dict)
        pred = out.argmax(dim=1)
        total_correct += int((pred[:batch_size] == data[viewpoint_object].y[:batch_size]).sum())
        f1(pred[:batch_size], data[viewpoint_object].y[:batch_size])
    return f1.compute().item()


# # We initialize conference node features with a single one-vector as feature:
# path = osp.join(osp.dirname(osp.realpath(__file__)), '../../data/DBLP')
# dataset = DBLP(path, transform=T.Constant(node_types='conference'))
# data = dataset[0]

viewpoint_object = 'Orders'
training_data = torch.load('../files/hetero_structures/order_management/train_graphs_sg.pt', weights_only=False)
val_data = torch.load('../files/hetero_structures/order_management/val_graphs_sg.pt', weights_only=False)
test_data = torch.load('../files/hetero_structures/order_management/test_graphs_sg.pt', weights_only=False)

train_loader = DataLoader(training_data, batch_size=64, shuffle=True)
val_loader = DataLoader(val_data, batch_size=64)
test_loader = DataLoader(test_data, batch_size=64)

for step, data in enumerate(train_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {len(data['Orders'].batch)}')
    print(data)

# for step, data in enumerate(val_loader):
#     print(f'Step {step + 1}:')
#     print('=======')
#     print(f'Number of graphs in the current batch: {len(data['Orders'].batch)}')
#     print(data)

model = HeteroGNN(training_data[0].metadata(), hidden_channels=64, out_channels=2, num_layers=2)
optimizer = torch.optim.Adam(model.parameters(), lr=0.005, weight_decay=0.001)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = model.to(device)

data = next(iter(train_loader))
with torch.no_grad():  # Initialize lazy modules.
    out = model(data.x_dict, data.edge_index_dict)

best_val = 0
pbar = tqdm(range(1, 101))
for epoch in pbar:
    loss = train()
    train_acc = test(train_loader)
    val_acc = test(val_loader)
    print(f"Epoch: {epoch:03d}, Loss: {loss:.4f}, Train: {train_acc:.4f}, 'f'Val: {val_acc:.4f}")

    if val_acc > best_val:
        best_val = val_acc
        print("Best value found!")
        torch.save(model.state_dict(), f"../files/models/order_management/het_BinaryClassifier.pt")
pbar.close()

test_acc = test(test_loader)
print(f"Final test Accuracy: {test_acc:.4f}")

# We have a simple test setup. We now have to try regression