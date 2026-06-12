import os
import torch
import torch.nn.functional as F
from torch_geometric.datasets import Reddit
from torch_geometric.loader import RandomNodeLoader
from torch_geometric.nn import SAGEConv
from tqdm import tqdm

from torch_geometric.explain import Explainer, Explanation
from torch_geometric.explain import GNNExplainer, DummyExplainer, CaptumExplainer, PGExplainer, AttentionExplainer
from torch_geometric.explain.config import ExplainerConfig, ModelMode
import numpy as np
from torch_geometric.explain import ThresholdConfig

import matplotlib.pyplot as plt

class SAGE(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels):
        super().__init__()
        self.convs = torch.nn.ModuleList()
        self.convs.append(SAGEConv(in_channels, hidden_channels))
        self.convs.append(SAGEConv(hidden_channels, out_channels))

    def forward(self, x, edge_index):
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i < len(self.convs) - 1:
                x = F.relu(x)
                x = F.dropout(x, p=0.5, training=self.training)
        return x


def train(model, loader, optimizer, device, num_train_nodes):
    model.train()
    total_loss = 0
    total_correct = 0

    for batch in tqdm(loader, desc="Training"):
        # Move batch to device
        batch = batch.to(device)

        # Forward pass
        optimizer.zero_grad()
        out = model(batch.x, batch.edge_index)

        # Loss and backpropagation
        loss = F.cross_entropy(out[batch.train_mask], batch.y[batch.train_mask])
        loss.backward()
        optimizer.step()

        # Accuracy calculation
        pred = out[batch.train_mask].argmax(dim=-1)
        total_correct += int((pred == batch.y[batch.train_mask]).sum())
        total_loss += loss.item()

    return total_loss / len(loader), total_correct / num_train_nodes


def test(model, loader, device):
    model.eval()
    total_correct = 0
    total_test_nodes = 0

    for batch in tqdm(loader, desc="Testing"):
        batch = batch.to(device)

        # Forward pass
        with torch.no_grad():
            out = model(batch.x, batch.edge_index)
            pred = out.argmax(dim=-1)

        # Evaluate accuracy only on test nodes in the current batch
        mask = batch.test_mask
        total_correct += int((pred[mask] == batch.y[mask]).sum())
        total_test_nodes += mask.sum().item()

    # Compute accuracy
    accuracy = total_correct / total_test_nodes
    return accuracy

"""
    For this test we use the Reddit dataset. This is a benchmark dataset of Reddit posts 
    that belong to different communities.
    The Reddit dataset includes:
        * 232,965 nodes (or posts)
        * 114,615,892 edges
        * 602 features
        * 41 classes (the community the post belongs to)
"""
dataset = Reddit(root="../data/Reddit")

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
data = dataset[0]

#Add masks if needed
if not hasattr(data, "train_mask"):
    num_nodes = data.x.size(0)
    data.train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    data.val_mask = torch.zeros(num_nodes, dtype=torch.bool)
    data.test_mask = torch.zeros(num_nodes, dtype=torch.bool)

    train_size = int(0.8 * num_nodes)
    val_size = int(0.1 * num_nodes)

    indices = torch.randperm(num_nodes)
    data.train_mask[indices[:train_size]] = True
    data.val_mask[indices[train_size:train_size + val_size]] = True
    data.test_mask[indices[train_size + val_size:]] = True

# Count number of training nodes
num_train_nodes = data.train_mask.sum().item()

# Define Loaders
train_loader = RandomNodeLoader(data, num_parts=40, shuffle=True)
test_loader = RandomNodeLoader(data, num_parts=5)

# Initialize model and optimizer
model = SAGE(data.num_features, 64, dataset.num_classes).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

best_test_acc = 0
best_model = None

# # Training and testing
# for epoch in range(1, 6):
#     loss, train_acc = train(model, train_loader, optimizer, device, num_train_nodes)
#     test_acc = test(model, test_loader, device)
#
#     if test_acc > best_test_acc:
#         best_test_acc = test_acc
#         best_model = model
#     print(
#         f"Epoch {epoch:02d}, Loss: {loss:.4f}, Train: {train_acc:.4f}, "
#         f"Test: {test_acc:.4f}"
#     )
#
# # Save trained model and its components to drive.
# model_path = ("./model.pt")
# state_dict_path = ("./model_state_dict.pt")
# torch.save(best_model, model_path)
# torch.save(model.state_dict(), state_dict_path)
# print(f"File saved at: {model_path}")
# print(f"File saved at: {state_dict_path}")

# Reload trained model
model_path = ("./model.pt")
state_dict_path = ("./model_state_dict.pt")

model = SAGE(data.num_features, 64, dataset.num_classes).to(device)
model.load_state_dict(torch.load(state_dict_path))

# Create another loader to make batches for explainability
explain_loader = RandomNodeLoader(data, num_parts=40, shuffle=True)
batch = next(iter(explain_loader)).to(device)

"""
    Suppose that the model predicts that a post with index x=143 was classified as belonging to some subreddit.
    We want to understand why. Specifically, we want to understand which features were most influential in this 
    prediction.
"""
index = 143

"""
Here’s how we would initialize Explainer and call it:
    * explanation_type='model’ because we want to understand why our model itself made this prediction.
    * node_mask_type=’attributes’ because we want to understand specific features of importance. 
      We default to node_edge_type=None to only look at node importance without masking any edges.
    * model_config : our mode = ‘multiclass_classification’ because the Reddit dataset includes 41 classes, 
      our task_level = ‘node’ because we are predicting nodes (a.k.a. posts), 
      and we specify return_type = ‘log_probs’ 
    * threshold_config : threshold_type=’topk’, value=10 because want to isolate the top 10 most influential nodes. 
      It is nodes because our task_level = ‘node’ .
"""
# Create model_based explainer
model_explainer = Explainer(
    model=model,
    algorithm=GNNExplainer(epochs=50),
    explanation_type='model',
    node_mask_type='attributes',
    edge_mask_type='object',
    model_config=dict(
        mode='multiclass_classification',
        task_level='node',
        return_type='log_probs',
    ),
    # this isolates the top 10 nodes whose features influenced node 143
    threshold_config=dict(threshold_type='topk', value=10)
)

"""
    Now we call the model explainer.
        * here, implicitly target=None because our explanation_type = ‘model’ not ‘phenomenon’.
        * at the end of the initialization and call, we arrive at our Explanation .
    The Explanation class holds all the key information that the Explainability module explains
"""
model_explanation = model_explainer(
    batch.x,
    batch.edge_index,
    index=index
)

# # Create phenomenon_based explainer
# phen_explainer = Explainer(
#     model=model,
#     algorithm=GNNExplainer(epochs=50),
#     explanation_type='phenomenon',
#     node_mask_type='attributes',
#     edge_mask_type='object',
#     model_config=dict(
#         mode='multiclass_classification',
#         task_level='node',
#         return_type='log_probs',
#     ),
#     # this isolates all nodes whose features have influence greater than 0.5
#     threshold_config=dict(threshold_type='hard', value=0.5)
# )
#
# # Call phenomenon_based explainer
# phen_explanation = model_explainer(
#     batch.x,
#     batch.edge_index,
#     # here you can add a range of node indices that you're interested in.
#     # since I add the range of the batch size, this will evaluate the entire batch set.
#     index=torch.arange(5825),
#     target=batch.y,  # Add ground truth labels
# )

"""
    By using get_prediction we can see the initial predictions of the model
    
    But, suppose we want to see what the model would predict if certain graph properties were masked out. 
    We can use the get_masked_prediction method. Say we want to see the model’s predictions if we were to 
    mask out node 5.
"""
model.eval()
with torch.no_grad():
    predictions = model_explainer.get_prediction(batch.x, batch.edge_index)

node_mask = torch.ones_like(batch.x)
node_mask[5] = 0  # Mask node 5 features

with torch.no_grad():
    masked_predictions = model_explainer.get_masked_prediction(batch.x, batch.edge_index, node_mask=node_mask)

"""
    We can calculate the mean difference and graph it out.
"""
# Difference in predictions
difference = predictions - masked_predictions
mean_difference = difference.mean(dim=0).cpu().numpy()

"""
    The graph shows the average impact of masking node 5 on the logits for any given class (in this case, the impact
    on predicting the community the post will be predicted to belong to). This allows us to identify which classes are 
    most sensitive to masking, where a positive difference means masking node 5 increases the logits on average, 
    while a negative difference means masking node 5 decreases the logits on average.
"""
# plt.rcParams.update({'font.size': 50})
# plt.figure(figsize=(50, 35))
# plt.plot(mean_difference, color="olive", linewidth=3.5, label="Mean Difference")
# plt.title('Mean Difference Between Original and Masked Predictions')
# plt.xlabel('Class')
# plt.ylabel('Mean Difference in Logits')
# plt.legend()
# plt.show()

"""
    We can perform more granular analysis by finding the explanation subgraphs.
    * get_explanation_subgraph() returns another Explanation object that includes only the nodes and edges that are of 
      non-zero importance to the explanation, isolating the parts of the graph that most influenced the prediction.
        -- This can help reduce the noise from irrelevant nodes and edges.
    * get_complement_subgraph() is the exact opposite. It returns an Explanation object that includes only the nodes 
      and edges that with zero importance to the explanation.
        -- This can help identify useless nodes and edges
"""
subgraph = model_explanation.get_explanation_subgraph()
complement_subgraph = model_explanation.get_complement_subgraph()

"""
    Suppose we want to list all the features that influenced the prediction of a given node.
    We can run the following code (adapted from the visualize_feature_importance function) which does the following:
        * Computes the total importance of each feature across all nodes.
        * Outputs a list of features whose importance is non-zero.
        * The number of features in the list will be defined by how the Explainer is initialized. Specifically, 
          depending on the ThresholdConfig. Here the length is 10 because 
          threshold_config = dict(threshold_type=’topk’, value=10) .
"""
node_mask = model_explanation.get('node_mask')
if node_mask is None:
    raise ValueError(f"The attribute 'node_mask' is not available "
                      f"in '{model_explanation.__class__.__name__}' "
                      f"(got {model_explanation.available_explanations})")
if node_mask.dim() != 2 or node_mask.size(1) <= 1:
    raise ValueError(f"Cannot compute feature importance for "
                      f"object-level 'node_mask' "
                      f"(got shape {node_mask.size()})")

score = node_mask.sum(dim=0)
non_zero_indices = torch.nonzero(score, as_tuple=True)[0]
non_zero_scores = score[non_zero_indices]

# Sort the non-zero scores in descending order
sorted_indices = non_zero_indices[torch.argsort(non_zero_scores, descending=True)]
print(sorted_indices)

"""
    Visually explaining what's happening
    There are methods to picture what's happening in the explanation.
    The visualize_graph method depicts the nodes and edges that were influential in the model’s prediction. 
    Notably, it displays edges with varying opacity, depending on how significant the edge is 
    (the darker, the more significant). Because of this, Explainer cannot be initialized with edge_mask_type=None
    
    This is part of the reason why we couldn't use heterogeneous data structures, because it wouldn't provide us
    with edge explanations so we couldn't see the subgraphs
"""

# # Decide the number of top important features you would like to see
k = 10
plt.rcParams.update({'font.size': 10})

model_explanation.visualize_graph('model_graph.png')

"""
    Another method that visualizes an element of an Explanation is visualize_feature_importance. 
    As the name suggests, this method produces a bar graph with the top k most influential features that influenced the 
    prediction of a node. Similarly to the previous method, Explainer cannot be initialized with node_mask_type=None
"""
model_explanation.visualize_feature_importance("model_topk.png", top_k=k)

# phen_explanation.visualize_feature_importance("phen_topk.png", top_k=k)
# phen_explanation.visualize_graph('phen_graph.png', backend="graphviz")