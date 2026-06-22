"""
    A simple integreation of explainability to one of our models

    Based on graph_class.py, follows a dataset as it trains a model and then is used for explainability.
"""

import torch

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool

from torch_geometric.explain import Explainer, Explanation
from torch_geometric.explain import GNNExplainer
from torch_geometric.explain.metric import (
   fidelity,
   characterization_score,
   fidelity_curve_auc,
)

from tqdm import tqdm
import matplotlib.pyplot as plt

"""
    In this model Here, we make use of the GCNConv with ReLU(x)=max(x,0) activation for obtaining localized node 
    embeddings, before we apply our final classifier on top of a graph readout layer.
"""
class GCN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GCN, self).__init__()
        self.conv1 = GCNConv(num_node_features, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, hidden_channels)
        self.conv3 = GCNConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index, batch):
        # 1. Obtain node embeddings
        x = self.conv1(x, edge_index)
        x = x.relu()
        x = self.conv2(x, edge_index)
        x = x.relu()
        x = self.conv3(x, edge_index)

        # 2. Readout layer
        x = global_mean_pool(x, batch)  # [hidden_channels, batch_size]

        # 3. Apply a final classifier
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin(x)

        return x

class GNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GNN, self).__init__()
        self.conv1 = GraphConv(num_node_features, hidden_channels)
        self.conv2 = GraphConv(hidden_channels, hidden_channels)
        self.conv3 = GraphConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index, batch):
        x = self.conv1(x, edge_index)
        x = x.relu()
        x = self.conv2(x, edge_index)
        x = x.relu()
        x = self.conv3(x, edge_index)

        x = global_mean_pool(x, batch)

        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin(x)

        return x

def train():
    model.train()

    for data in train_loader:  # Iterate in batches over the training dataset.
         out = model(data.x, data.edge_index, data.batch)  # Perform a single forward pass.
         loss = criterion(out, data.y)  # Compute the loss.
         loss.backward()  # Derive gradients.
         optimizer.step()  # Update parameters based on gradients.
         optimizer.zero_grad()  # Clear gradients.

def test(loader):
     model.eval()

     correct = 0
     for data in loader:  # Iterate in batches over the training/test dataset.
         out = model(data.x, data.edge_index, data.batch)
         pred = out.argmax(dim=1)  # Use the class with highest probability.
         correct += int((pred == data.y).sum())  # Check against ground-truth labels.
     return correct / len(loader.dataset)  # Derive ratio of correct predictions.


train_loader = torch.load(f"files/hetero_structures/order_management/PackageDelivered/multPackages/train_graphs_hom.pt", weights_only=False)
val_loader = torch.load(f"files/hetero_structures/order_management/PackageDelivered/multPackages/val_graphs_hom.pt", weights_only=False)
test_loader = torch.load(f"files/hetero_structures/order_management/PackageDelivered/multPackages/test_graphs_hom.pt", weights_only=False)

for step, data in enumerate(train_loader):
    print(f'Step {step + 1}:')
    print('=======')
    print(f'Number of graphs in the current batch: {data.num_graphs}')
    print(data)
    print()

# Define some variables for the models
num_node_features = 11
num_classes = 2

"""
    Improving Accuracy

    Can we do better than this? As multiple papers pointed out (Xu et al. (2018), Morris et al. (2018)), applying
    neighborhood normalization decreases the expressivity of GNNs in distinguishing certain graph structures.
    An alternative formulation (Morris et al. (2018)) omits neighborhood normalization completely and adds a simple
    skip-connection to the GNN layer in order to preserve central node information. This layer is implemented under the
    name GraphConv in PyTorch Geometric.
    This new implementation gets us up to an 82% accuracy
"""
model = GNN(hidden_channels=64)
print(model)

optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
criterion = torch.nn.CrossEntropyLoss()
pbar = tqdm(range(1, 301))
min_val = 0.5
for epoch in pbar:
    train()
    train_acc = test(train_loader)
    val_acc = test(val_loader)
    print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Val Acc: {val_acc:.4f}')
    if val_acc > min_val:
        print("New best!")
        min_val = val_acc
        torch.save(model.state_dict(), f"./GNN_MultPckgs.pth")
pbar.close()

"""
    Load the best saved model so we can make some explainer tests
"""
state_dict_path = ("./GNN_MultPckgs.pth")
model.load_state_dict(torch.load(state_dict_path))
test_acc = test(test_loader)
print(f'Final ACCs: Test Acc: {test_acc:.4f}')

# Create another loader to make batches for explainability
explain_loader = torch.load(f"files/hetero_structures/order_management/PayOrder/deliveryOnTime/exp_graphs_hom.pt", weights_only=False)

device = torch.device('cpu')
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
        task_level='graph',
        return_type='probs',
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
    index=index,
    batch = batch.batch
)

"""
    By using get_prediction we can see the initial predictions of the model

    But, suppose we want to see what the model would predict if certain graph properties were masked out. 
    We can use the get_masked_prediction method. Say we want to see the model’s predictions if we were to 
    mask out node 5 of the features.
"""
model.eval()
with torch.no_grad():
    predictions = model_explainer.get_prediction(batch.x, batch.edge_index, batch = batch.batch)

node_mask = torch.ones_like(batch.x)
node_mask[5] = 0  # Mask node 5 features

with torch.no_grad():
    masked_predictions = model_explainer.get_masked_prediction(batch.x, batch.edge_index,
                                                               node_mask=node_mask, batch = batch.batch)

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
plt.rcParams.update({'font.size': 50})
plt.figure(figsize=(50, 35))
plt.plot(mean_difference, color="olive", linewidth=3.5, label="Mean Difference")
plt.title('Mean Difference Between Original and Masked Predictions')
plt.xlabel('Class')
plt.ylabel('Mean Difference in Logits')
plt.legend()
plt.show()

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
model_explanation.visualize_feature_importance("model_topk.png", top_k=10)

# Choose a node that you're interested in explaining
index = 100

# Create model_based explainer
metric_explainer = Explainer(
    model=model,
    algorithm=GNNExplainer(epochs=200),
    explanation_type='model',
    node_mask_type='object',
    edge_mask_type='object',
    model_config=dict(
        mode='multiclass_classification',
        task_level='node',
        return_type='probs',
    )
  )

# Call model_based explainer
metric_explanation = metric_explainer(
    batch.x,
    batch.edge_index,
    index=index,
    batch=batch.batch,
)


is_valid = model_explanation.validate()

# Fidelity
fid_pos, fid_neg = fidelity(
   explainer=metric_explainer,
   explanation=metric_explanation
)

#Characterization score
char_score = characterization_score(
    fid_pos,
    fid_neg,
    pos_weight=0.7,    # Higher weight
    neg_weight=0.3     # Lower weight
)

# Print results
print(f"Test is valid: {is_valid}")
print(f"Fidelity Score: {fid_pos}, {fid_neg}")