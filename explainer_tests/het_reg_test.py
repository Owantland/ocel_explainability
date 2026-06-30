"""
Heterogeneous Node Classification on DBLP with PyTorch Geometric (PyG)
========================================================================

This example uses PyG's bundled `DBLP` dataset -- a real heterogeneous
academic graph (NOT synthetic, unlike the earlier examples in this series)
-- together with a mini-batch `NeighborLoader` that avoids depending on
`torch-sparse`.

IMPORTANT SCOPE NOTE: DBLP ships only classification labels (4-class
research-area labels for `author` nodes), not a continuous regression
target. So this example is *node classification*, not regression -- the
model/training code is otherwise structurally identical to the node
regression example (same per-relation message passing, same train/val/test
mask pattern), just with a softmax head and cross-entropy loss instead of
a scalar head and MSE/L1 loss.

Dataset structure (loaded automatically by PyG):
    - Node types : 'author', 'paper', 'term', 'conference'
    - Edge types : ('author','to','paper') + reverse
                   ('paper','to','term') + reverse
                   ('paper','to','conference') + reverse
    - Labels     : 'author' nodes only, 4 classes, with train/val/test
                   masks already provided by the dataset.
    - Caveat     : 'conference' nodes have no raw features in the original
                   data. We add a constant placeholder feature via
                   `T.Constant` so every node type has *something* to
                   project into the model's hidden space.

ON THE LOADER (the actual point of this example):
    PyG's `NeighborLoader` samples a fixed-size neighborhood around a batch
    of "seed" nodes, for mini-batch training on graphs too large (or, for
    practice, just inconvenient) to run full-batch. Historically this
    required `torch-sparse`'s compiled sampling routines. As of recent PyG
    versions, `NeighborLoader` instead prefers `pyg-lib`'s sampler when
    it's installed, and `pyg-lib` has NO dependency on `torch-sparse` at
    all. So: install `pyg-lib`, and this loader works without ever
    touching `torch-sparse`.

Install requirements:
    pip install torch torch_geometric pyg-lib --break-system-packages
    (pyg-lib wheels are version-matched to your torch/CUDA build --
    see https://data.pyg.org/whl/ if the plain pip install doesn't find one)
"""

import torch
import torch.nn.functional as F
from torch.nn import Linear
import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import HGTConv

from torch_geometric.utils import k_hop_subgraph
from torch.nn.functional import cosine_similarity

from torch_geometric.explain import Explainer, CaptumExplainer, GNNExplainer
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import pandas as pd

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ---------------------------------------------------------------------------
# 1. Load the DBLP dataset
# ---------------------------------------------------------------------------
# `T.Constant(node_types='conference')` gives 'conference' nodes a constant
# placeholder feature (a single column of 1s), since the raw dataset has no
# features for that node type at all.
dataset = DBLP(root="data/DBLP", transform=T.Constant(node_types="conference"))
data = dataset[0]  # DBLP is a single heterogeneous graph, not a list of graphs
data = data.to(device)

metadata = data.metadata()
num_classes = int(data["author"].y.max().item()) + 1


# ---------------------------------------------------------------------------
# 2. Mini-batch loaders via NeighborLoader (no torch-sparse dependency)
#
# `input_nodes=('author', data['author'].train_mask)` tells the loader
# which nodes are the actual prediction targets for this split -- it then
# samples a neighborhood AROUND each batch of authors, pulling in whatever
# papers/terms/conferences are needed to compute their embeddings.
# `num_neighbors=[10, 10]` caps the fan-out at 10 neighbors per hop, for 2
# hops (matching the 2-layer model below).
# ---------------------------------------------------------------------------
train_loader = NeighborLoader(
    data,
    num_neighbors=[10, 10],
    batch_size=128,
    input_nodes=("author", data["author"].train_mask),
    shuffle=True,
)

val_loader = NeighborLoader(
    data,
    num_neighbors=[10, 10],
    batch_size=128,
    input_nodes=("author", data["author"].val_mask),
    shuffle=False,
)

test_loader = NeighborLoader(
    data,
    num_neighbors=[10, 10],
    batch_size=128,
    input_nodes=("author", data["author"].test_mask),
    shuffle=False,
)


# ---------------------------------------------------------------------------
# 3. Define the GNN (same HGTConv-based design as the earlier node
#    regression example, swapped to a classification head)
# ---------------------------------------------------------------------------
class HeteroNodeClassifier(torch.nn.Module):
    def __init__(self, metadata, hidden_channels=64, num_layers=2,
                 heads=4, num_classes=4, target_node_type="author"):
        super().__init__()
        self.target_node_type = target_node_type

        # Lazy per-type input projection, same as the HGTConv regression
        # model -- handles 'author'/'paper'/'term' real features and
        # 'conference' nodes' constant placeholder feature uniformly.
        self.convs = torch.nn.ModuleList(
            [
                HGTConv(-1, hidden_channels, metadata, heads=heads)
                for _ in range(num_layers)
            ]
        )

        self.out_lin = Linear(hidden_channels, num_classes)

    def forward(self, x_dict, edge_index_dict):
        for conv in self.convs:
            x_dict = {nt: x.relu() for nt, x in conv(x_dict, edge_index_dict).items()}
        return self.out_lin(x_dict[self.target_node_type])  # raw logits


model = HeteroNodeClassifier(
    metadata, hidden_channels=64, num_layers=2, heads=4,
    num_classes=num_classes, target_node_type="author",
).to(device)

# Materialize HGTConv's lazy linear layers with one real forward pass
# before constructing the optimizer (same reason as the regression example).
with torch.no_grad():
    batch = next(iter(train_loader))
    model(batch.x_dict, batch.edge_index_dict)

optimizer = torch.optim.Adam(model.parameters(), lr=5e-3, weight_decay=1e-5)


# ---------------------------------------------------------------------------
# 4. Train / evaluate
#
# Each `batch` from NeighborLoader is its own small HeteroData object: a
# sampled subgraph containing the seed authors plus their neighborhood.
# `batch['author'].batch_size` tells you how many of `batch['author'].x`'s
# rows are the actual seed nodes for this step -- NeighborLoader always
# places them FIRST, with sampled neighbor nodes appended after. So we
# only compute loss/accuracy on `out[:batch_size]`, not the whole subgraph.
# ---------------------------------------------------------------------------
def train_one_epoch():
    model.train()
    total_loss = total_correct = total_examples = 0

    for batch in train_loader:
        batch = batch.to(device)
        optimizer.zero_grad()

        out = model(batch.x_dict, batch.edge_index_dict)
        batch_size = batch["author"].batch_size
        seed_out = out[:batch_size]
        seed_y = batch["author"].y[:batch_size]

        loss = F.cross_entropy(seed_out, seed_y)
        loss.backward()
        optimizer.step()

        total_loss += loss.item() * batch_size
        total_correct += (seed_out.argmax(dim=-1) == seed_y).sum().item()
        total_examples += batch_size

    return total_loss / total_examples, total_correct / total_examples


@torch.no_grad()
def evaluate(loader):
    model.eval()
    total_correct = total_examples = 0

    for batch in loader:
        batch = batch.to(device)
        out = model(batch.x_dict, batch.edge_index_dict)
        batch_size = batch["author"].batch_size
        seed_out = out[:batch_size]
        seed_y = batch["author"].y[:batch_size]

        total_correct += (seed_out.argmax(dim=-1) == seed_y).sum().item()
        total_examples += batch_size

    return total_correct / total_examples


# if __name__ == "__main__":
#     n_epochs = 100
#     best_val_acc = 0.0
#
#     for epoch in range(1, n_epochs + 1):
#         train_loss, train_acc = train_one_epoch()
#         val_acc = evaluate(val_loader)
#
#         if val_acc > best_val_acc:
#             best_val_acc = val_acc
#             torch.save(model.state_dict(), f'./dblap_test.pth')
#
#         if epoch % 5 == 0 or epoch == 1:
#             print(
#                 f"Epoch {epoch:02d} | Train Loss: {train_loss:.4f} | "
#                 f"Train Acc: {train_acc:.4f} | Val Acc: {val_acc:.4f}"
#             )
#
#     test_acc = evaluate(test_loader)
#     print(f"\nFinal Test Accuracy: {test_acc:.4f} (best Val Accuracy: {best_val_acc:.4f})")
#

model.load_state_dict(torch.load(f'./dblap_test.pth'))

"""Ablation Feature Importance"""
# def get_masked_edge_index(edge_index_dict, self_edges=True):
#     new_edge_index_dict = {}
#     for edge_type, edge_index in edge_index_dict.items():
#         if self_edges:
#             # 保留 self-edges
#             mask = edge_index[0] == edge_index[1]
#         else:
#             # 保留 neighbor-edges
#             mask = edge_index[0] != edge_index[1]
#         new_edge_index_dict[edge_type] = edge_index[:, mask]
#     return new_edge_index_dict
#
# # 设置字体大小
# plt.rcParams.update({'font.size': 16})
#
# # 循环遍历不同的 target
# for target in range(4):
#     # 准备存储输出的列表
#     output_self_list = []
#     output_neighbor_list = []
#
#     # 循环遍历 test_loader 中的所有 batch
#     for batch in tqdm(test_loader, desc=f"Processing batches for target {target}"):
#         batch = batch.to(device)
#         x_dict, edge_index_dict = batch.x_dict, batch.edge_index_dict
#
#         # 获取 self-edge 和 neighbor-edge 的边索引
#         edge_index_dict_self = get_masked_edge_index(edge_index_dict, self_edges=True)
#         edge_index_dict_neighbor = get_masked_edge_index(edge_index_dict, self_edges=False)
#
#         # 执行模型，计算 self-edge 和 neighbor-edge 的输出
#         output_self = model(x_dict, edge_index_dict_self)
#         output_neighbor = model(x_dict, edge_index_dict_neighbor)
#
#         # 提取该 batch 中所有节点的 target 结果
#         output_self_target = output_self[:, target].detach().cpu().numpy()
#         output_neighbor_target = output_neighbor[:, target].detach().cpu().numpy()
#         # output_neighbor_target
#
#         # 将结果添加到列表中
#         output_self_list.extend(output_self_target)
#         output_neighbor_list.extend(output_neighbor_target)
#
#     # 计算self-edge和neighbor-edge的频率计数和bin
#     self_counts, self_bins = np.histogram(output_self_list, bins=200)
#     neighbor_counts, neighbor_bins = np.histogram(output_neighbor_list, bins=200)
#
#     # 计算频率比例
#     self_freq = self_counts / sum(self_counts)
#     neighbor_freq = neighbor_counts / sum(neighbor_counts)
#
#     # 绘制self-edge贡献的频率分布
#     plt.hist(self_bins[:-1], bins=self_bins, weights=self_freq, alpha=0.6, color='blue',
#              label='Self')
#
#     # 绘制neighbor-edge贡献的频率分布
#     plt.hist(neighbor_bins[:-1], bins=neighbor_bins, weights=neighbor_freq, alpha=0.6, color='orange',
#              label='Neighbor')
#
#     # 计算并标记均值
#     self_mean = np.mean(output_self_list)
#     neighbor_mean = np.mean(output_neighbor_list)
#
#     plt.axvline(self_mean, color='blue', linestyle='dashed', linewidth=2)
#     plt.axvline(neighbor_mean, color='orange', linestyle='dashed', linewidth=2)
#
#     # 根据均值的位置动态调整文字位置
#     if self_mean < neighbor_mean:
#         plt.text(self_mean - 0.05, 0.65, f'{self_mean:.2f}', color='blue', ha='right')
#         plt.text(neighbor_mean + 0.01, 0.65, f'{neighbor_mean:.2f}', color='orange', ha='left')
#     else:
#         plt.text(self_mean + 0.01, 0.65, f' {self_mean:.2f}', color='blue', ha='left')
#         plt.text(neighbor_mean - 0.05, 0.65, f'{neighbor_mean:.2f}', color='orange', ha='right')
#
#     # 添加标签和图例
#     plt.xlabel('Feature Importance')
#     plt.ylabel('Frequency Rate')
#     plt.xlim([-0.1, 0.6])  # 限制x轴范围
#     plt.ylim([0, 0.7])  # 设定y轴范围为0到0.7
#     plt.legend(loc='upper right')
#
#     # 添加网格
#     plt.grid(True)
#
#     # 保存图像
#     plt.savefig(f'./target_{target}_feature_importance.png', dpi=300,
#                 bbox_inches='tight')
#     plt.close()
#
# print("All images saved.")


"""
    PYG Explainer
"""
# explainer = Explainer(
#     model=model,
#     algorithm= CaptumExplainer('IntegratedGradients'),
#     explanation_type='model',
#     node_mask_type='attributes',
#     # edge_mask_type='object',
#     model_config=dict(
#         mode='multiclass_classification',
#         task_level='node',
#         return_type='probs',
#     )
# )
#
# feature_importances_list = [[] for _ in range(4)]  # 为每个 target 创建一个列表
#
# total_batches = len(test_loader)  # 获取总批次数以显示进度
# for batch in tqdm(test_loader, total=total_batches, desc="Processing batches"):
#     batch = batch.to(device)
#     x_dict, edge_index_dict = batch.x_dict, batch.edge_index_dict
#     for target in range(4):
#         explanation = explainer(x_dict, edge_index_dict)
#         feature_importances = explanation["author"].node_mask  # 取得当前样本的 feature importance
#         feature_importances_list[target].append(feature_importances)
#
# avg_feature_importances = []
# for feature_list in feature_importances_list:
#     avg_importance = torch.cat(feature_list, dim=0).mean(dim=0)[:334]  # 限制计算前 64 个 features
#     # avg_importance_abs = torch.abs(avg_importance)  # 计算绝对值
#     avg_feature_importances.append(avg_importance.cpu().detach().numpy())
#
# # 创建 DataFrame
# columns = [i for i in range(1, 335)]
# index = ['1', '2', '3', '4']
# feature_importance_df = pd.DataFrame(avg_feature_importances, index=index, columns=columns)
# print(feature_importance_df.head())

"""
     Counterfactual Explanation
"""
results = evaluate(test_loader)
print(results)


# get all edge index
data_edge_index = None
prev_edge_index = None
# store edge types according to edge index
edge_type_store = {}
for i, key in enumerate(data.edge_index_dict.keys()):
    if key == ('author', 'to', 'paper'):
        print(key)
        data_edge_index = data.edge_index_dict[key]
        for item in data.edge_index_dict[key].t():
            edge_type_store[f"{item[0]},{item[1]}"] = i
        if prev_edge_index is not None:
            data_edge_index = torch.cat((data_edge_index, prev_edge_index), dim=1)
        prev_edge_index = data_edge_index
print(data_edge_index.shape)

# get data_x
data_x = data.x_dict['author']
print(data_x.shape)

data_x_node_feat = data_x[:, :64]
print(data_x_node_feat.shape)
data_x_node_type = data_x[:, 64:]
print(data_x_node_type.shape)

print(data_x_node_type)
print(torch.unique(data_x_node_type, dim=0))

# Need to find another way to encode the node type information

edge_t0 = torch.unique(data_x_node_type, dim=0)[0]
edge_t1 = torch.unique(data_x_node_type, dim=0)[1]
edge_t2 = torch.unique(data_x_node_type, dim=1)[269]
print(edge_t2)
data_x_node_type_transformed = []
for item in data_x_node_type:
    if (item==edge_t0).all():
        data_x_node_type_transformed.append(0)
    if (item==edge_t1).all():
        data_x_node_type_transformed.append(1)
    if (item==edge_t2).all():
        data_x_node_type_transformed.append(2)
print(np.unique(data_x_node_type_transformed))
print(len(data_x_node_type_transformed))

# construct neighbouring graph
def get_1hop_complete_subgraph(data_edge_index, node_idx):
    """
    from the input graph, specify a node index, get 1-hop neighbourhood of the node,
    both from source to target (input node as target)
    and from target to source (input node as source)
    :param data: torch geometric data, full dataset
    :param node_idx: int, node index
    :return: tensor list of neighbouring node indices including node index, tensor list of edges
    """
    stt_subgraph_info = k_hop_subgraph(node_idx=node_idx, num_hops=1, edge_index=data_edge_index[('author', 'to', 'paper')],
                                       relabel_nodes=False, flow="source_to_target")
    tts_subgraph_info = k_hop_subgraph(node_idx=node_idx, num_hops=1, edge_index=data_edge_index[('paper', 'to', 'author')],
                                       relabel_nodes=False, flow="target_to_source")
    stt_nodes = stt_subgraph_info[0]
    tts_nodes = tts_subgraph_info[0]
    subg_nodes = stt_nodes # torch.unique(torch.cat((stt_nodes, tts_nodes)))
    stt_edges = stt_subgraph_info[1].t()
    tts_edges = tts_subgraph_info[1].t()
    subg_edges = stt_edges #torch.unique(torch.cat((stt_edges, tts_edges), dim=0), dim=0)
    return subg_nodes, subg_edges

def graph_dissimilarity(data_x, data_x_node_type_transformed, edge_type_store, node_idx_1, subg_nodes_1, subg_edges_1, subg_nodes_2, subg_edges_2,
                        lamb_node=1, lamb_node_type=1, lamb_e=1, lamb_g=1):
    data_x_node_feat = data_x
    node_feat_dissim = node_features_dissimilarity(data_x_node_feat, node_idx_1, subg_nodes_2)
    node_type_dissim = node_type_dissimilarity(data_x_node_type_transformed, subg_nodes_1, subg_nodes_2)
    edge_feat_dissim = edge_features_dissimilarity(edge_type_store, subg_edges_1, subg_edges_2)
    graph_structure_dissim = graph_structure_dissimilarity(subg_edges_1, subg_edges_2)
    return lamb_node * node_feat_dissim + lamb_node_type*node_type_dissim + lamb_e * edge_feat_dissim + lamb_g * graph_structure_dissim, [node_feat_dissim, node_type_dissim, edge_feat_dissim, graph_structure_dissim]


# node features dissimilarity ranged [0, 1]:
# normalised L2 distance + cosine distance, between the input node 1 and the neighbouring nodes of node 2
def node_features_dissimilarity(data_x, node_idx_1, sug_nodes_2):
    feat1 = data_x[node_idx_1].view(1, -1)
    feat2 = data_x[sug_nodes_2]
    return (torch.norm(feat2 - feat1, p=2, dim=1).mean() / (
            (torch.norm(feat2, p=2, dim=1).mean()) + torch.norm(feat1, p=2)) + (
                    1 - (cosine_similarity(feat1, feat2).mean() + 1) / 2)) / 2

# Multiset Jaccard distance
def node_type_dissimilarity(data_x_node_type_transformed, subg_nodes_1, subg_nodes_2):
    dict_a = get_node_type_count(data_x_node_type_transformed, subg_nodes_1)
    dict_b = get_node_type_count(data_x_node_type_transformed, subg_nodes_2)

    numerator = 0
    denominator = 0

    for i in [0, 1, 2]:
        try:
            count_a = dict_a[i]
        except:
            count_a = 0
        try:
            count_b = dict_b[i]
        except:
            count_b = 0
        numerator += min(count_a, count_b)
        denominator += max(count_a, count_b)
    return 1-numerator/denominator


def get_node_type_count(data_x_node_type_transformed, subg_nodes):
    a = np.array(data_x_node_type_transformed)[[subg_nodes]].flatten()
    unique_a = torch.unique(torch.from_numpy(a), return_counts=True)

    dict_a = {}
    for i, type in enumerate(unique_a[0]):
        dict_a[int(type)] = int(unique_a[1][i])
    return dict_a


    # edge features dissimilarity ranged [0, 1]:
def edge_features_dissimilarity(edge_idx_store, subg_edges_1, subg_edges_2):
    idxs1 = get_edge_type(edge_idx_store, subg_edges_1)
    idxs2 = get_edge_type(edge_idx_store, subg_edges_2)
    return (idxs1.mean() - idxs2.mean()).abs() / 4

def get_edge_type(edge_type_store, subg_edges):
    idxs = []
    for item in subg_edges:
        #print(f"{item[0]},{item[1]}")
        idxs.append(edge_type_store[f"{item[0]},{item[1]}"])
    return torch.Tensor(idxs).to(torch.float)

# graph structure dissimilarity
def graph_structure_dissimilarity(subg_edges_1, subg_edges_2):
    return abs(len(subg_edges_1) - len(subg_edges_2)) / max(len(subg_edges_1), len(subg_edges_2))



def compute_counterfactual(node_idx, target_prediction, predictions, data_edge_index, data_x, edge_type_store, data_x_node_type_transformed, mixed_idxs=None):
    """
    compute one counterfactual for node indicated by node_idx
    :param node_idx: int
    :param predictions: predictions, either regression or classification
    :param target_prediction
    :param data
    :return: counterfactual node, counterfactual subgraph, input subgraph, graph dissimilarity with input graph
    """
    if target_prediction == "mixed":
        candidate_ces_idxs = mixed_idxs
    else:
        candidate_ces_idxs = torch.where(predictions==target_prediction)[0]
    explainee_sgraph = get_1hop_complete_subgraph(data_edge_index, node_idx)
    optimal_ce_node = None
    optimal_ce_sgraph = None
    optimal_ce_dissimilarity = 100
    optimal_ce_dissimilarities = None
    for idx in candidate_ces_idxs:
        print(idx)
        candidate_ce_sgraph = get_1hop_complete_subgraph(data_edge_index, int(idx))
        print(candidate_ce_sgraph[0])
        this_ce_dissimilarity, this_dissimilarities = graph_dissimilarity(data_x,
                                                                          data_x_node_type_transformed,
                                                                          edge_type_store,
                                                                          node_idx,
                                                                          explainee_sgraph[0],
                                                                          explainee_sgraph[1],
                                                                          candidate_ce_sgraph[0],
                                                                          candidate_ce_sgraph[1])
        if this_ce_dissimilarity <= optimal_ce_dissimilarity:
            optimal_ce_dissimilarity = this_ce_dissimilarity
            optimal_ce_node = idx
            optimal_ce_sgraph = candidate_ce_sgraph
            optimal_ce_dissimilarities = this_dissimilarities
    return optimal_ce_node, optimal_ce_sgraph, explainee_sgraph, optimal_ce_dissimilarity, optimal_ce_dissimilarities



# get all predictions and transform to classification labels
y_pred = model(data.x_dict, data.edge_index_dict)
y_pred_class = torch.argmax(y_pred, dim=-1)
edge_idx_store = edge_type_store

print(torch.where(y_pred_class==0))

# want to explain the prediction result for node
node_idx = 9
print("original prediction: class", int(y_pred_class[node_idx]))

# want to find counterfactual explanation for class 5
target_prediction = 3
optimal_ce_node, optimal_ce_sgraph, explainee_sgraph, optimal_ce_dissimilarity, optimal_ce_dissimilarities = compute_counterfactual(node_idx=node_idx, target_prediction=target_prediction, predictions=y_pred_class, data_edge_index=data.edge_index_dict, data_x=data_x, data_x_node_type_transformed=data_x_node_type_transformed, edge_type_store=edge_type_store)


# print out counterfactual node information
print(optimal_ce_node)
print(y_pred_class[optimal_ce_node])
print(optimal_ce_dissimilarity)
print(optimal_ce_dissimilarities)

