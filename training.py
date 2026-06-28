import torch

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader

from model_classes import REG_GNN, HGT_CLASS, REG_GAT, HGT
from torchmetrics import F1Score, ConfusionMatrix, Accuracy

import sup_funcs as sf
import pandas as pd
from tqdm import tqdm
import os

import matplotlib.pyplot as plt
import networkx as nx

class Modelling:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])
        self.viewpoint_object = self.path_dict['kpi_viewpoint']
        self.device = torch.device('cpu')

        # Load relevant datasets
        self.train_data = torch.load(f"{self.path_dict['pytorch_path']}/train_graphs_sg.pt", weights_only=False)
        self.val_data = torch.load(f"{self.path_dict['pytorch_path']}/val_graphs_sg.pt", weights_only=False)
        self.test_data = torch.load(f"{self.path_dict['pytorch_path']}/test_graphs_sg.pt", weights_only=False)

        kpi_type = self.path_dict['kpi_type']
        if kpi_type == 0:  # Regression
            self.model = HGT.HGT(hidden_channels=24, out_channels=1, num_layers=2,
                                 num_heads=2, data=self.train_data[0], viewpoint=self.viewpoint_object)
            test_kpi = f"TimeFrom_{self.viewpoint_object}_to_{self.path_dict['kpi_event']}"
        elif kpi_type == 1:
            self.model = HGT_CLASS.HGT_CLASS(hidden_channels=64, out_channels=2, num_heads=2,
                                             num_layers=2, data=self.train_data[0],
                                             viewpoint=self.viewpoint_object)
            test_kpi = f"Classifier_{self.path_dict['kpi_event']}"
        self.model = self.model.to(self.device)

        # Define save path for the models
        model_path = self.path_dict['model_path']

        if not os.path.exists(f"{model_path}/Hetero"):
            os.makedirs(f"{model_path}/Hetero")
        self.model_path = f"{model_path}/Hetero/{test_kpi}.pth"

        # Normalize using train-graph statistics only, gathered across ALL of
        # their viewpoint object nodes (the multi-graph analogue of the train_mask slice
        # used in the single-large-graph version).
        train_y_all = torch.cat([g[self.viewpoint_object].y for g in self.train_data])
        target_mean, target_std = train_y_all.mean(), train_y_all.std()
        for m in [self.train_data, self.val_data, self.test_data]:
            for g in m:
                g[self.viewpoint_object].y = (g[self.viewpoint_object].y - target_mean) / target_std
        print(f"Mean (hours): {round(target_mean.item() / 3600)}, STD (hours): {round(target_std.item() / 3600)}")
        self.target_mean, self.target_std = target_mean.to(self.device), target_std.to(self.device)

    def decode_epoch(self, epoch_val):
        timestamp = pd.Timestamp(epoch_val, unit='s')
        return timestamp

    def decode_time(self, total_secs):
        timestamp = pd.Timedelta(round(total_secs, 2), unit='s')
        return timestamp

    """
        Heterogeneous Regression training and validation functions
    """
    def het_train(self, model, train_loader, optimizer, criterion, device):
        model.train()
        total_loss, total_examples = 0.0, 0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x_dict, batch.edge_index_dict)
            y = batch[self.viewpoint_object].y
            loss = criterion(out, y)
            loss.backward()
            optimizer.step()

            batch_size = len(batch[self.viewpoint_object].batch)
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def het_loss_test(self, loader, model, criterion, device):
        model.eval()

        total_loss, total_examples = 0.0, 0
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x_dict, batch.edge_index_dict)
            y = batch[self.viewpoint_object].y
            loss = criterion(out, y)

            batch_size = len(batch[self.viewpoint_object].batch)
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    """
        Hetero Classifier training and validation functions
    """
    def class_train(self, train_loader, optimizer, criterion):
        self.model.train()

        total_loss = total_examples = 0
        for batch in train_loader:
            batch = batch.to(self.device)
            optimizer.zero_grad()
            batch_size = len(batch[self.viewpoint_object].batch)
            out = self.model(batch.x_dict, batch.edge_index_dict)
            seed_out = out[:batch_size]
            seed_y = batch[self.viewpoint_object].y[:batch_size]

            loss = criterion(seed_out, seed_y)
            loss.backward()
            optimizer.step()

            total_loss += loss.item() * batch_size
            total_examples += batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def class_eval(self, loader):
        self.model.eval()
        total_correct = total_examples = 0
        f1 = F1Score("binary")
        for batch in loader:
            batch = batch.to(self.device)
            out = self.model(batch.x_dict, batch.edge_index_dict)
            batch_size = len(batch[self.viewpoint_object].batch)
            seed_out = out[:batch_size].argmax(dim=-1)
            seed_y = batch[self.viewpoint_object].y[:batch_size]

            total_correct += (seed_out == seed_y).sum().item()
            f1(seed_out, seed_y)
            total_examples += batch_size

        return total_correct / total_examples, f1.compute().item()

    # Update the regression model
    def Het_Reg_Modelling(self, training_data, val_data, test_data):
        """
        :param het_train_data:
        :param het_val_data:
        :param het_test_data:
        :return: Trains and tests a regression model for heterogeneous graph structures for comparison with the
        homogeneous model predictions and explanations.
        """
        viewpoint_object = self.viewpoint_object

        # Create the loaders for training and validation
        train_loader = DataLoader(training_data, batch_size=16, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=16)
        test_loader = DataLoader(test_data, batch_size=16)

        # Define save path for the models
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        test_kpi = f"TimeFrom_{self.viewpoint_object}_to_{kpi_event}"

        if not os.path.exists(f"{model_path}/Hetero"):
            os.makedirs(f"{model_path}/Hetero")
        model_path = f"{model_path}/Hetero/{test_kpi}.pth"

        # Model values
        data = training_data[0]
        model = HGT.HGT(hidden_channels=24, out_channels=1, num_layers=2,
                        num_heads=2, data=data, viewpoint=viewpoint_object)
        device = torch.device("cpu")
        model = model.to(device)
        data = data.to(device)
        criterion = torch.nn.L1Loss()

        with torch.no_grad():
            batch = next(iter(train_loader))
            model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-5)

        best_val = 10e7

        pbar = tqdm(range(1, 30))
        for epoch in pbar:
            loss = self.het_train(model, train_loader, optimizer, criterion, device)
            val_mse = self.het_loss_test(val_loader, model, criterion, device)  # Should use a separate validation set loader
            print(f'Epoch: {epoch:03d}, Loss: {loss:.4f}, Val MSE: {val_mse:.4f}')
            if val_mse < best_val:
                best_val = val_mse
                print("New best!")
                torch.save(model.state_dict(), model_path)
        pbar.close()

        # num_layers = 5
        # width_layers = 8
        # num_heads = 3
        # hidden_channels = [width_layers] * num_layers
        # criterion = torch.nn.L1Loss()
        # learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
        # patience = 5
        # epochs_sg = 10
        # """
        #     Heterogeneous model training loop:
        #     Trains 5 different instances of the model with decreasing learning rates in each epoch.
        # """
        # to_train = [i for i in range(1, 6)]
        # flag = True
        # while flag:
        #     for i in to_train:
        #         model = HET_GNN.HeteroGNN(hidden_channels=hidden_channels, out_channels=1, num_layers=num_layers,
        #                                   num_heads=num_heads, data=training_data[0], viewpoint=viewpoint_object)
        #         model = model.to(device)
        #         best_val = 10e7
        #         counter = 0
        #         for epoch, lr in enumerate(learning_rates):
        #             optimizer = torch.optim.Adam(model.parameters(), lr=lr)
        #             loss = self.het_train(model, train_loader, optimizer, criterion, device)
        #             val_loss = self.het_loss_test(val_loader, model, criterion, device)
        #             print(f"{i} - Epoch: {epoch:03d}, LR: {lr}, Loss: {loss:.4f}, Val Loss: {val_loss}")
        #             if val_loss < best_val:
        #                 print('New Best')
        #                 best_val = val_loss
        #                 torch.save(model.state_dict(),
        #                            f"{model_path}_{i}.pth")
        #                 counter = 0
        #                 model_name = f"{test_kpi}_{i}.pth"
        #             else:
        #                 counter += 1
        #
        #             if counter > patience:
        #                 print('---')
        #                 break
        #         if epoch + 1 >= epochs_sg:
        #             to_train.remove(i)
        #     if len(to_train) == 0:
        #         flag = False

        test_loss = self.het_loss_test(test_loader, model, criterion, device)
        print(f'Final MAE: {test_loss}')

        #
        # # Save the results in a result file
        # self.SaveResults('Heterogeneous', test_kpi, test_loss, mean, std, model_name)

    def BinaryModelling(self, training_data, val_data, test_data):
        viewpoint_object = self.viewpoint_object

        # Create loaders
        train_loader = DataLoader(training_data, batch_size=16, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=16)
        test_loader = DataLoader(test_data, batch_size=16)

        # Choose criterion
        criterion = F.cross_entropy

        # Materialize HGTConv's lazy linear layers with one real forward pass
        # before constructing the optimizer (same reason as the regression example).
        with torch.no_grad():
            batch = next(iter(train_loader))
            self.model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)#, weight_decay=1e-5)

        # Run training loop
        best_val = 0.0
        pbar = tqdm(range(1, 51))

        for epoch in pbar:
            train_loss = self.class_train(train_loader, optimizer, criterion)
            val_acc, val_f1 = self.class_eval(val_loader)

            print(f'Epoch: {epoch:03d}, Loss: {train_loss:.4f}, Val ACC: {val_acc:.4f} | Val F1: {val_f1:.4f}')
            if val_f1 > best_val:
                best_val = val_acc
                print("New best!")
                torch.save(self.model.state_dict(), self.model_path)

    """
        Model saving functions
    """
    def SaveResults(self, type, kpi, value, mean, std, model):
        # Open the results file
        results = pd.read_csv(self.path_dict['results_path'])

        for i, row in results.iterrows():
            if row['KPI'] == kpi:
                if row['Graph Type'] == type:
                    val = row['Metric']
                    if value < val:
                        results.loc[(results.KPI == kpi) & (results['Graph Type'] == type), 'Metric'] = value
                        results.loc[(results.KPI == kpi) & (results['Graph Type'] == type), 'Model'] = model
                    found = True
                else:
                    found = False
            else:
                found = False

        # If the KPI isnt logged, create a new entry
        if not found:
            result = {"Graph Type": [type], "KPI": [kpi], "Metric": [value],
                      "Mean": [mean.item()], "STD": [std.item()], "Model":[model]}
            res_df = pd.DataFrame(result)
            results = pd.concat([results, res_df])

        # Save the updated result file
        results.to_csv(self.path_dict['results_path'], index=False)

    def SaveBestResult(self):
        """
        :return:
        Unlike SaveResult this function checks all 5 generated models for the best one and saves it to the file so we
        can compare the most effective results for each KPI
        """

        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        kpi_type = self.path_dict['kpi_type']
        device = torch.device("cpu")
        num_node_features = 11 if self.database == 'order_management' else 14

        num_layers = 5
        width_layers = 8
        num_heads = 3
        hidden_channels = [width_layers] * num_layers

        # Prepare test data
        # Load homogeneous data sets
        file_path = self.path_dict['pytorch_path']
        hom_train_data = torch.load(f"{file_path}/train_graphs_hom.pt", weights_only=False)
        hom_test_data = torch.load(f"{file_path}/test_graphs_hom.pt", weights_only=False)

        ys = torch.cat([d.y for d in hom_train_data])
        mean, std = ys.mean(), ys.std()
        test_data = [self.normalize_target(d, mean, std) for d in hom_test_data]
        test_loader = DataLoader(test_data, batch_size=64)

        # Load heterogeneous data sets
        file_path = self.path_dict['pytorch_path']
        het_train_data = torch.load(f"{file_path}/train_graphs_sg.pt", weights_only=False)
        het_test_data = torch.load(f"{file_path}/test_graphs_sg.pt", weights_only=False)

        ys = torch.cat([d[self.viewpoint_object].y for d in het_train_data])
        mean, std = ys.mean(), ys.std()
        het_test_data = [self.normalize_het(d, mean, std) for d in het_test_data]
        het_test_loader = DataLoader(het_test_data, batch_size=64)

        if kpi_type == 0:
            kpi = f"TimeFrom_{self.viewpoint_object}_to_{kpi_event}"
        else:
            kpi = f"BinaryClass_{kpi_event}.pth"

        hom_models_path = f"{model_path}Homo"
        het_models_path = f"{model_path}Hetero"

        # Update homogeneous models
        directory = os.fsencode(hom_models_path)
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            if filename.__contains__(kpi):
                model = REG_GNN.REG_GNN(in_channels=num_node_features, hidden_channels=64, num_layers=3)
                model.load_state_dict(torch.load(f"{hom_models_path}/{filename}", weights_only=False))
                criterion = torch.nn.L1Loss()

                test_mae = self.loss_test(test_loader, model, criterion, device)
                self.SaveResults('Homogeneous', kpi, test_mae, mean, std, filename)
                print(f'Final MAE: {test_mae}')

        # Update heterogeneous models
        directory = os.fsencode(het_models_path)
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            # if filename.__contains__(kpi):
            #     model.load_state_dict(torch.load(f"{het_models_path}/{filename}", weights_only=False))
            #     criterion = torch.nn.L1Loss()
            #
            #     test_mae = self.het_loss_test(het_test_loader, model, criterion, device)
            #     self.SaveResults('Heterogeneous', kpi, test_mae, mean, std, filename)
            #     print(f'Final MAE: {test_mae}')

    """
        Explanation Function
    """
    @torch.no_grad()
    def _predict_proba(self, batch):
        batch = batch.to(self.device)
        out = self.model(batch.x_dict, batch.edge_index_dict)
        seed_out = out[:1]
        return F.softmax(seed_out, dim=-1)[0]

    def feature_importance_for_node(self, batch, node_type, node_idx, baseline_confidence,
                                    predicted_class, top_k=10):
        """
        Leave-one-out at the FEATURE level: for one specific node, zero out
        each feature dimension individually and measure the resulting drop in
        confidence for the original predicted class. This refines the
        node-level analysis above -- "paper[12] matters a lot" becomes
        "...specifically because of features 3, 7, and 19" (e.g. particular
        words in a bag-of-words representation).
        """
        x = batch[node_type].x[node_idx]
        num_features = x.size(0)
        feature_importances = []  # (feature_idx, confidence_drop, flips_prediction)

        for f in range(num_features):
            if x[f].item() == 0.0:
                continue  # already zero -- nothing to remove, skip for speed
            perturbed = batch.clone()
            perturbed[node_type].x[node_idx, f] = 0.0
            proba = self._predict_proba(perturbed)
            confidence_drop = baseline_confidence - proba[predicted_class].item()
            flips = proba.argmax().item() != predicted_class
            feature_importances.append((f, confidence_drop, flips))

        feature_importances.sort(key=lambda t: t[1], reverse=True)
        return feature_importances[:top_k]

    def build_explanation_subgraph(self, batch, node_importances, edge_importances,
                                   node_top_k=10, edge_top_k=15):
        """
        Turn the counterfactual node/edge importance scores into an actual
        NetworkX subgraph -- the "explanation subgraph": the seed author plus
        only the most important surrounding nodes and edges, rather than the
        entire (much larger) sampled neighborhood. Conceptually the same idea
        as GNNExplainer's `get_explanation_subgraph()`, just built from our
        leave-one-out scores instead of learned soft masks.
        """
        import networkx as nx

        G = nx.MultiDiGraph()

        # Always include the seed author node.
        G.add_node((self.viewpoint_object, 0), node_type=self.viewpoint_object, importance=1.0, is_seed=True, flips=False)

        # Keep only the top-k most important neighbor NODES (by confidence drop).
        top_nodes = node_importances[:node_top_k]
        for nt, i, drop, flips in top_nodes:
            G.add_node((nt, i), node_type=nt, importance=drop, is_seed=False, flips=flips)

        # Keep only the top-k most important EDGES, adding their endpoints if
        # not already present -- an edge can matter even if one endpoint
        # individually didn't make the node top-k cut.
        top_edges = edge_importances[:edge_top_k]
        for edge_type, e, drop, flips in top_edges:
            src_type, _, dst_type = edge_type
            edge_index = batch[edge_type].edge_index
            src, dst = edge_index[:, e].tolist()
            src_key, dst_key = (src_type, src), (dst_type, dst)

            for key, ntype in [(src_key, src_type), (dst_key, dst_type)]:
                if key not in G.nodes:
                    is_seed = (ntype == self.viewpoint_object and key[1] == 0)
                    G.add_node(key, node_type=ntype, importance=0.0, is_seed=is_seed, flips=False)

            G.add_edge(src_key, dst_key, edge_type=edge_type[1], importance=drop, flips=flips)

        return G

    def visualize_explanation_subgraph(self, G, save_path="explanation_subgraph.png"):
        """Draw the explanation subgraph: node color = type, node size =
        importance, red edges/outlines = "removing this flips the prediction"."""
        import matplotlib.pyplot as plt
        import networkx as nx

        type_colors = {"author": "#4C72B0", "paper": "#DD8452", "term": "#55A868", "conference": "#C44E52"}

        pos = nx.spring_layout(G, seed=42, k=0.9)

        node_colors, node_sizes, edge_colors_outline = [], [], []
        for node, attrs in G.nodes(data=True):
            node_colors.append(type_colors.get(attrs["node_type"], "gray"))
            base_size = 900 if attrs.get("is_seed") else 250
            node_sizes.append(base_size + max(attrs.get("importance", 0), 0) * 2500)
            edge_colors_outline.append("red" if attrs.get("flips") else "black")

        edge_colors, edge_widths = [], []
        for _, _, attrs in G.edges(data=True):
            edge_colors.append("red" if attrs.get("flips") else "gray")
            edge_widths.append(1 + max(attrs.get("importance", 0), 0) * 12)

        plt.figure(figsize=(10, 8))
        nx.draw_networkx_nodes(
            G, pos, node_color=node_colors, node_size=node_sizes,
            edgecolors=edge_colors_outline, linewidths=1.5, alpha=0.9,
        )
        nx.draw_networkx_edges(
            G, pos, edge_color=edge_colors, width=edge_widths,
            arrows=True, connectionstyle="arc3,rad=0.1", alpha=0.7,
        )
        labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=7)

        legend_handles = [
            plt.Line2D([0], [0], marker="o", color="w", label=nt,
                       markerfacecolor=color, markersize=10)
            for nt, color in type_colors.items()
        ]
        legend_handles.append(plt.Line2D([0], [0], color="red", lw=2, label="flips prediction"))
        plt.legend(handles=legend_handles, loc="best", fontsize=8)

        plt.title("Counterfactual Explanation Subgraph")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"Saved explanation subgraph visualization to {save_path}")

    def class_explanation(self, explain_subgraph, object_idx, top_k):
        baseline_proba = self._predict_proba(explain_subgraph)
        predicted_class = baseline_proba.argmax().item()
        baseline_confidence = baseline_proba[predicted_class].item()

        print(f"\nExplaining {self.viewpoint_object} node #{object_idx}")
        print(f"  Predicted class: {predicted_class} (confidence {baseline_confidence:.4f})")
        print(f"  Sampled neighborhood: " +
              ", ".join(f"{nt}={explain_subgraph[nt].num_nodes}" for nt in explain_subgraph.node_types))

        # --- leave-one-out over every NODE in the sampled neighborhood ---
        node_importances = []  # (node_type, local_idx, confidence_drop, flips_prediction)
        for node_type in explain_subgraph.node_types:
            n = explain_subgraph[node_type].x.size(0)
            # Skip the seed author itself (always index 0 for 'author') -- we
            # want to know what its prediction depends ON, not what removing
            # the node being predicted does (that's trivially destructive).
            start = 1 if node_type == self.viewpoint_object else 0
            for i in range(start, n):
                perturbed = explain_subgraph.clone()
                perturbed[node_type].x[i] = 0.0  # "remove" this node's signal
                proba = self._predict_proba(perturbed)
                confidence_drop = baseline_confidence - proba[predicted_class].item()
                flips = proba.argmax().item() != predicted_class
                node_importances.append((node_type, i, confidence_drop, flips))

        # --- leave-one-out over every EDGE in the sampled neighborhood ---
        edge_importances = []  # (edge_type, position, confidence_drop, flips_prediction)
        for edge_type in explain_subgraph.edge_types:
            edge_index = explain_subgraph[edge_type].edge_index
            num_edges = edge_index.size(1)
            for e in range(num_edges):
                perturbed = explain_subgraph.clone()
                keep = torch.ones(num_edges, dtype=torch.bool, device=self.device)
                keep[e] = False
                perturbed[edge_type].edge_index = edge_index[:, keep]
                proba = self._predict_proba(perturbed)
                confidence_drop = baseline_confidence - proba[predicted_class].item()
                flips = proba.argmax().item() != predicted_class
                edge_importances.append((edge_type, e, confidence_drop, flips))

        node_importances.sort(key=lambda t: t[2], reverse=True)
        edge_importances.sort(key=lambda t: t[2], reverse=True)

        # --- feature-level counterfactual on the SEED author's own input ---
        seed_feature_importances = self.feature_importance_for_node(
            explain_subgraph, self.viewpoint_object, 0, baseline_confidence, predicted_class, top_k=top_k
        )

        # --- feature-level counterfactual on the single most influential
        #     NEIGHBOR node, since "this node matters" is coarser than
        #     "these specific features of this node matter" ---
        if node_importances:
            top_node_type, top_node_idx, _, _ = node_importances[0]
            top_node_feature_importances = self.feature_importance_for_node(
                explain_subgraph, top_node_type, top_node_idx, baseline_confidence, predicted_class, top_k=top_k
            )
        else:
            top_node_type, top_node_idx = None, None
            top_node_feature_importances = []
        print(f"\n  Top {top_k} most important NODES (confidence drop if removed):")
        for node_type, i, drop, flips in node_importances[:top_k]:
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {node_type}[{i}]: confidence drop = {drop:+.4f}{flag}")

        print(f"\n  Top {top_k} most important EDGES (confidence drop if removed):")
        for edge_type, e, drop, flips in edge_importances[:top_k]:
            src, dst = explain_subgraph[edge_type].edge_index[:, e].tolist()
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {edge_type} edge ({src} -> {dst}): confidence drop = {drop:+.4f}{flag}")

        print(f"\n  Top {top_k} most important FEATURES on the seed {self.viewpoint_object} itself:")
        for f, drop, flips in seed_feature_importances:
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {self.viewpoint_object}[0].x[{f}]: confidence drop = {drop:+.4f}{flag}")

        if top_node_type is not None:
            print(f"\n  Top {top_k} most important FEATURES on the most influential neighbor "
                  f"({top_node_type}[{top_node_idx}]):")
            for f, drop, flips in top_node_feature_importances:
                flag = "  <-- FLIPS PREDICTION" if flips else ""
                print(f"    {top_node_type}[{top_node_idx}].x[{f}]: confidence drop = {drop:+.4f}{flag}")

        any_flips = (
                any(f for *_, f in node_importances)
                or any(f for *_, f in edge_importances)
                or any(f for *_, f in seed_feature_importances)
                or any(f for *_, f in top_node_feature_importances)
        )
        if any_flips:
            print("\n  >>> A genuine counterfactual exists above: removing that single")
            print("      node, edge, or feature changes the model's predicted class entirely.")
        else:
            print("\n  >>> No single node/edge/feature removal flips the prediction -- the")
            print("      model's decision here is robust to any one removal (it relies on")
            print("      combined, redundant evidence rather than one critical piece).")


        """
            Build the explanation subgraph
        """
        explanation_graph = self.build_explanation_subgraph(
            explain_subgraph, node_importances, edge_importances, node_top_k=10, edge_top_k=15
        )
        self.visualize_explanation_subgraph(explanation_graph, save_path="explanation_subgraph.png")

    @torch.no_grad()
    def _predict_value_for_graph(self, graph, object_idx, perturbed_graph=None):
        """Run the model on (a possibly perturbed copy of) ONE small graph
        and return the de-normalized prediction for one paper in it."""
        g = perturbed_graph if perturbed_graph is not None else graph
        out = self.model(g.x_dict, g.edge_index_dict)
        denorm = out * self.target_std + self.target_mean
        return denorm[object_idx].item()

    def reg_feature_importance_for_node_in_graph(self, graph, node_type, node_idx, baseline_value,
                                                 target_object_idx, top_k=10):
        """Leave-one-out at the FEATURE level, same idea as the single-
        large-graph version, just scoped to one small graph's tensors."""
        x = graph[node_type].x[node_idx]
        num_features = x.size(0)
        feature_importances = []
        for f in range(num_features):
            if x[f].item() == 0.0:
                continue
            perturbed = graph.clone()
            perturbed[node_type].x[node_idx, f] = 0.0
            pred = self._predict_value_for_graph(graph, target_object_idx, perturbed_graph=perturbed)
            shift = abs(baseline_value - pred)
            large_shift = shift > self.target_std.item()
            feature_importances.append((f, shift, large_shift))
        feature_importances.sort(key=lambda t: t[1], reverse=True)
        return feature_importances[:top_k]

    def reg_explanation(self, explain_subgraph, object_idx, top_k):
        baseline_value = self._predict_value_for_graph(explain_subgraph, object_idx)
        print(f"\nExplaining {self.viewpoint_object} #{0} in graph #{object_idx}")
        print(f"  Predicted impact score: {baseline_value:.4f}")
        print("  Graph size: " +
              ", ".join(f"{nt}={explain_subgraph[nt].num_nodes}" for nt in explain_subgraph.node_types))

        # --- leave-one-out over every NODE in this graph ---
        node_importances = []  # (node_type, idx, value_shift, large_shift)
        for node_type in explain_subgraph.node_types:
            n = explain_subgraph[node_type].x.size(0)
            for idx in range(n):
                if node_type == "paper" and idx == object_idx:
                    continue  # skip the target itself, same reasoning as before
                perturbed = explain_subgraph.clone()
                perturbed[node_type].x[idx] = 0.0
                pred = self._predict_value_for_graph(explain_subgraph, object_idx, perturbed_graph=perturbed)
                shift = abs(baseline_value - pred)
                large_shift = shift > self.target_std.item()
                node_importances.append((node_type, idx, shift, large_shift))

            # --- leave-one-out over every EDGE in this graph ---
            edge_importances = []  # (edge_type, position, value_shift, large_shift)
            for edge_type in explain_subgraph.edge_types:
                edge_index = explain_subgraph[edge_type].edge_index
                num_edges = edge_index.size(1)
                for e in range(num_edges):
                    perturbed = explain_subgraph.clone()
                    keep = torch.ones(num_edges, dtype=torch.bool, device=self.device)
                    keep[e] = False
                    perturbed[edge_type].edge_index = edge_index[:, keep]
                    pred = self._predict_value_for_graph(explain_subgraph, object_idx, perturbed_graph=perturbed)
                    shift = abs(baseline_value - pred)
                    large_shift = shift > self.target_std.item()
                    edge_importances.append((edge_type, e, shift, large_shift))

            node_importances.sort(key=lambda t: t[2], reverse=True)
            edge_importances.sort(key=lambda t: t[2], reverse=True)

            seed_feature_importances = self.reg_feature_importance_for_node_in_graph(
                explain_subgraph, self.viewpoint_object, object_idx, baseline_value, object_idx, top_k=top_k
            )

            if node_importances:
                top_node_type, top_node_idx, _, _ = node_importances[0]
                top_node_feature_importances = self.reg_feature_importance_for_node_in_graph(
                    explain_subgraph, top_node_type, top_node_idx, baseline_value, object_idx, top_k=top_k
                )
            else:
                top_node_type, top_node_idx = None, None
                top_node_feature_importances = []

            print(f"\n  Top {top_k} most important NODES (predicted value shift if removed):")
            for node_type, i, shift, large in node_importances[:top_k]:
                flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
                print(f"    {node_type}[{i}]: value shift = {shift:.4f}{flag}")

            print(f"\n  Top {top_k} most important EDGES (predicted value shift if removed):")
            for edge_type, e, shift, large in edge_importances[:top_k]:
                src, dst = explain_subgraph[edge_type].edge_index[:, e].tolist()
                flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
                print(f"    {edge_type} edge ({src} -> {dst}): value shift = {shift:.4f}{flag}")

            print(f"\n  Top {top_k} most important FEATURES on the seed paper itself:")
            for f, shift, large in seed_feature_importances:
                flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
                print(f"    paper[{object_idx}].x[{f}]: value shift = {shift:.4f}{flag}")

            if top_node_type is not None:
                print(f"\n  Top {top_k} most important FEATURES on the most influential neighbor "
                      f"({top_node_type}[{top_node_idx}]):")
                for f, shift, large in top_node_feature_importances:
                    flag = "  <-- LARGE SHIFT (>1 std)" if large else ""
                    print(f"    {top_node_type}[{top_node_idx}].x[{f}]: value shift = {shift:.4f}{flag}")

            any_large = (
                    any(l for *_, l in node_importances)
                    or any(l for *_, l in edge_importances)
                    or any(l for *_, l in seed_feature_importances)
                    or any(l for *_, l in top_node_feature_importances)
            )
            if any_large:
                print("\n  >>> At least one single node, edge, or feature removal above shifts the")
                print("      prediction by more than one target standard deviation.")
            else:
                print("\n  >>> No single removal shifts the prediction by more than one target")
                print("      standard deviation -- this prediction is robust to any one removal.")

            return node_importances, edge_importances, seed_feature_importances, top_node_feature_importances

    def cf_explanation(self):
        object_idx = 1971 #1971 1964 1945 1798 1755 1989
        top_k = 5

        # Load saved model for explanation
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))

        # Select a subgraph to explain
        for x in self.test_data:
            if x['Orders']['last_event'] == True:
                # print(x['Orders'])
                if x['Orders']['id'] == object_idx:
                    explain_subgraph = x
                    break

        if self.path_dict['kpi_type'] == 0:
            self.reg_explanation(explain_subgraph, 0, top_k)
        elif self.path_dict['kpi_type'] == 1:
            self.class_explanation(explain_subgraph, object_idx, top_k)

    def Modelling(self):
        """
            Main function, obtains the relevant files and selects the appropriate training and validation functions to
            run for the chosen KPI
        """
        kpi_type = self.path_dict['kpi_type']
        if kpi_type == 0: # Regression
            self.Het_Reg_Modelling(self.train_data, self.val_data, self.test_data)
        elif kpi_type == 1: #Binary Classification
            self.BinaryModelling(self.train_data, self.val_data, self.test_data)