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
import copy

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
            self.model = HGT.HGT(hidden_channels=64, out_channels=1, num_layers=3,
                                 num_heads=4, data=self.train_data[0], viewpoint=self.viewpoint_object)
            test_kpi = f"TimeFrom_{self.viewpoint_object}_to_{self.path_dict['kpi_event']}"

            # Standardize the output values
            train_y_all = torch.cat([g[self.viewpoint_object].y for g in self.train_data])
            target_mean, target_std = train_y_all.mean(), train_y_all.std()
            target_median = train_y_all.median()
            for m in [self.train_data, self.val_data, self.test_data]:
                for g in m:
                    g[self.viewpoint_object].y = (g[self.viewpoint_object].y - target_mean) / target_std
            print(f"Mean (hours): {round(target_mean.item() / 3600)}, STD (hours): {round(target_std.item() / 3600)}")
            print(f"Median (hours): {round(target_median.item() / 3600)}")
            self.target_mean, self.target_std = target_mean.to(self.device), target_std.to(self.device)

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

        # Build human-readable feature name registry for explainability
        self.feature_names = self._build_feature_names()

        # Max feature dim across node types — used for on-the-fly hom conversion
        self.hom_in_channels = max(
            self.train_data[0][nt].x.size(1) for nt in self.train_data[0].node_types
        )

    def _build_feature_names(self):
        """Build a dict mapping node_type -> list[str] of feature names, in feature-index order."""
        names = {}

        # Events: one-hot of event types sorted alphabetically (matches table_generation ORDER BY 1)
        ev_types = sorted(self.pd_df['type'].unique())
        names['Events'] = ev_types

        # Attribute-bearing object types from config
        attributes = self.path_dict.get('attributes', {})
        for node_type, attrs in attributes.items():
            names[node_type] = list(attrs)

        # Time-sensitive attributes (Products, etc.)
        time_attrs = self.path_dict.get('time_attributes', {})
        for node_type, attrs in time_attrs.items():
            names.setdefault(node_type, list(attrs))

        # One-hot encoded types (Customers, Employees) — feature count from training data
        encoding_types = self.path_dict.get('encoding', [])
        for node_type in encoding_types:
            if self.train_data and node_type in self.train_data[0].node_types:
                n_feats = self.train_data[0][node_type].x.size(1)
                names[node_type] = [f"{node_type.lower()}_enc_{i}" for i in range(n_feats)]

        return names

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
            mask = batch[self.viewpoint_object].mask.squeeze(-1).bool()
            loss = criterion(out[mask], y[mask])
            loss.backward()
            optimizer.step()

            batch_size = mask.sum().item()
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def het_loss_test(self, loader, model, criterion, device):
        """Returns MAE (L1) regardless of training criterion, for consistent reporting."""
        model.eval()
        mae_criterion = torch.nn.L1Loss()

        total_loss, total_examples = 0.0, 0
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x_dict, batch.edge_index_dict)
            y = batch[self.viewpoint_object].y
            mask = batch[self.viewpoint_object].mask.squeeze(-1).bool()
            loss = mae_criterion(out[mask], y[mask])

            batch_size = mask.sum().item()
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def het_full_metrics(self, loader, model, device):
        """Returns (MAE, RMSE, R²) all in normalized units."""
        model.eval()
        all_preds, all_targets = [], []
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x_dict, batch.edge_index_dict)
            y = batch[self.viewpoint_object].y
            mask = batch[self.viewpoint_object].mask.squeeze(-1).bool()
            all_preds.append(out[mask])
            all_targets.append(y[mask])
        preds = torch.cat(all_preds)
        targets = torch.cat(all_targets)
        mae = (preds - targets).abs().mean().item()
        rmse = ((preds - targets) ** 2).mean().sqrt().item()
        ss_res = ((targets - preds) ** 2).sum()
        ss_tot = ((targets - targets.mean()) ** 2).sum()
        r2 = (1 - ss_res / ss_tot).item() if ss_tot > 0 else float("nan")
        return mae, rmse, r2

    """
        Homogeneous baseline helpers
    """
    def _build_hom_data(self, hetero_data_list):
        """Convert a list of HeteroData (sg, already z-normalized) to homogeneous Data objects.

        All node types are zero-padded to self.hom_in_channels and concatenated;
        all edge indices are offset-adjusted and concatenated. The graph-level
        target y is taken from the masked seed Orders node.
        """
        from torch_geometric.data import Data
        result = []
        max_feat = self.hom_in_channels

        for graph in hetero_data_list:
            node_types_sorted = sorted(graph.node_types)

            # Build padded node feature matrix and track offsets
            xs, offsets = [], {}
            offset = 0
            for nt in node_types_sorted:
                x = graph[nt].x
                d = x.size(1)
                if d < max_feat:
                    x = F.pad(x, (0, max_feat - d))
                xs.append(x)
                offsets[nt] = offset
                offset += x.size(0)
            x_all = torch.cat(xs, dim=0)

            # Build unified edge index
            edge_indices = []
            for edge_type in graph.edge_types:
                src_type, _, dst_type = edge_type
                ei = graph[edge_type].edge_index.clone()
                ei[0] += offsets[src_type]
                ei[1] += offsets[dst_type]
                edge_indices.append(ei)
            edge_index_all = torch.cat(edge_indices, dim=1) if edge_indices else torch.zeros(2, 0, dtype=torch.long)

            # Extract seed node target (mask selects the seed Orders node)
            mask = graph[self.viewpoint_object].mask.squeeze(-1).bool()
            y = graph[self.viewpoint_object].y[mask]  # shape [1]

            result.append(Data(x=x_all, edge_index=edge_index_all, y=y))
        return result

    def _hom_train(self, model, train_loader, optimizer, criterion, device):
        model.train()
        total_loss, total_examples = 0.0, 0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x, batch.edge_index, batch.batch)
            loss = criterion(out, batch.y)
            loss.backward()
            optimizer.step()
            total_loss += float(loss) * batch.num_graphs
            total_examples += batch.num_graphs
        return total_loss / total_examples

    @torch.no_grad()
    def _hom_loss_test(self, loader, model, device):
        """Returns MAE (L1) for consistent reporting."""
        model.eval()
        mae_criterion = torch.nn.L1Loss()
        total_loss, total_examples = 0.0, 0
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x, batch.edge_index, batch.batch)
            loss = mae_criterion(out, batch.y)
            total_loss += float(loss) * batch.num_graphs
            total_examples += batch.num_graphs
        return total_loss / total_examples

    @torch.no_grad()
    def _hom_full_metrics(self, loader, model, device):
        """Returns (MAE, RMSE, R²) in normalized units."""
        model.eval()
        all_preds, all_targets = [], []
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x, batch.edge_index, batch.batch)
            all_preds.append(out)
            all_targets.append(batch.y)
        preds = torch.cat(all_preds)
        targets = torch.cat(all_targets)
        mae = (preds - targets).abs().mean().item()
        rmse = ((preds - targets) ** 2).mean().sqrt().item()
        ss_res = ((targets - preds) ** 2).sum()
        ss_tot = ((targets - targets.mean()) ** 2).sum()
        r2 = (1 - ss_res / ss_tot).item() if ss_tot > 0 else float("nan")
        return mae, rmse, r2

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

    def sweep_hyperparams(self, training_data=None, val_data=None):
        """
        Short grid search over key HGT hyperparameters.
        Each config trains for up to 50 epochs (early stop patience=10) and
        the best val MAE is recorded. Saves a figure with learning curves and
        a bar chart for the top 5 configurations.
        """
        if training_data is None:
            training_data = self.train_data
        if val_data is None:
            val_data = self.val_data

        train_loader = DataLoader(training_data, batch_size=16, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=16)
        device = self.device

        grid = [
            {"hidden_channels": hc, "num_layers": nl, "num_heads": nh, "lr": lr}
            for hc in [32, 64, 128]
            for nl in [2, 3]
            for nh in [2, 4]
            for lr in [1e-3, 5e-4]
        ]

        results = []  # (best_val_mae, cfg, val_history)
        print(f"Sweeping {len(grid)} configurations (50 epochs each)…")

        for cfg in grid:
            model = HGT.HGT(
                hidden_channels=cfg["hidden_channels"],
                out_channels=1,
                num_layers=cfg["num_layers"],
                num_heads=cfg["num_heads"],
                data=training_data[0],
                viewpoint=self.viewpoint_object,
            ).to(device)

            train_criterion = torch.nn.HuberLoss(delta=1.0)

            with torch.no_grad():
                batch = next(iter(train_loader))
                model(batch.x_dict, batch.edge_index_dict)

            optimizer = torch.optim.Adam(model.parameters(), lr=cfg["lr"], weight_decay=1e-5)
            scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=50, eta_min=1e-5)

            best_val = float("inf")
            no_improve = 0
            val_history = []
            for epoch in range(1, 51):
                self.het_train(model, train_loader, optimizer, train_criterion, device)
                val_mae = self.het_loss_test(val_loader, model, None, device)
                scheduler.step()
                val_history.append(val_mae)
                if val_mae < best_val:
                    best_val = val_mae
                    no_improve = 0
                else:
                    no_improve += 1
                if no_improve >= 10:
                    break

            results.append((best_val, cfg, val_history))
            print(f"  hc={cfg['hidden_channels']:3d} layers={cfg['num_layers']} "
                  f"heads={cfg['num_heads']} lr={cfg['lr']:.0e}  →  val MAE={best_val:.4f}")

        results.sort(key=lambda t: t[0])
        top5 = results[:5]

        print("\n--- Sweep results (best first) ---")
        for val_mae, cfg, _ in top5:
            print(f"  val MAE={val_mae:.4f}  |  {cfg}")
        print(f"\nBest config: {top5[0][1]}  (val MAE={top5[0][0]:.4f})")

        self._plot_sweep_results(top5)
        return top5[0][1]

    def _plot_sweep_results(self, top5):
        """Save a two-panel figure: learning curves (left) and bar chart (right)."""
        fig, (ax_curves, ax_bar) = plt.subplots(1, 2, figsize=(14, 5))

        colors = plt.cm.tab10.colors

        for rank, (best_val, cfg, history) in enumerate(top5):
            label = (f"hc={cfg['hidden_channels']} L={cfg['num_layers']} "
                     f"H={cfg['num_heads']} lr={cfg['lr']:.0e}")
            ax_curves.plot(range(1, len(history) + 1), history,
                           color=colors[rank], label=label, linewidth=1.8)
            # Mark the best epoch
            best_epoch = history.index(min(history)) + 1
            ax_curves.scatter(best_epoch, min(history), color=colors[rank],
                              s=60, zorder=5)

        ax_curves.set_xlabel("Epoch")
        ax_curves.set_ylabel("Val MAE (normalized)")
        ax_curves.set_title("Learning curves — top 5 configs")
        ax_curves.legend(fontsize=8, loc="upper right")
        ax_curves.grid(True, alpha=0.3)

        labels = [
            f"hc={cfg['hidden_channels']}\nL={cfg['num_layers']} H={cfg['num_heads']}\nlr={cfg['lr']:.0e}"
            for _, cfg, _ in top5
        ]
        best_vals = [v for v, _, _ in top5]
        bars = ax_bar.barh(range(len(top5)), best_vals, color=colors[:len(top5)])
        ax_bar.set_yticks(range(len(top5)))
        ax_bar.set_yticklabels(labels, fontsize=8)
        ax_bar.invert_yaxis()  # best config at top
        ax_bar.set_xlabel("Best val MAE (normalized)")
        ax_bar.set_title("Best val MAE — top 5 configs")
        for bar, val in zip(bars, best_vals):
            ax_bar.text(bar.get_width() + 0.005, bar.get_y() + bar.get_height() / 2,
                        f"{val:.4f}", va="center", fontsize=8)
        ax_bar.grid(True, axis="x", alpha=0.3)

        plt.tight_layout()

        save_path = f"{self.path_dict['explainer_path']}sweep_results.png"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"Sweep plot saved to {save_path}")

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
        model = HGT.HGT(hidden_channels=64, num_layers=3, num_heads=2,
                        out_channels=1, data=data, viewpoint=viewpoint_object)
        device = torch.device("cpu")
        model = model.to(device)

        # Huber loss for training (robust to outliers); MAE reported via het_loss_test
        train_criterion = torch.nn.HuberLoss(delta=1.0)

        with torch.no_grad():
            batch = next(iter(train_loader))
            model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-5)

        max_epochs = 200
        early_stop_patience = 20
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer, T_max=max_epochs, eta_min=1e-5
        )

        best_val_mae = float("inf")
        best_state = None
        epochs_without_improvement = 0

        pbar = tqdm(range(1, max_epochs + 1))

        for epoch in pbar:
            train_loss = self.het_train(model, train_loader, optimizer, train_criterion, device)
            val_mae = self.het_loss_test(val_loader, model, None, device)
            scheduler.step()

            if val_mae < best_val_mae:
                print("New Best!")
                best_val_mae = val_mae
                best_state = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1

            if epoch % 10 == 0 or epoch == 1:
                current_lr = optimizer.param_groups[0]["lr"]
                mae_hours = val_mae * self.target_std.item() / 3600
                print(
                    f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
                    f"Val MAE: {val_mae:.4f} ({mae_hours:.1f}h) | LR: {current_lr:.2e}"
                )

            if epochs_without_improvement >= early_stop_patience:
                print(f"\nEarly stopping at epoch {epoch} "
                      f"(no val improvement for {early_stop_patience} epochs)")
                break
        pbar.close()

        if best_state is not None:
            model.load_state_dict(best_state)

        test_mae, test_rmse, test_r2 = self.het_full_metrics(test_loader, model, device)
        test_mae_hours = test_mae * self.target_std.item() / 3600
        test_rmse_hours = test_rmse * self.target_std.item() / 3600
        print(f'Test MAE:  {test_mae:.4f} std  ({test_mae_hours:.1f} hours)')
        print(f'Test RMSE: {test_rmse:.4f} std  ({test_rmse_hours:.1f} hours)')
        print(f'Test R²:   {test_r2:.4f}')

        # Save best model + architecture config sidecar
        torch.save(model.state_dict(), self.model_path)
        import json
        arch_cfg = {"hidden_channels": 64, "num_layers": 3, "num_heads": 4}
        with open(self.model_path.replace(".pth", "_arch.json"), "w") as f:
            json.dump(arch_cfg, f)

    def _hom_reg_train_loop(self, model, model_name, train_data, val_data, test_data):
        """Shared training loop for both homogeneous baselines."""
        import json

        device = self.device
        train_hom = self._build_hom_data(train_data)
        val_hom = self._build_hom_data(val_data)
        test_hom = self._build_hom_data(test_data)

        train_loader = DataLoader(train_hom, batch_size=16, shuffle=True)
        val_loader = DataLoader(val_hom, batch_size=16)
        test_loader = DataLoader(test_hom, batch_size=16)

        model = model.to(device)
        train_criterion = torch.nn.HuberLoss(delta=1.0)

        optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)
        max_epochs = 200
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer, T_max=max_epochs, eta_min=1e-5
        )

        best_val_mae = float("inf")
        best_state = None
        epochs_without_improvement = 0

        pbar = tqdm(range(1, max_epochs + 1))
        for epoch in pbar:
            train_loss = self._hom_train(model, train_loader, optimizer, train_criterion, device)
            val_mae = self._hom_loss_test(val_loader, model, device)
            scheduler.step()

            if val_mae < best_val_mae:
                best_val_mae = val_mae
                best_state = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
                print("New Best!")
            else:
                epochs_without_improvement += 1

            if epoch % 10 == 0 or epoch == 1:
                mae_hours = val_mae * self.target_std.item() / 3600
                print(f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
                      f"Val MAE: {val_mae:.4f} ({mae_hours:.1f}h)")

            if epochs_without_improvement >= 20:
                print(f"\nEarly stopping at epoch {epoch}")
                break
        pbar.close()

        if best_state is not None:
            model.load_state_dict(best_state)

        test_mae, test_rmse, test_r2 = self._hom_full_metrics(test_loader, model, device)
        test_mae_hours = test_mae * self.target_std.item() / 3600
        test_rmse_hours = test_rmse * self.target_std.item() / 3600
        print(f'Test MAE:  {test_mae:.4f} std  ({test_mae_hours:.1f} hours)')
        print(f'Test RMSE: {test_rmse:.4f} std  ({test_rmse_hours:.1f} hours)')
        print(f'Test R²:   {test_r2:.4f}')

        # Save checkpoint + arch sidecar
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        kpi = f"TimeFrom_{self.viewpoint_object}_to_{kpi_event}"
        os.makedirs(f"{model_path}/Homo", exist_ok=True)
        save_path = f"{model_path}/Homo/{model_name}_{kpi}.pth"
        torch.save(model.state_dict(), save_path)
        arch = {"in_channels": self.hom_in_channels, "hidden_channels": 64, "num_layers": 3}
        with open(save_path.replace(".pth", "_arch.json"), "w") as f:
            json.dump(arch, f)
        print(f"Saved to {save_path}")
        return save_path

    def Hom_GCN_Modelling(self):
        """Train REG_GNN (GCN-based homogeneous baseline) on converted sg data."""
        print("\n=== Training GCN Baseline ===")
        model = REG_GNN.REG_GNN(in_channels=self.hom_in_channels, hidden_channels=64, num_layers=3)
        self._hom_reg_train_loop(model, "GCN", self.train_data, self.val_data, self.test_data)

    def Hom_GAT_Modelling(self):
        """Train REG_GAT (GAT-based homogeneous baseline) on converted sg data."""
        print("\n=== Training GAT Baseline ===")
        model = REG_GAT.REG_GAT(in_channels=self.hom_in_channels, hidden_channels=64, num_layers=3)
        self._hom_reg_train_loop(model, "GAT", self.train_data, self.val_data, self.test_data)

    def compare_models(self, save_path=None):
        """Load all three trained models, evaluate on the test set, and save a comparison chart."""
        import json

        device = self.device
        kpi_event = self.path_dict['kpi_event']
        kpi = f"TimeFrom_{self.viewpoint_object}_to_{kpi_event}"
        model_path = self.path_dict['model_path']

        test_hom = self._build_hom_data(self.test_data)
        test_hom_loader = DataLoader(test_hom, batch_size=16)
        test_het_loader = DataLoader(self.test_data, batch_size=16)

        std_h = self.target_std.item() / 3600  # conversion factor to hours

        rows = []  # list of (name, MAE_h, RMSE_h, R²)

        # --- HGT ---
        hgt_path = self.model_path
        arch_path = hgt_path.replace(".pth", "_arch.json")
        if os.path.exists(hgt_path):
            arch = {"hidden_channels": 64, "num_layers": 3, "num_heads": 4}
            if os.path.exists(arch_path):
                with open(arch_path) as f:
                    arch = json.load(f)
            from model_classes import HGT
            hgt = HGT.HGT(hidden_channels=arch["hidden_channels"], out_channels=1,
                           num_layers=arch["num_layers"], num_heads=arch["num_heads"],
                           data=self.train_data[0], viewpoint=self.viewpoint_object).to(device)
            hgt.load_state_dict(torch.load(hgt_path, weights_only=False))
            mae, rmse, r2 = self.het_full_metrics(test_het_loader, hgt, device)
            rows.append(("HGT (het)", mae * std_h, rmse * std_h, r2))
        else:
            print(f"Warning: HGT checkpoint not found at {hgt_path} — skipping.")

        # --- GCN ---
        gcn_path = f"{model_path}/Homo/GCN_{kpi}.pth"
        gcn_arch_path = gcn_path.replace(".pth", "_arch.json")
        if os.path.exists(gcn_path):
            arch = {"in_channels": self.hom_in_channels, "hidden_channels": 64, "num_layers": 3}
            if os.path.exists(gcn_arch_path):
                with open(gcn_arch_path) as f:
                    arch = json.load(f)
            gcn = REG_GNN.REG_GNN(in_channels=arch["in_channels"],
                                   hidden_channels=arch["hidden_channels"],
                                   num_layers=arch["num_layers"]).to(device)
            gcn.load_state_dict(torch.load(gcn_path, weights_only=False))
            mae, rmse, r2 = self._hom_full_metrics(test_hom_loader, gcn, device)
            rows.append(("GCN (hom)", mae * std_h, rmse * std_h, r2))
        else:
            print(f"Warning: GCN checkpoint not found at {gcn_path} — skipping.")

        # --- GAT ---
        gat_path = f"{model_path}/Homo/GAT_{kpi}.pth"
        gat_arch_path = gat_path.replace(".pth", "_arch.json")
        if os.path.exists(gat_path):
            arch = {"in_channels": self.hom_in_channels, "hidden_channels": 64, "num_layers": 3}
            if os.path.exists(gat_arch_path):
                with open(gat_arch_path) as f:
                    arch = json.load(f)
            gat = REG_GAT.REG_GAT(in_channels=arch["in_channels"],
                                   hidden_channels=arch["hidden_channels"],
                                   num_layers=arch["num_layers"]).to(device)
            gat.load_state_dict(torch.load(gat_path, weights_only=False))
            mae, rmse, r2 = self._hom_full_metrics(test_hom_loader, gat, device)
            rows.append(("GAT (hom)", mae * std_h, rmse * std_h, r2))
        else:
            print(f"Warning: GAT checkpoint not found at {gat_path} — skipping.")

        if not rows:
            print("No trained models found. Train at least one model before comparing.")
            return

        # Print table
        print(f"\n{'Model':<18} {'MAE (h)':>10} {'RMSE (h)':>10} {'R²':>8}")
        print("─" * 50)
        for name, mae_h, rmse_h, r2 in rows:
            print(f"{name:<18} {mae_h:>10.1f} {rmse_h:>10.1f} {r2:>8.4f}")

        # Bar chart
        if save_path is None:
            save_path = f"{self.path_dict['explainer_path']}model_comparison.png"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        names = [r[0] for r in rows]
        maes = [r[1] for r in rows]
        rmses = [r[2] for r in rows]
        r2s = [r[3] for r in rows]

        fig, axes = plt.subplots(1, 3, figsize=(13, 4))
        colors = ["#4C72B0", "#DD8452", "#55A868"][:len(rows)]
        for ax, vals, title, ylabel in zip(
            axes,
            [maes, rmses, r2s],
            ["Test MAE (hours)", "Test RMSE (hours)", "Test R²"],
            ["Hours", "Hours", "R²"],
        ):
            bars = ax.bar(names, vals, color=colors)
            ax.set_title(title)
            ax.set_ylabel(ylabel)
            for bar, val in zip(bars, vals):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(vals) * 0.01,
                        f"{val:.2f}", ha="center", va="bottom", fontsize=9)
            ax.grid(True, axis="y", alpha=0.3)

        plt.suptitle("Model comparison — heterogeneous HGT vs homogeneous baselines", fontsize=11)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"\nComparison chart saved to {save_path}")

    def BinaryModelling(self, training_data, val_data, test_data):
        viewpoint_object = self.viewpoint_object

        # Create loaders
        train_loader = DataLoader(training_data, batch_size=16, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=16)
        test_loader = DataLoader(test_data, batch_size=16)

        # # Choose criterion
        # criterion = F.cross_entropy
        #
        # # Materialize HGTConv's lazy linear layers with one real forward pass
        # # before constructing the optimizer (same reason as the regression example).
        # with torch.no_grad():
        #     batch = next(iter(train_loader))
        #     self.model(batch.x_dict, batch.edge_index_dict)
        #
        # optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)#, weight_decay=1e-5)
        #
        # # Run training loop
        # best_val = 0.0
        # pbar = tqdm(range(1, 51))
        #
        # for epoch in pbar:
        #     train_loss = self.class_train(train_loader, optimizer, criterion)
        #     val_acc, val_f1 = self.class_eval(val_loader)
        #
        #     print(f'Epoch: {epoch:03d}, Loss: {train_loss:.4f}, Val ACC: {val_acc:.4f} | Val F1: {val_f1:.4f}')
        #     if val_f1 > best_val:
        #         best_val = val_acc
        #         print("New best!")
        #         torch.save(self.model.state_dict(), self.model_path)

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

    def class_evaluate_explanation(self, batch, node_importances, edge_importances,
                                   node_top_k=10, edge_top_k=15, verbose=True):
        """
        Quantify how good the counterfactual explanation subgraph actually is --
        the same fidelity / characterization / sparsity ideas used earlier for
        the homogeneous regression model, adapted here to HARD node/edge
        inclusion (this explanation is a discrete top-k selection, not a
        continuous learned mask) and to classification confidence instead of a
        continuous regression value.

          - Fidelity+ (higher = better): REMOVE the explanation (zero its
            nodes' features, drop its edges) and keep everything else. If the
            explanation correctly identified what the model relies on,
            confidence in the original predicted class should drop a lot.
          - Fidelity- (closer to 0 = better): KEEP ONLY the explanation, zero
            or drop everything else. If the explanation is self-sufficient,
            confidence should stay close to the original (in either direction
            -- a big swing either way means the explanation alone doesn't
            reproduce the original decision).
          - Characterization score: fidelity+ / (fidelity+ + |fidelity-|),
            bounded in [0, 1]. Same combined-score idea as the regression
            example, adapted since these are raw confidence deltas rather than
            the bounded probabilities PyG's built-in formula assumes.
          - Node/edge sparsity: what fraction of the FULL sampled neighborhood
            was excluded from the explanation -- i.e. how compact it is. A
            good explanation is both faithful (high fidelity+, low fidelity-)
            AND sparse; flagging the entire neighborhood as "important" would
            trivially nail fidelity but tell you nothing.
        """
        baseline_proba = self._predict_proba(batch)
        predicted_class = baseline_proba.argmax().item()
        baseline_confidence = baseline_proba[predicted_class].item()

        # Explanation membership, grouped by type (same top-k selection used to
        # build/draw the explanation subgraph above).
        explanation_nodes_by_type = {}
        for nt, i, _drop, _flips in node_importances[:node_top_k]:
            explanation_nodes_by_type.setdefault(nt, set()).add(i)
        explanation_nodes_by_type.setdefault(self.viewpoint_object, set()).add(0)  # seed always included

        explanation_edges_by_type = {}
        for et, e, _drop, _flips in edge_importances[:edge_top_k]:
            explanation_edges_by_type.setdefault(et, set()).add(e)

        # --- Fidelity+: remove the explanation, keep everything else ---
        complement = batch.clone()
        for nt, idx_set in explanation_nodes_by_type.items():
            for i in idx_set:
                complement[nt].x[i] = 0.0
        for et, pos_set in explanation_edges_by_type.items():
            edge_index = complement[et].edge_index
            num_edges = edge_index.size(1)
            keep = torch.tensor(
                [pos not in pos_set for pos in range(num_edges)],
                dtype=torch.bool, device=self.device,
            )
            complement[et].edge_index = edge_index[:, keep]
        proba_complement = self._predict_proba(complement)
        fidelity_plus = baseline_confidence - proba_complement[predicted_class].item()

        # --- Fidelity-: keep ONLY the explanation, zero/drop everything else ---
        subgraph = batch.clone()
        for nt in subgraph.node_types:
            keep_idx = explanation_nodes_by_type.get(nt, set())
            n = subgraph[nt].x.size(0)
            for i in range(n):
                if i not in keep_idx:
                    subgraph[nt].x[i] = 0.0
        for et in subgraph.edge_types:
            edge_index = subgraph[et].edge_index
            num_edges = edge_index.size(1)
            keep_pos = explanation_edges_by_type.get(et, set())
            keep = torch.tensor(
                [pos in keep_pos for pos in range(num_edges)],
                dtype=torch.bool, device=self.device,
            )
            subgraph[et].edge_index = edge_index[:, keep]
        proba_subgraph = self._predict_proba(subgraph)
        fidelity_minus = baseline_confidence - proba_subgraph[predicted_class].item()

        denom = fidelity_plus + abs(fidelity_minus)
        characterization_score = fidelity_plus / denom if denom > 1e-8 else 0.0

        # --- Sparsity: share of the full sampled neighborhood excluded ---
        total_nodes = sum(batch[nt].x.size(0) for nt in batch.node_types)
        explanation_node_count = sum(len(s) for s in explanation_nodes_by_type.values())
        node_sparsity = 1 - (explanation_node_count / total_nodes)

        total_edges = sum(batch[et].edge_index.size(1) for et in batch.edge_types)
        explanation_edge_count = sum(len(s) for s in explanation_edges_by_type.values())
        edge_sparsity = 1 - (explanation_edge_count / total_edges) if total_edges > 0 else float("nan")

        metrics = {
            "fidelity_plus": fidelity_plus,
            "fidelity_minus": fidelity_minus,
            "characterization_score": characterization_score,
            "node_sparsity": node_sparsity,
            "edge_sparsity": edge_sparsity,
        }

        if verbose:
            print("\n--- Explanation subgraph quality metrics ---")
            print(f"  Fidelity+        : {fidelity_plus:+.4f}  "
                  f"(higher is better -- removing the explanation should hurt confidence)")
            print(f"  Fidelity-        : {fidelity_minus:+.4f}  "
                  f"(closer to 0 is better -- the explanation alone should reproduce the prediction)")
            print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
            print(f"  Node sparsity    : {node_sparsity:.2%}  (share of sampled nodes excluded from the explanation)")
            print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of sampled edges excluded from the explanation)")

        return metrics

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

        self.class_evaluate_explanation(explain_subgraph, node_importances, edge_importances,
                                   node_top_k=10, edge_top_k=15, verbose=True)

    """
        Regression explanations
    """
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

    def reg_explanation_subgraph(self, graph, seed_paper_idx, node_importances,
                                 edge_importances,node_top_k=10):
        """
        Turn the counterfactual node/edge importance scores into an actual
        NetworkX subgraph.
        """
        import networkx as nx

        seed_key = (self.viewpoint_object, seed_paper_idx)
        G = nx.MultiDiGraph()
        G.add_node(seed_key, node_type=self.viewpoint_object, importance=1.0,
                   is_seed=True, large_shift=False, is_connector=False)

        included = {seed_key}
        for nt, i, shift, large in node_importances[:node_top_k]:
            key = (nt, i)
            G.add_node(key, node_type=nt, importance=shift, is_seed=False,
                       large_shift=large, is_connector=False)
            included.add(key)

        # Flatten every real edge in this small graph for path-finding /
        # induced-subgraph lookups below.
        all_edges = []  # (edge_type, src_key, dst_key)
        for edge_type in graph.edge_types:
            src_type, _, dst_type = edge_type
            edge_index = graph[edge_type].edge_index
            for e in range(edge_index.size(1)):
                src, dst = edge_index[:, e].tolist()
                all_edges.append((edge_type, (src_type, src), (dst_type, dst)))

        edge_importance_lookup = {}
        for edge_type, e, shift, large in edge_importances:
            edge_index = graph[edge_type].edge_index
            src, dst = edge_index[:, e].tolist()
            edge_importance_lookup[(edge_type, src, dst)] = (shift, large)

        def add_real_edge(edge_type, src_key, dst_key):
            shift, large = edge_importance_lookup.get(
                (edge_type, src_key[1], dst_key[1]), (0.0, False)
            )
            G.add_edge(src_key, dst_key, edge_type=edge_type[1], importance=shift, large_shift=large)

        # --- Pass 1: the real induced subgraph on the selected nodes ---
        for edge_type, src_key, dst_key in all_edges:
            if src_key in included and dst_key in included:
                add_real_edge(edge_type, src_key, dst_key)

        # --- Pass 2: repair any node that's STILL isolated (the 2-hop
        #     case) by pulling in the shortest real path to the seed ---
        full_nx = nx.Graph()  # undirected, for path-finding only
        for edge_type, src_key, dst_key in all_edges:
            full_nx.add_edge(src_key, dst_key, edge_type=edge_type)

        isolated = [n for n in included if G.degree(n) == 0]
        for node_key in isolated:
            try:
                path = nx.shortest_path(full_nx, source=node_key, target=seed_key)
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                continue  # truly unreachable in this small graph -- leave isolated

            for n in path:
                if n not in G.nodes:
                    # Mark as a connector: structurally necessary to show
                    # the real path, but not independently ranked important.
                    G.add_node(n, node_type=n[0], importance=0.0, is_seed=False,
                               large_shift=False, is_connector=True)

            for a, b in zip(path[:-1], path[1:]):
                if G.has_edge(a, b) or G.has_edge(b, a):
                    continue
                edge_type = full_nx[a][b]["edge_type"]
                shift, large = edge_importance_lookup.get(
                    (edge_type, a[1], b[1]),
                    edge_importance_lookup.get((edge_type, b[1], a[1]), (0.0, False)),
                )
                G.add_edge(a, b, edge_type=edge_type[1], importance=shift, large_shift=large)

        return G

    def reg_visualize_explanation_subgraph(self, G, save_path="explanation_subgraph_regression.png"):
        """Draw the explanation subgraph: node color = type, node size =
        importance (value shift), red edges/outlines = "removing this
        shifts the prediction by more than one target standard deviation".
        Connector nodes (pulled in only to show a real 2-hop path, not
        independently ranked important) are drawn smaller and faded."""
        import matplotlib.pyplot as plt
        import networkx as nx

        palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
                   "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"]
        node_types = sorted({attrs["node_type"] for _, attrs in G.nodes(data=True)})
        type_colors = {nt: palette[i % len(palette)] for i, nt in enumerate(node_types)}

        # Now that the graph is properly connected (see build_explanation_
        # subgraph above), kamada_kawai tends to give a clearer layout for
        # small graphs than spring_layout; fall back if anything's amiss.
        try:
            pos = nx.kamada_kawai_layout(G)
        except Exception:
            pos = nx.spring_layout(G, seed=42, k=0.9)

        node_colors, node_sizes, edge_colors_outline, alphas = [], [], [], []
        for node, attrs in G.nodes(data=True):
            node_colors.append(type_colors.get(attrs["node_type"], "gray"))
            if attrs.get("is_seed"):
                node_sizes.append(900)
            elif attrs.get("is_connector"):
                node_sizes.append(150)  # smaller -- structural, not independently ranked
            else:
                node_sizes.append(250 + max(attrs.get("importance", 0), 0) * 1500)
            edge_colors_outline.append("red" if attrs.get("large_shift") else "black")
            alphas.append(0.4 if attrs.get("is_connector") else 0.9)

        edge_colors, edge_widths = [], []
        for _, _, attrs in G.edges(data=True):
            edge_colors.append("red" if attrs.get("large_shift") else "gray")
            edge_widths.append(1 + max(attrs.get("importance", 0), 0) * 8)

        plt.figure(figsize=(10, 8))
        nx.draw_networkx_nodes(
            G, pos, node_color=node_colors, node_size=node_sizes,
            edgecolors=edge_colors_outline, linewidths=1.5, alpha=alphas,
        )
        nx.draw_networkx_edges(
            G, pos, edge_color=edge_colors, width=edge_widths,
            arrows=True, connectionstyle="arc3,rad=0.1", alpha=0.7,
        )
        labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes if not G.nodes[node].get("is_connector")}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=7)

        legend_handles = [
            plt.Line2D([0], [0], marker="o", color="w", label=nt,
                       markerfacecolor=color, markersize=10)
            for nt, color in type_colors.items()
        ]
        legend_handles.append(plt.Line2D([0], [0], color="red", lw=2, label="large shift (>1 std)"))
        legend_handles.append(plt.Line2D([0], [0], marker="o", color="w", label="connector (faded)",
                                         markerfacecolor="gray", alpha=0.4, markersize=8))
        plt.legend(handles=legend_handles, loc="best", fontsize=8)

        plt.title("Counterfactual Explanation Subgraph (Regression)")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"Saved explanation subgraph visualization to {save_path}")

    def evaluate_explanation_quality(self, graph, paper_idx, node_importances, edge_importances,
                                     node_top_k=10, edge_top_k=15, verbose=True):
        """
        Quantify how good the counterfactual explanation subgraph actually
        is for a REGRESSION prediction -- the same fidelity / characterization
        / sparsity ideas used for the DBLP classification model, adapted to
        value shifts instead of confidence drops.

          - Fidelity+ (higher = better): REMOVE the explanation (zero its
            nodes, drop its edges), keep everything else. The prediction
            should shift a lot if the explanation correctly identified
            what the model relies on.
          - Fidelity- (closer to 0 = better): KEEP ONLY the explanation,
            zero/drop everything else. The prediction should stay close to
            the original if the explanation is self-sufficient.
          - Characterization score: fidelity+ / (fidelity+ + fidelity-),
            bounded in [0, 1].
          - Node/edge sparsity: share of the FULL small graph excluded
            from the explanation -- how compact it is.
        """
        baseline_value = self._predict_value_for_graph(graph, paper_idx)

        explanation_nodes_by_type = {}
        for nt, i, _shift, _large in node_importances[:node_top_k]:
            explanation_nodes_by_type.setdefault(nt, set()).add(i)
        explanation_nodes_by_type.setdefault(self.viewpoint_object, set()).add(paper_idx)

        explanation_edges_by_type = {}
        for et, e, _shift, _large in edge_importances[:edge_top_k]:
            explanation_edges_by_type.setdefault(et, set()).add(e)

        # --- Fidelity+: remove the explanation, keep everything else ---
        complement = graph.clone()
        for nt, idx_set in explanation_nodes_by_type.items():
            for i in idx_set:
                complement[nt].x[i] = 0.0
        for et, pos_set in explanation_edges_by_type.items():
            edge_index = complement[et].edge_index
            num_edges = edge_index.size(1)
            keep = torch.tensor(
                [pos not in pos_set for pos in range(num_edges)],
                dtype=torch.bool, device=self.device,
            )
            complement[et].edge_index = edge_index[:, keep]
        pred_complement = self._predict_value_for_graph(graph, paper_idx, perturbed_graph=complement)
        fidelity_plus = abs(baseline_value - pred_complement)

        # --- Fidelity-: keep ONLY the explanation ---
        subgraph = graph.clone()
        for nt in subgraph.node_types:
            keep_idx = explanation_nodes_by_type.get(nt, set())
            n = subgraph[nt].x.size(0)
            for i in range(n):
                if i not in keep_idx:
                    subgraph[nt].x[i] = 0.0
        for et in subgraph.edge_types:
            edge_index = subgraph[et].edge_index
            num_edges = edge_index.size(1)
            keep_pos = explanation_edges_by_type.get(et, set())
            keep = torch.tensor(
                [pos in keep_pos for pos in range(num_edges)],
                dtype=torch.bool, device=self.device,
            )
            subgraph[et].edge_index = edge_index[:, keep]
        pred_subgraph = self._predict_value_for_graph(graph, paper_idx, perturbed_graph=subgraph)
        fidelity_minus = abs(baseline_value - pred_subgraph)

        denom = fidelity_plus + fidelity_minus
        characterization_score = fidelity_plus / denom if denom > 1e-8 else 0.0

        total_nodes = sum(graph[nt].x.size(0) for nt in graph.node_types)
        explanation_node_count = sum(len(s) for s in explanation_nodes_by_type.values())
        node_sparsity = 1 - (explanation_node_count / total_nodes)

        total_edges = sum(graph[et].edge_index.size(1) for et in graph.edge_types)
        explanation_edge_count = sum(len(s) for s in explanation_edges_by_type.values())
        edge_sparsity = 1 - (explanation_edge_count / total_edges) if total_edges > 0 else float("nan")

        metrics = {
            "fidelity_plus": fidelity_plus,
            "fidelity_minus": fidelity_minus,
            "characterization_score": characterization_score,
            "node_sparsity": node_sparsity,
            "edge_sparsity": edge_sparsity,
        }

        if verbose:
            print("\n--- Explanation subgraph quality metrics ---")
            print(f"  Fidelity+        : {fidelity_plus:.4f}  "
                  f"(higher is better -- removing the explanation should shift the prediction)")
            print(f"  Fidelity-        : {fidelity_minus:.4f}  "
                  f"(closer to 0 is better -- the explanation alone should reproduce the prediction)")
            print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
            print(f"  Node sparsity    : {node_sparsity:.2%}  (share of the graph excluded from the explanation)")
            print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of edges excluded from the explanation)")

        return metrics

    def reg_explanation(self, explain_subgraph, object_idx, graph_id, top_k):
        baseline_value = self._predict_value_for_graph(explain_subgraph, object_idx)

        # --- leave-one-out over every NODE in this graph ---
        node_importances = []  # (node_type, idx, value_shift, large_shift)
        for node_type in explain_subgraph.node_types:
            n = explain_subgraph[node_type].x.size(0)
            for idx in range(n):
                if node_type == self.viewpoint_object and idx == object_idx:
                    continue
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

        return (node_importances, edge_importances, seed_feature_importances,
                top_node_feature_importances, baseline_value)

    # ------------------------------------------------------------------
    # Explainability — visualizations and entry points
    # ------------------------------------------------------------------

    def plot_feature_importances(self, node_type, feature_importances, save_path):
        """Horizontal bar chart of per-feature value shifts for one node."""
        names = self.feature_names.get(node_type, [])
        feats, shifts, larges = [], [], []
        for f, shift, large in feature_importances:
            label = names[f] if f < len(names) else f"feat_{f}"
            feats.append(label)
            shifts.append(shift / 3600)
            larges.append(large)

        if not feats:
            return

        fig, ax = plt.subplots(figsize=(7, max(3, len(feats) * 0.45)))
        colors = ["#e74c3c" if l else "#7f8c8d" for l in larges]
        bars = ax.barh(range(len(feats)), shifts, color=colors)
        ax.set_yticks(range(len(feats)))
        ax.set_yticklabels(feats, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel("Value shift if removed (hours)")
        ax.set_title(f"Feature importance — {node_type} node")
        for bar, val in zip(bars, shifts):
            ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                    f"{val:.2f}h", va="center", fontsize=8)
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color="#e74c3c", label=">1 std shift"),
                            Patch(color="#7f8c8d", label="≤1 std shift")],
                  fontsize=8, loc="lower right")
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def plot_node_type_summary(self, node_importances, save_path):
        """Bar chart of total influence per node type across the whole trace."""
        from collections import defaultdict
        type_shift = defaultdict(float)
        type_count = defaultdict(int)
        for node_type, idx, shift, _ in node_importances:
            type_shift[node_type] += shift / 3600
            type_count[node_type] += 1

        types = sorted(type_shift, key=lambda t: type_shift[t], reverse=True)
        total_shifts = [type_shift[t] for t in types]
        counts = [type_count[t] for t in types]

        fig, ax1 = plt.subplots(figsize=(8, 4))
        x = range(len(types))
        bars = ax1.bar(x, total_shifts, color="#4C72B0", alpha=0.8, label="Total shift (hours)")
        ax1.set_xticks(x)
        ax1.set_xticklabels(types, rotation=30, ha="right", fontsize=9)
        ax1.set_ylabel("Cumulative value shift if type removed (hours)")
        ax1.set_title("Node type importance summary")

        ax2 = ax1.twinx()
        ax2.plot(x, counts, "o--", color="#e74c3c", label="Node count")
        ax2.set_ylabel("Number of nodes of this type", color="#e74c3c")
        ax2.tick_params(axis="y", labelcolor="#e74c3c")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8, loc="upper right")
        ax1.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def explain_trace(self, order_id, top_k=5, save_dir=None):
        """
        Full explanation for a single order trace (last-event snapshot).
        Loads the model, finds the trace, runs LOO attribution, prints a
        human-readable summary, saves visualizations, and returns all results.
        """
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}")
        os.makedirs(save_dir, exist_ok=True)

        # Load model — use sidecar arch config if present, else fall back to self.model
        import json
        arch_cfg_path = self.model_path.replace(".pth", "_arch.json")
        if os.path.exists(arch_cfg_path):
            with open(arch_cfg_path) as f:
                arch = json.load(f)
            self.model = HGT.HGT(
                hidden_channels=arch["hidden_channels"],
                out_channels=1,
                num_layers=arch["num_layers"],
                num_heads=arch["num_heads"],
                data=self.test_data[0],
                viewpoint=self.viewpoint_object,
            ).to(self.device)
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        # Find the last-event snapshot for this order in test data
        explain_subgraph = None
        for g in self.test_data:
            if (g[self.viewpoint_object]['last_event'].item()
                    and g[self.viewpoint_object]['id'].item() == order_id):
                explain_subgraph = g
                break
        if explain_subgraph is None:
            raise ValueError(f"Order ID {order_id} with last_event=True not found in test data.")

        (node_importances, edge_importances,
         seed_feats, top_neighbor_feats, baseline_value) = self.reg_explanation(
            explain_subgraph, 0, order_id, top_k
        )

        metrics = self.evaluate_explanation_quality(
            explain_subgraph, 0, node_importances, edge_importances,
            node_top_k=10, edge_top_k=15, verbose=False
        )

        # --- Feature importance charts ---
        self.plot_feature_importances(
            self.viewpoint_object, seed_feats,
            os.path.join(save_dir, f"feat_importance_{self.viewpoint_object}.png")
        )
        if node_importances:
            top_nt, top_ni, _, _ = node_importances[0]
            self.plot_feature_importances(
                top_nt, top_neighbor_feats,
                os.path.join(save_dir, f"feat_importance_{top_nt}.png")
            )

        # --- Node-type summary chart ---
        self.plot_node_type_summary(
            node_importances,
            os.path.join(save_dir, "node_type_summary.png")
        )

        # --- Explanation subgraph ---
        exp_graph = self.reg_explanation_subgraph(
            explain_subgraph, 0, node_importances, edge_importances, node_top_k=10
        )
        self.reg_visualize_explanation_subgraph(
            exp_graph, save_path=os.path.join(save_dir, "explanation_subgraph.png")
        )

        # --- Human-readable summary ---
        names = self.feature_names
        std_hours = self.target_std.item() / 3600
        print(f"\n{'='*60}")
        print(f"Explanation for {self.viewpoint_object} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours")
        print(f"  Graph size : " +
              ", ".join(f"{nt}={explain_subgraph[nt].num_nodes}" for nt in explain_subgraph.node_types))
        print(f"\nTop {top_k} nodes by influence (value shift if removed):")
        for rank, (nt, idx, shift, large) in enumerate(node_importances[:top_k], 1):
            feat_vals = ""
            if explain_subgraph[nt].x.size(0) > idx:
                node_names = names.get(nt, [])
                non_zero = [(node_names[f] if f < len(node_names) else f"feat_{f}",
                             explain_subgraph[nt].x[idx, f].item())
                            for f in range(explain_subgraph[nt].x.size(1))
                            if explain_subgraph[nt].x[idx, f].item() != 0][:2]
                if non_zero:
                    feat_vals = "  [" + ", ".join(f"{n}={v:.2f}" for n, v in non_zero) + "]"
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {nt}[{idx}]{feat_vals}: shift={shift/3600:+.2f}h{flag}")

        print(f"\nTop {top_k} edges by influence:")
        for rank, (et, e, shift, large) in enumerate(edge_importances[:top_k], 1):
            src, dst = explain_subgraph[et].edge_index[:, e].tolist()
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {et[0]}→{et[2]} ({src}→{dst}): shift={shift/3600:+.2f}h{flag}")

        print(f"\nTop {top_k} features on seed {self.viewpoint_object} node:")
        seed_names = names.get(self.viewpoint_object, [])
        for rank, (f, shift, large) in enumerate(seed_feats[:top_k], 1):
            fname = seed_names[f] if f < len(seed_names) else f"feat_{f}"
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {fname}: shift={shift/3600:+.2f}h{flag}")

        print(f"\nExplanation quality metrics:")
        print(f"  Fidelity+       : {metrics['fidelity_plus']:.4f}  (↑ better)")
        print(f"  Fidelity−       : {metrics['fidelity_minus']:.4f}  (↓ better)")
        print(f"  Characterization: {metrics['characterization_score']:.4f}  (↑ better, max 1.0)")
        print(f"  Node sparsity   : {metrics['node_sparsity']:.1%}")
        print(f"  Edge sparsity   : {metrics['edge_sparsity']:.1%}")
        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            "order_id": order_id,
            "predicted_hours": baseline_value / 3600,
            "node_importances": node_importances,
            "edge_importances": edge_importances,
            "seed_feature_importances": seed_feats,
            "top_neighbor_feature_importances": top_neighbor_feats,
            "metrics": metrics,
            "save_dir": save_dir,
        }

    def explain_aggregate(self, n_traces=50, top_k=5, save_dir=None):
        """
        Run LOO explanation on n_traces last-event graphs from the test set and
        aggregate: mean shift per node type, mean shift per feature per type,
        and mean/std of all explanation quality metrics. Saves charts and CSV.
        """
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        last_event_graphs = [g for g in self.test_data
                             if g[self.viewpoint_object]['last_event'].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running aggregate explanation on {len(sample)} traces…")

        from collections import defaultdict
        type_shifts = defaultdict(list)           # node_type -> [shift_hours, ...]
        feat_shifts = defaultdict(lambda: defaultdict(list))  # node_type -> feat_idx -> [shifts]
        all_metrics = []

        for g in sample:
            try:
                (node_imp, edge_imp, seed_feats, _, _) = self.reg_explanation(g, 0, None, top_k)
            except Exception:
                continue

            for nt, idx, shift, _ in node_imp:
                type_shifts[nt].append(shift / 3600)

            for f, shift, _ in seed_feats:
                feat_shifts[self.viewpoint_object][f].append(shift / 3600)

            m = self.evaluate_explanation_quality(g, 0, node_imp, edge_imp,
                                                   node_top_k=10, edge_top_k=15, verbose=False)
            all_metrics.append(m)

        import statistics

        # --- Aggregate node-type chart ---
        types = sorted(type_shifts, key=lambda t: -sum(type_shifts[t]) / max(len(type_shifts[t]), 1))
        mean_shifts = [sum(type_shifts[t]) / len(type_shifts[t]) for t in types]

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(types, mean_shifts, color="#4C72B0", alpha=0.85)
        ax.set_ylabel("Mean value shift if removed (hours)")
        ax.set_title(f"Aggregate node type importance (n={len(sample)} traces)")
        ax.set_xticklabels(types, rotation=30, ha="right", fontsize=9)
        ax.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, "aggregate_node_type_importance.png"), dpi=150)
        plt.close()

        # --- Aggregate feature importance per node type ---
        names = self.feature_names
        for nt, fdict in feat_shifts.items():
            feat_names_nt = names.get(nt, [])
            items = sorted(fdict.items(), key=lambda kv: -sum(kv[1]) / max(len(kv[1]), 1))
            feats = [feat_names_nt[f] if f < len(feat_names_nt) else f"feat_{f}" for f, _ in items]
            means = [sum(vs) / len(vs) for _, vs in items]
            if not feats:
                continue
            fig, ax = plt.subplots(figsize=(7, max(3, len(feats) * 0.45)))
            ax.barh(range(len(feats)), means, color="#DD8452", alpha=0.85)
            ax.set_yticks(range(len(feats)))
            ax.set_yticklabels(feats, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel("Mean value shift if removed (hours)")
            ax.set_title(f"Aggregate feature importance — {nt} (n={len(sample)})")
            ax.grid(True, axis="x", alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(save_dir, f"aggregate_feat_importance_{nt}.png"), dpi=150)
            plt.close()

        # --- Metrics CSV ---
        import csv
        metric_keys = ["fidelity_plus", "fidelity_minus", "characterization_score",
                       "node_sparsity", "edge_sparsity"]
        csv_path = os.path.join(save_dir, "aggregate_metrics.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["trace"] + metric_keys)
            writer.writeheader()
            for i, m in enumerate(all_metrics):
                writer.writerow({"trace": i, **{k: round(m[k], 6) for k in metric_keys}})
            # Summary row
            summary = {"trace": "mean"}
            for k in metric_keys:
                vals = [m[k] for m in all_metrics]
                summary[k] = round(sum(vals) / len(vals), 6) if vals else float("nan")
            writer.writerow(summary)
            summary_std = {"trace": "std"}
            for k in metric_keys:
                vals = [m[k] for m in all_metrics]
                summary_std[k] = round(statistics.stdev(vals), 6) if len(vals) > 1 else 0.0
            writer.writerow(summary_std)

        print(f"\nAggregate explanation quality (n={len(all_metrics)} traces):")
        for k in metric_keys:
            vals = [m[k] for m in all_metrics]
            mean_v = sum(vals) / len(vals) if vals else float("nan")
            std_v = statistics.stdev(vals) if len(vals) > 1 else 0.0
            print(f"  {k:25s}: {mean_v:.4f} ± {std_v:.4f}")
        print(f"\nAggregate outputs saved to: {save_dir}")

        return all_metrics

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
            self.reg_explanation(explain_subgraph, 0, object_idx, top_k)
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

    def Baseline(self):
        """Train both homogeneous baselines and print a comparison table against the HGT."""
        self.Hom_GCN_Modelling()
        self.Hom_GAT_Modelling()
        self.compare_models()