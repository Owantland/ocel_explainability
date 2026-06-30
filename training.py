import torch

import torch.nn.functional as F
from torch_geometric.loader import DataLoader

from model_classes import REG_GNN, HGT_CLASS, REG_GAT, HGT
from torchmetrics import F1Score, ConfusionMatrix, Accuracy

import sup_funcs as sf
import pandas as pd
from tqdm import tqdm
import os

import matplotlib.pyplot as plt
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

        train_loader = DataLoader(training_data, batch_size=64, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=64)
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

        # Plot only the top two values
        top2 = results[:2]
        self._plot_sweep_results(top2)
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
        # # PAckageDelivered
        # model = HGT.HGT(hidden_channels=64, num_layers=3, num_heads=4,
        #                 out_channels=1, data=data, viewpoint=viewpoint_object)
        # PayOrder
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
        arch_cfg = {"hidden_channels": 64, "num_layers": 3, "num_heads": 2}
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
