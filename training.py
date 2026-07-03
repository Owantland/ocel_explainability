import torch
import json
import random
import numpy as np

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
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

        # Load relevant datasets
        self.train_data = torch.load(f"{self.path_dict['pytorch_path']}/train_graphs_sg.pt", weights_only=False)
        self.val_data = torch.load(f"{self.path_dict['pytorch_path']}/val_graphs_sg.pt", weights_only=False)
        self.test_data = torch.load(f"{self.path_dict['pytorch_path']}/test_graphs_sg.pt", weights_only=False)

        kpi_type = self.path_dict['kpi_type']
        _DEFAULTS = {
            0: {'hidden_channels': 24, 'num_layers': 2, 'num_heads': 2, 'lr': 0.001, 'weight_decay': 1e-5},
            1: {'hidden_channels': 64, 'num_layers': 2, 'num_heads': 2, 'lr': 0.001, 'weight_decay': 1e-5},
        }
        if kpi_type == 0:  # Regression
            self.task_id = f"TimeFrom_{self.viewpoint_object}_to_{self.path_dict['kpi_event']}"
            self.params = self._load_params() or _DEFAULTS[0]
            self.model = self._build_model(self.params)

            # Standardize the output values.
            # Load from sidecar if it exists — survives graph regeneration without silent
            # de-normalisation. Falls back to computing from train data on first run.
            train_y_all = torch.cat([g[self.viewpoint_object].y for g in self.train_data])
            _model_dir = f"{self.path_dict['model_path']}/Hetero"
            _norm_path = f"{_model_dir}/{self.task_id}_norm.json"
            if os.path.exists(_norm_path):
                with open(_norm_path) as _f:
                    _saved = json.load(_f)
                target_mean = torch.tensor(_saved["target_mean"])
                target_std  = torch.tensor(_saved["target_std"])
            else:
                target_mean, target_std = train_y_all.mean(), train_y_all.std()
            for m in [self.train_data, self.val_data, self.test_data]:
                for g in m:
                    g[self.viewpoint_object].y = (g[self.viewpoint_object].y - target_mean) / target_std
            print(f"Mean (hours): {round(target_mean.item() / 3600)}, STD (hours): {round(target_std.item() / 3600)}")
            self.target_mean, self.target_std = target_mean.to(self.device), target_std.to(self.device)

        elif kpi_type == 1:
            self.task_id = f"Classifier_{self.path_dict['kpi_event']}"
            self.params = self._load_params() or _DEFAULTS[1]
            self.model = self._build_model(self.params)

        # Node feature standardization — covers continuous attributes and Events
        # (Events contains a mix of one-hot type flags and continuous temporal features;
        # z-normalising all dims is valid and the model adapts via its linear projections)
        continuous_node_types = (
            list((self.path_dict.get('attributes') or {}).keys()) +
            list((self.path_dict.get('time_attributes') or {}).keys()) +
            ['Events']
        )
        for node_type in continuous_node_types:
            x_train = [g[node_type].x for g in self.train_data if g[node_type].num_nodes > 0]
            if not x_train:
                continue
            x_cat = torch.cat(x_train, dim=0)
            feat_mean = x_cat.mean(dim=0)
            feat_std = x_cat.std(dim=0).clamp(min=1e-8)
            for split in [self.train_data, self.val_data, self.test_data]:
                for g in split:
                    if g[node_type].num_nodes > 0:
                        g[node_type].x = (g[node_type].x - feat_mean) / feat_std

        self.model = self.model.to(self.device)

        self.feature_names = {
            **{nt: names for nt, names in (self.path_dict.get('attributes') or {}).items()},
            **{nt: names for nt, names in (self.path_dict.get('time_attributes') or {}).items()},
        }

        # Extend feature names for enriched node types based on actual graph feature counts
        _temporal_names = ['elapsed_h', 'waiting_h', 'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos']
        _order_extra = ['n_items', 'total_weight', 'n_products']
        if self.train_data and self.train_data[0]['Events'].num_nodes > 0:
            n_ev = self.train_data[0]['Events'].x.shape[1]
            n_types = max(0, n_ev - len(_temporal_names))
            self.feature_names['Events'] = (
                [f'type_{i}' for i in range(n_types)] + _temporal_names
            )[:n_ev]
        if self.train_data and self.train_data[0][self.viewpoint_object].num_nodes > 0:
            vp = self.viewpoint_object
            n_vp = self.train_data[0][vp].x.shape[1]
            base_names = list((self.path_dict.get('attributes') or {}).get(vp, []))
            self.feature_names[vp] = (base_names + _order_extra)[:n_vp]

        # Define save path for the models
        model_path = self.path_dict['model_path']

        if not os.path.exists(f"{model_path}/Hetero"):
            os.makedirs(f"{model_path}/Hetero")
        self.model_path = f"{model_path}/Hetero/{self.task_id}.pth"

    def decode_epoch(self, epoch_val):
        timestamp = pd.Timestamp(epoch_val, unit='s')
        return timestamp

    def decode_time(self, total_secs):
        timestamp = pd.Timedelta(round(total_secs, 2), unit='s')
        return timestamp

    @property
    def _params_path(self):
        return f"{self.path_dict['model_path']}/Hetero/model_params.json"

    def _load_params(self):
        if os.path.exists(self._params_path):
            with open(self._params_path) as f:
                all_params = json.load(f)
            result = all_params.get(self.task_id)
            if result:
                return result
        arch_path = f"{self.path_dict['model_path']}/Hetero/{self.task_id}_arch.json"
        if os.path.exists(arch_path):
            with open(arch_path) as f:
                return json.load(f)
        return None

    def _save_params(self, params):
        all_params = {}
        if os.path.exists(self._params_path):
            with open(self._params_path) as f:
                all_params = json.load(f)
        all_params[self.task_id] = params
        with open(self._params_path, 'w') as f:
            json.dump(all_params, f, indent=2)

    def _build_model(self, params):
        kpi_type = self.path_dict['kpi_type']
        if kpi_type == 0:
            return HGT.HGT(hidden_channels=params['hidden_channels'], out_channels=1,
                           num_layers=params['num_layers'], num_heads=params['num_heads'],
                           data=self.train_data[0], viewpoint=self.viewpoint_object)
        elif kpi_type == 1:
            return HGT_CLASS.HGT_CLASS(hidden_channels=params['hidden_channels'], out_channels=2,
                                       num_heads=params['num_heads'], num_layers=params['num_layers'],
                                       data=self.train_data[0], viewpoint=self.viewpoint_object)

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
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
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

    def Het_Reg_Modelling(self, training_data, val_data, test_data):
        torch.manual_seed(42)
        random.seed(42)
        np.random.seed(42)

        batch_size = self.path_dict.get('batch_size', 16)
        train_loader = DataLoader(training_data, batch_size=batch_size, shuffle=True)
        val_loader   = DataLoader(val_data,      batch_size=batch_size, shuffle=False)
        test_loader  = DataLoader(test_data,     batch_size=batch_size, shuffle=False)

        model = self.model.to(self.device)
        criterion = torch.nn.L1Loss()

        with torch.no_grad():
            batch = next(iter(train_loader)).to(self.device)
            model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(model.parameters(),
                                     lr=self.params['lr'],
                                     weight_decay=self.params['weight_decay'])

        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", factor=0.5, patience=10
        )

        max_epochs = 200
        early_stop_patience = 20

        best_val_mae = float("inf")
        best_state = None
        epochs_without_improvement = 0
        log = []

        pbar = tqdm(range(1, max_epochs + 1))

        for epoch in pbar:
            train_loss = self.het_train(model, train_loader, optimizer, criterion, self.device)
            val_mae    = self.het_loss_test(val_loader, model, criterion, self.device)
            scheduler.step(val_mae)

            current_lr = optimizer.param_groups[0]["lr"]
            log.append({'epoch': epoch, 'train_loss': train_loss,
                        'val_mae': val_mae, 'lr': current_lr})

            if val_mae < best_val_mae:
                print("New Best!")
                best_val_mae = val_mae
                best_state = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1

            if epoch % 10 == 0 or epoch == 1:
                print(
                    f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
                    f"Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}"
                )

            if epochs_without_improvement >= early_stop_patience:
                print(f"\nEarly stopping at epoch {epoch} "
                      f"(no val improvement for {early_stop_patience} epochs)")
                break
        pbar.close()

        if best_state is not None:
            model.load_state_dict(best_state)
        test_loss = self.het_loss_test(test_loader, model, criterion, self.device)
        print(f'Final MAE: {test_loss}')

        torch.save(self.model.state_dict(), self.model_path)

        norm_path = self.model_path.replace(".pth", "_norm.json")
        with open(norm_path, "w") as f:
            json.dump({"target_mean": self.target_mean.item(),
                       "target_std":  self.target_std.item()}, f)

        log_path = self.model_path.replace(".pth", "_training_log.csv")
        pd.DataFrame(log).to_csv(log_path, index=False)

    def sweep(self, n_trials=30):
        """
        Performs a training sweep of multiple hyperparameters to find the optimal model settings
        :param n_trials: number of trials for sweep
        :return: Saves the best hyperparameters to a json file
        """
        import optuna
        torch.manual_seed(42)
        random.seed(42)
        np.random.seed(42)

        criterion = torch.nn.L1Loss()
        batch_size = self.path_dict.get('batch_size', 16)

        def objective(trial):
            hidden_channels = trial.suggest_categorical('hidden_channels', [8, 16, 24, 32, 48, 64, 128, 256])
            num_heads = trial.suggest_categorical('num_heads', [1, 2])
            if hidden_channels % num_heads != 0:
                raise optuna.TrialPruned()
            num_layers = trial.suggest_int('num_layers', 1, 2)
            lr = trial.suggest_categorical('lr', [1e-3, 1e-2])

            trial_params = {'hidden_channels': hidden_channels, 'num_layers': num_layers,
                            'num_heads': num_heads, 'lr': lr}
            model = self._build_model(trial_params).to(self.device)

            train_loader = DataLoader(self.train_data, batch_size=batch_size, shuffle=True)
            val_loader   = DataLoader(self.val_data,   batch_size=batch_size, shuffle=False)
            with torch.no_grad():
                batch = next(iter(train_loader)).to(self.device)
                model(batch.x_dict, batch.edge_index_dict)

            optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=0.001)
            scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer, mode="min", factor=0.5, patience=10
            )
            best_val, patience_count = float('inf'), 0

            for epoch in range(1, 31):
                self.het_train(model, train_loader, optimizer, criterion, self.device)
                val_mae = self.het_loss_test(val_loader, model, criterion, self.device)
                scheduler.step(val_mae)
                trial.report(val_mae, epoch)
                if trial.should_prune():
                    raise optuna.TrialPruned()
                if val_mae < best_val:
                    best_val, patience_count = val_mae, 0
                else:
                    patience_count += 1
                    if patience_count >= 4:
                        break
            return best_val

        study = optuna.create_study(direction='minimize',
                                    pruner=optuna.pruners.MedianPruner(n_warmup_steps=10))
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

        # ── Visualise top-3 trials ────────────────────────────────────────────
        completed = [t for t in study.trials
                     if t.state == optuna.trial.TrialState.COMPLETE]
        top3 = sorted(completed, key=lambda t: t.value)[:3]

        out_dir = f"files/explainer_outputs/{self.database}/validation_2000"
        os.makedirs(out_dir, exist_ok=True)

        fig, ax = plt.subplots(figsize=(10, 5))
        colors = ['#e15759', '#4e79a7', '#59a14f']
        for rank, (trial_obj, color) in enumerate(zip(top3, colors), start=1):
            epochs = sorted(trial_obj.intermediate_values)
            maes   = [trial_obj.intermediate_values[e] for e in epochs]
            label  = (f"#{rank}  h={trial_obj.params['hidden_channels']}  "
                      f"L={trial_obj.params['num_layers']}  "
                      f"heads={trial_obj.params['num_heads']}  "
                      f"lr={trial_obj.params['lr']:.1e}  "
                      f"(best={trial_obj.value:.4f})")
            ax.plot(epochs, maes, color=color, lw=1.8, label=label)
            best_ep = min(trial_obj.intermediate_values, key=trial_obj.intermediate_values.get)
            ax.scatter([best_ep], [trial_obj.intermediate_values[best_ep]],
                       color=color, s=60, zorder=5)

        ax.set_xlabel('Epoch')
        ax.set_ylabel('Val MAE (normalised)')
        ax.set_title(f'Sweep — val MAE over epochs: top-3 trials\n'
                     f'({self.database}, cant={self.cant})')
        ax.legend(fontsize=8, loc='upper right')
        plt.tight_layout()
        sweep_path = f"{out_dir}/sweep_top3.png"
        plt.savefig(sweep_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Sweep plot saved to {sweep_path}")

        best = study.best_params
        best_params = {'hidden_channels': best['hidden_channels'], 'num_layers': best['num_layers'],
                       'num_heads': best['num_heads'], 'lr': best['lr'], 'weight_decay': 0.001}
        print(f"Best params for {self.task_id}: {best_params}  (val MAE: {study.best_value:.4f})")
        self._save_params(best_params)
        self.params = best_params
        self.model = self._build_model(best_params).to(self.device)

    # ── Homogeneous event graph methods ───────────────────────────────────────

    def _hetero_to_homo(self, graphs):
        """Convert normalised HeteroData prefixes to homogeneous Data (events only)."""
        from torch_geometric.data import Data
        vp = self.viewpoint_object
        et = ('Events', 'to', 'Events')
        result = []
        for g in graphs:
            edge_index = (g[et].edge_index if et in g.edge_types
                          else torch.zeros(2, 0, dtype=torch.long))
            result.append(Data(
                x          = g['Events'].x,
                edge_index = edge_index,
                y          = g[vp].y[0],             # shape [1], normalised
                id         = g[vp].id[0, 0],
                last_event = g[vp].last_event[0, 0],
            ))
        return result

    def homo_train_step(self, model, loader, optimizer, criterion, device):
        model.train()
        total_loss, total_examples = 0.0, 0
        for batch in loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out  = model(batch.x, batch.edge_index, batch.batch)  # [B]
            loss = criterion(out, batch.y.squeeze(-1))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            total_examples += batch.num_graphs
            total_loss     += loss.item() * batch.num_graphs
        return total_loss / total_examples

    @torch.no_grad()
    def homo_eval(self, loader, model, criterion, device):
        model.eval()
        total_loss, total_examples = 0.0, 0
        for batch in loader:
            batch = batch.to(device)
            out  = model(batch.x, batch.edge_index, batch.batch)
            loss = criterion(out, batch.y.squeeze(-1))
            total_examples += batch.num_graphs
            total_loss     += float(loss) * batch.num_graphs
        return total_loss / total_examples

    def Homo_Reg_Modelling(self):
        """Train a homogeneous GCN on event-only graphs; save checkpoint + training log."""

        torch.manual_seed(42)
        random.seed(42)
        np.random.seed(42)

        homo_train = self._hetero_to_homo(self.train_data)
        homo_val   = self._hetero_to_homo(self.val_data)
        homo_test  = self._hetero_to_homo(self.test_data)

        batch_size  = self.path_dict.get('batch_size', 16)
        train_loader = DataLoader(homo_train, batch_size=batch_size, shuffle=True)
        val_loader   = DataLoader(homo_val,   batch_size=batch_size, shuffle=False)
        test_loader  = DataLoader(homo_test,  batch_size=batch_size, shuffle=False)

        in_ch = homo_train[0].x.size(-1)  # 17 (normalised Events features)
        model = REG_GNN.REG_GNN(
            in_channels     = in_ch,
            hidden_channels = self.params.get('hidden_channels', 48),
            num_layers      = self.params.get('num_layers', 3),
        ).to(self.device)

        homo_model_path = self.model_path.replace(".pth", "_homo.pth")
        criterion = torch.nn.L1Loss()

        optimizer = torch.optim.Adam(
            model.parameters(),
            lr           = self.params['lr'],
            weight_decay = self.params['weight_decay'],
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", factor=0.5, patience=10
        )

        max_epochs, early_stop_patience = 200, 20
        best_val_mae, best_state        = float("inf"), None
        epochs_without_improvement      = 0
        log = []

        pbar = tqdm(range(1, max_epochs + 1), desc="HomoGNN")
        for epoch in pbar:
            train_loss = self.homo_train_step(model, train_loader, optimizer, criterion, self.device)
            val_mae    = self.homo_eval(val_loader, model, criterion, self.device)
            scheduler.step(val_mae)

            current_lr = optimizer.param_groups[0]["lr"]
            log.append({'epoch': epoch, 'train_loss': train_loss,
                        'val_mae': val_mae, 'lr': current_lr})

            if val_mae < best_val_mae:
                print("New Best!")
                best_val_mae = val_mae
                best_state   = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1

            if epoch % 10 == 0 or epoch == 1:
                print(f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
                      f"Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}")

            if epochs_without_improvement >= early_stop_patience:
                print(f"\nEarly stopping at epoch {epoch}")
                break
        pbar.close()

        if best_state is not None:
            model.load_state_dict(best_state)
        test_loss = self.homo_eval(test_loader, model, criterion, self.device)
        print(f"HomoGNN Final MAE: {test_loss:.4f}")

        torch.save(model.state_dict(), homo_model_path)
        pd.DataFrame(log).to_csv(
            homo_model_path.replace(".pth", "_training_log.csv"), index=False
        )
        print(f"HomoGNN checkpoint saved to {homo_model_path}")

    def compare_models(self):
        """Evaluate HGT and HomoGNN side-by-side on the test split and save a comparison plot."""

        vp = self.viewpoint_object
        homo_test      = self._hetero_to_homo(self.test_data)
        homo_model_path = self.model_path.replace(".pth", "_homo.pth")

        # ── Load HGT ────────────────────────────────────────────────────────
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        # ── Load HomoGNN ─────────────────────────────────────────────────────
        in_ch = homo_test[0].x.size(-1)
        homo_model = REG_GNN.REG_GNN(
            in_channels     = in_ch,
            hidden_channels = self.params.get('hidden_channels', 48),
            num_layers      = self.params.get('num_layers', 3),
        ).to(self.device)
        homo_model.load_state_dict(torch.load(homo_model_path, weights_only=False))
        homo_model.eval()

        # ── Collect predictions ──────────────────────────────────────────────
        records = []
        with torch.no_grad():
            for g_het, g_hom in zip(self.test_data, homo_test):
                g_het = g_het.to(self.device)
                g_hom = g_hom.to(self.device)

                hgt_pred_n  = self.model(g_het.x_dict, g_het.edge_index_dict)[0].item()
                homo_pred_n = homo_model(
                    g_hom.x.unsqueeze(0) if g_hom.x.dim() == 1 else g_hom.x,
                    g_hom.edge_index,
                    torch.zeros(g_hom.num_nodes, dtype=torch.long, device=self.device),
                ).item()

                true_n = g_het[vp].y[0].item()
                denorm = lambda v: (v * self.target_std.item() + self.target_mean.item()) / 3600.0

                records.append({
                    'true_h':       denorm(true_n),
                    'hgt_pred_h':   denorm(hgt_pred_n),
                    'homo_pred_h':  denorm(homo_pred_n),
                    'n_events':     g_het['Events'].num_nodes,
                    'last_event':   bool(g_het[vp].last_event[0].item()),
                })

        df        = pd.DataFrame(records)
        last_mask = df['last_event'].values
        y_true    = df['true_h'].values

        # ── Metrics helper ────────────────────────────────────────────────────
        def _metrics(y_t, y_p):
            ae   = np.abs(y_t - y_p)
            mae  = ae.mean()
            rmse = np.sqrt((ae**2).mean())
            ss_r = ((y_t - y_p)**2).sum()
            ss_t = ((y_t - y_t.mean())**2).sum()
            r2   = 1 - ss_r / ss_t if ss_t > 0 else float('nan')
            return mae, rmse, r2

        def _depth_mae(y_t, y_p, n_ev):
            bins   = [(1,3,'1-3'), (4,6,'4-6'), (7,9,'7-9'), (10,9999,'10+')]
            return {lbl: np.abs(y_t[(n_ev>=lo)&(n_ev<=hi)] - y_p[(n_ev>=lo)&(n_ev<=hi)]).mean()
                    for lo, hi, lbl in bins}

        n_ev = df['n_events'].values
        models_preds = [('HomoGNN (GCN)', df['homo_pred_h'].values),
                        ('HGT (ours)',    df['hgt_pred_h'].values)]

        # ── Print table ───────────────────────────────────────────────────────
        sep = '─' * 70
        print(f"\n{'Model':<18}  {'ALL PREFIXES':^30}  {'LAST-EVENT ONLY':^18}")
        print(f"{'':18}  {'MAE(h)':>7}  {'RMSE(h)':>7}  {'R²':>6}  "
              f"{'MAE(h)':>7}  {'RMSE(h)':>7}  {'R²':>6}")
        print(sep)
        for name, preds in models_preds:
            ma, rm, r2   = _metrics(y_true, preds)
            mal, rml, r2l = _metrics(y_true[last_mask], preds[last_mask])
            print(f"{name:<18}  {ma:7.1f}  {rm:7.1f}  {r2:6.3f}  "
                  f"{mal:7.1f}  {rml:7.1f}  {r2l:6.3f}")
        print()

        # ── Plot ─────────────────────────────────────────────────────────────
        out_dir = f"files/explainer_outputs/{self.database}/validation_2000"
        os.makedirs(out_dir, exist_ok=True)

        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # Left: scatter on last-event prefixes
        ax = axes[0]
        y_last = y_true[last_mask]
        lim = max(y_last.max(),
                  df['homo_pred_h'].values[last_mask].max(),
                  df['hgt_pred_h'].values[last_mask].max()) * 1.08

        hgt_mae_last  = _metrics(y_last, df['hgt_pred_h'].values[last_mask])[0]
        homo_mae_last = _metrics(y_last, df['homo_pred_h'].values[last_mask])[0]

        ax.scatter(y_last, df['homo_pred_h'].values[last_mask],
                   alpha=0.55, s=22, color='steelblue',
                   label=f"HomoGNN  (MAE={homo_mae_last:.1f}h)")
        ax.scatter(y_last, df['hgt_pred_h'].values[last_mask],
                   alpha=0.55, s=22, color='tomato',
                   label=f"HGT       (MAE={hgt_mae_last:.1f}h)")
        ax.plot([0, lim], [0, lim], 'k--', lw=1, label='Perfect')
        ax.set_xlim(0, lim); ax.set_ylim(0, lim)
        ax.set_xlabel('True remaining time (h)')
        ax.set_ylabel('Predicted remaining time (h)')
        ax.set_title('Predicted vs True — last-event prefixes')
        ax.legend(fontsize=9)

        # Right: MAE by depth (all prefixes)
        ax    = axes[1]
        BINS  = ['1-3', '4-6', '7-9', '10+']
        x     = np.arange(len(BINS))
        width = 0.3
        depth_data = {name: _depth_mae(y_true, preds, n_ev)
                      for name, preds in models_preds}
        colors = {'HomoGNN (GCN)': 'steelblue', 'HGT (ours)': 'tomato'}
        for i, (name, clr) in enumerate(colors.items()):
            vals = [depth_data[name][b] for b in BINS]
            bars = ax.bar(x + (i - 0.5) * width, vals, width, label=name,
                          color=clr, edgecolor='white', linewidth=0.5)
            for bar, v in zip(bars, vals):
                if not np.isnan(v):
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()+1,
                            f'{v:.0f}', ha='center', va='bottom', fontsize=7)
        ax.set_xticks(x)
        ax.set_xticklabels(BINS)
        ax.set_xlabel('Prefix depth (n events seen)')
        ax.set_ylabel('MAE (h)')
        ax.set_title('MAE by prefix depth — all prefixes')
        ax.legend(fontsize=9)

        plt.suptitle(f'HomoGNN vs HGT — {self.database} (cant={self.cant})',
                     fontsize=12, y=1.01)
        plt.tight_layout()
        out_path = f"{out_dir}/homo_comparison.png"
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Comparison plot saved to {out_path}")

    def plot_training_curves(self):
        """Plot train loss and val MAE over epochs for HGT vs HomoGNN side-by-side."""
        het_path  = self.model_path.replace(".pth", "_training_log.csv")
        homo_path = self.model_path.replace(".pth", "_homo_training_log.csv")
        het_df    = pd.read_csv(het_path)
        homo_df   = pd.read_csv(homo_path)

        HGT_COLOR  = '#e15759'
        HOMO_COLOR = '#4e79a7'

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        # ── Left: Training Loss ───────────────────────────────────────────
        ax = axes[0]
        ax.plot(het_df['epoch'],  het_df['train_loss'],  color=HGT_COLOR,  lw=1.8, label='HGT')
        ax.plot(homo_df['epoch'], homo_df['train_loss'], color=HOMO_COLOR, lw=1.8, label='HomoGNN')
        ax.set_xlabel('Epoch')
        ax.set_ylabel('Train Loss (normalised L1)')
        ax.set_title('Training Loss')
        ax.legend(fontsize=9)

        # ── Right: Validation MAE ─────────────────────────────────────────
        ax = axes[1]
        for df, name, color in [(het_df, 'HGT', HGT_COLOR), (homo_df, 'HomoGNN', HOMO_COLOR)]:
            best_idx = df['val_mae'].idxmin()
            best_ep  = int(df.loc[best_idx, 'epoch'])
            best_mae = df.loc[best_idx, 'val_mae']
            ax.plot(df['epoch'], df['val_mae'], color=color, lw=1.8,
                    label=f"{name}  (best={best_mae:.4f} @ ep {best_ep})")
            ax.scatter([best_ep], [best_mae], color=color, s=60, zorder=5)
        ax.set_xlabel('Epoch')
        ax.set_ylabel('Val MAE (normalised)')
        ax.set_title('Validation MAE')
        ax.legend(fontsize=9)

        plt.suptitle(f'Training Curves — HGT vs HomoGNN\n({self.database}, cant={self.cant})',
                     fontsize=12)
        plt.tight_layout()

        out_dir  = f"files/explainer_outputs/{self.database}/validation_2000"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/training_curves.png"
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Training curves saved to {out_path}")

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
