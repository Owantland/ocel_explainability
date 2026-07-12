import ast
import time
import torch
import json
import random
import numpy as np

from torch_geometric.data import Data
from torch_geometric.loader import DataLoader

from model_classes import REG_GNN, HGT

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
        self.kpi_viewpoint = self.path_dict['kpi_viewpoint']
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

        # Load relevant datasets
        self.train_data = torch.load(f"{self.path_dict['pytorch_path']}/train_graphs_sg.pt", weights_only=False)
        self.val_data = torch.load(f"{self.path_dict['pytorch_path']}/val_graphs_sg.pt", weights_only=False)
        self.test_data = torch.load(f"{self.path_dict['pytorch_path']}/test_graphs_sg.pt", weights_only=False)

        # Model parameters
        # Matches sweep()'s budget and HOEG's Table 5/7 (single unified
        # epoch/patience regime for both tuning and final reported results)
        # for order_management, where this budget trains cleanly. logistics'
        # last-event class is far rarer (138 of 8651 test prefixes) and the
        # model needs materially more epochs to learn it -- under 30/4 it
        # collapsed to a near-constant predictor (last-event R2 = -1700.6,
        # zero feature attribution on every non-viewpoint node type) after
        # early-stopping at epoch 6. Kept on a longer, pre-existing budget
        # here as a deliberate, documented deviation from HOEG parity for
        # this one dataset -- see TRAINING_VS_HOEG.md recommendation 1.
        if self.database == 'logistics':
            self.max_epochs = 100
            self.early_stop_patience = 10
        else:
            self.max_epochs = 30
            self.early_stop_patience = 4

        kpi_type = self.path_dict['kpi_type']
        if kpi_type != 0:
            raise NotImplementedError(
                f"kpi_type={kpi_type} is not supported -- only regression (kpi_type=0) is "
                f"implemented. The binary-classification path (BinaryModelling, HGT_CLASS, "
                f"class_train/class_eval) was removed as unused/abandoned scaffolding."
            )

        # Load model defaults if sweep is not succesful
        _DEFAULTS = {'hidden_channels': 64, 'num_layers': 2, 'num_heads': 2, 'lr': 0.001, 'weight_decay': 1e-5}

        # Appropriately name the task
        self.task_id = f"TimeFrom_{self.kpi_viewpoint}_to_{self.path_dict['kpi_event']}"
        self.params = self._load_params() or _DEFAULTS # Ensure there are sweep hyperparemeters to load
        self.model = self._build_model(self.params)

        # Standardize the output values.
        # Checks if there is a saved file containing the standardized values to import
        train_y_all = torch.cat([g[self.kpi_viewpoint].y for g in self.train_data])
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
                g[self.kpi_viewpoint].y = (g[self.kpi_viewpoint].y - target_mean) / target_std

        # Print out the Mean and STD of the chosen regression database
        print(f"Training-set target normalization stats -- Mean (hours): "
              f"{round(target_mean.item() / 3600)}, STD (hours): {round(target_std.item() / 3600)} "
              f"(used to z-normalize the remaining-time target and as the '>1 std = large shift' "
              f"threshold in LOO; not a validation/error metric)")
        self.target_mean, self.target_std = target_mean.to(self.device), target_std.to(self.device)

        # Node feature standardization — Must always be performed
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

        # Constructs a list of feature names for use in the explainer layer
        self.feature_names = {
            **{nt: names for nt, names in (self.path_dict.get('attributes') or {}).items()},
            **{nt: names for nt, names in (self.path_dict.get('time_attributes') or {}).items()},
        }

        # Extend feature names for enriched node types based on actual graph feature counts.
        # Events layout: [ev_type one-hot | temporal | C3 activity counts | O1-ext obj counts]
        _temporal_names = ['elapsed_h', 'waiting_h', 'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos']
        _order_extra = ['n_items', 'total_weight', 'n_products', 'n_packages']
        if self.train_data and self.train_data[0]['Events'].num_nodes > 0:
            _ocel      = pd.read_csv(f"{self.path_dict['graph_output_path']}ocel.csv")
            _n_types   = len(ast.literal_eval(_ocel['ev_type'].iloc[0]))
            _obj_types = [c.replace('::ids', '') for c in _ocel.columns if c.endswith('::ids')]

            # Real event-type names, via the same alphabetical query ocel_generator.py's
            # _build_ev_encodings used to build the one-hot encoding -- index i here matches
            # the one-hot/C3 index i there exactly, so this is a pure label lookup, not a
            # recomputation.
            self.funcs.cursor.execute("SELECT DISTINCT OCEL_TYPE_MAP FROM EVENT_MAP_TYPE ORDER BY 1")
            _ev_names = [r[0] for r in self.funcs.cursor.fetchall()]
            if len(_ev_names) == _n_types:
                _type_names = _ev_names
                _c3_names = [f'c3_{name}' for name in _ev_names]
            else:
                _type_names = [f'type_{i}' for i in range(_n_types)]
                _c3_names = [f'c3_{i}' for i in range(_n_types)]

            self.feature_names['Events'] = (
                _type_names + _temporal_names + _c3_names
                + [f'o1_{ot}' for ot in _obj_types]
            )

        # Real names for encoding/role_encoding node types (e.g. Customers/Employees for
        # order_management, HandlingUnit/Truck/Forklift/Vehicle for logistics) -- config
        # driven, not hardcoded to a specific dataset's type names. Mirrors the exact
        # queries ocel_generator.py used to build these encodings (_build_role_encodings,
        # get_1h_encoding), including its >50-distinct-entities -> 1D fallback.
        _role_encoding = self.path_dict.get('role_encoding') or {}
        for _ob_type in (self.path_dict.get('encoding') or []):
            # Search beyond graph 0 -- unlike Events/the viewpoint, an encoded type like
            # Employees may have zero nodes in early/short prefixes even though it's
            # populated later, so graph 0 alone isn't a reliable presence check.
            _graph_with_type = next(
                (g for g in self.train_data if g[_ob_type].num_nodes > 0), None
            ) if self.train_data else None
            if _graph_with_type is None:
                continue
            _n_dims = _graph_with_type[_ob_type].x.shape[1]
            _table = f'object_{_ob_type}'
            if not self.funcs.col_names(_table):
                _table = f'event_{_ob_type}'

            if _ob_type in _role_encoding:
                _role_col = _role_encoding[_ob_type]
                self.funcs.cursor.execute(
                    f"SELECT DISTINCT {_role_col} FROM {_table} WHERE {_role_col} IS NOT NULL ORDER BY 1"
                )
                _names = [r[0] for r in self.funcs.cursor.fetchall()]
            else:
                self.funcs.cursor.execute(f"SELECT DISTINCT OCEL_ID FROM {_table} ORDER BY 1")
                _ids = [r[0] for r in self.funcs.cursor.fetchall()]
                _names = [f'{_ob_type}_present'] if len(_ids) > 50 else _ids

            if len(_names) == _n_dims:
                self.feature_names[_ob_type] = _names

        if self.train_data and self.train_data[0][self.kpi_viewpoint].num_nodes > 0:
            vp = self.kpi_viewpoint
            n_vp = self.train_data[0][vp].x.shape[1]
            base_names = list((self.path_dict.get('attributes') or {}).get(vp, []))
            self.feature_names[vp] = (base_names + _order_extra)[:n_vp]

        # Define save path for the model
        model_path = self.path_dict['model_path']
        if not os.path.exists(f"{model_path}/Hetero"):
            os.makedirs(f"{model_path}/Hetero")
        self.model_path = f"{model_path}/Hetero/{self.task_id}.pth"

    # Define model parameter functions
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

    # Constructs the HGT regression model
    def _build_model(self, params):
        return HGT.HGT(hidden_channels=params['hidden_channels'], out_channels=1,
                       num_layers=params['num_layers'], num_heads=params['num_heads'],
                       data=self.train_data[0], viewpoint=self.kpi_viewpoint)

    """
        Heterogeneous Regression training and validation functions
    """
    def het_train(self, model, train_loader, optimizer, criterion, device):
        model.train()
        total_loss, total_examples = 0.0, 0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out  = model(batch.x_dict, batch.edge_index_dict)
            mask = batch[self.kpi_viewpoint].mask.view(-1)
            y    = batch[self.kpi_viewpoint].y.view(-1, out.shape[-1])
            loss = criterion(out[mask], y[mask])
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

            batch_size = int(mask.sum())
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def het_loss_test(self, loader, model, criterion, device):
        model.eval()

        total_loss, total_examples = 0.0, 0
        for batch in loader:
            batch = batch.to(device)
            out  = model(batch.x_dict, batch.edge_index_dict)
            mask = batch[self.kpi_viewpoint].mask.view(-1)
            y    = batch[self.kpi_viewpoint].y.view(-1, out.shape[-1])
            loss = criterion(out[mask], y[mask])

            batch_size = int(mask.sum())
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    def Het_Reg_Modelling(self, training_data, val_data, test_data):
        # Estanlish seeds to ensure replicability
        torch.manual_seed(42)
        random.seed(42)
        np.random.seed(42)

        # Define the data loaders
        batch_size = self.path_dict.get('batch_size', 16)
        train_loader = DataLoader(training_data, batch_size=batch_size, shuffle=True)
        val_loader   = DataLoader(val_data,      batch_size=batch_size, shuffle=False)
        test_loader  = DataLoader(test_data,     batch_size=batch_size, shuffle=False)

        # Instantiate model, optimizer and criterion
        model = self.model.to(self.device)
        criterion = torch.nn.L1Loss()

        with torch.no_grad():
            batch = next(iter(train_loader)).to(self.device)
            model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(
            model.parameters(),
            lr=self.params['lr'],
            weight_decay=self.params['weight_decay'],
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", factor=0.5, patience=10
        )

        max_epochs, early_stop_patience = self.max_epochs, self.early_stop_patience
        best_val_mae, best_state = float("inf"), None
        epochs_without_improvement = 0
        log = []

        pbar = tqdm(range(1, max_epochs + 1))
        fit_start = time.time()  # fitting time, HOEG Table 7-style: loop only, not data loading

        for epoch in pbar:
            train_loss = self.het_train(model, train_loader, optimizer, criterion, self.device)
            val_mae    = self.het_loss_test(val_loader, model, criterion, self.device)
            scheduler.step(val_mae)

            current_lr = optimizer.param_groups[0]["lr"]
            log.append({'epoch': epoch, 'train_loss': train_loss,
                        'val_mae': val_mae, 'lr': current_lr})
            print(
                f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
                f"Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}"
            )

            if val_mae < best_val_mae:
                print("New Best!")
                best_val_mae = val_mae
                best_state = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1

            if epochs_without_improvement >= early_stop_patience:
                print(f"\nEarly stopping at epoch {epoch} "
                      f"(no val improvement for {early_stop_patience} epochs)")
                break
        pbar.close()
        fit_time_s = time.time() - fit_start
        print(f"Fitting time: {fit_time_s:.4f}s")

        if best_state is not None:
            model.load_state_dict(best_state)
        test_loss = self.het_loss_test(test_loader, model, criterion, self.device)
        print(f'Final MAE: {test_loss}')

        torch.save(self.model.state_dict(), self.model_path)

        norm_path = self.model_path.replace(".pth", "_norm.json")
        with open(norm_path, "w") as f:
            json.dump({"target_mean": self.target_mean.item(),
                       "target_std":  self.target_std.item(),
                       "fit_time_s":  fit_time_s}, f)

        log_path = self.model_path.replace(".pth", "_training_log.csv")
        pd.DataFrame(log).to_csv(log_path, index=False)

    def sweep(self, n_trials=None):
        """
        Performs a training sweep to find the optimal hidden_channels/lr (and, for logistics,
        num_layers) for this dataset.

        num_layers and num_heads are fixed, not tuned, for order_management (see
        TRAINING_VS_HOEG.md recs 5a/5b): HOEG (Smit et al. 2024) fixes message-passing depth at 2
        and has no attention-head analogue at all, and this session found num_layers=1 makes any
        node type without a direct edge to the viewpoint provably unreachable
        (EXPLAINABILITY_DEPTH.md) -- sweep() was previously free to (and did) select it.

        logistics is a deliberate, empirically-forced exception: retraining
        TimeFrom_CustomerOrder_to_Depart under the fixed depth=2 collapsed the model to a
        near-constant predictor (last-event R2 approx -1700) regardless of epoch/patience budget,
        while a prior one-off experiment (experiment_num_layers3.py) showed num_layers=3 alone
        cuts last-event MAE from 243.6h to 28.2h. sweep() now tunes num_layers in {3,4,5} for
        logistics specifically -- a second, documented deviation from HOEG parity for this
        dataset (see TRAINING_VS_HOEG.md), on top of its already-documented epoch/patience one.
        num_heads is still fixed at 2 for both datasets (no HOEG analogue either way).

        hidden_channels excludes {128, 256} for logistics specifically (rec 5d): HOEG's own
        Section 6.1 finding is that smaller hidden_dims suit messier data, and this project
        separately measured ~285s/epoch at hidden_channels=128 on logistics, so those two values
        are disproportionately expensive there for a choice literature already argues against.

        Trials are persisted to a SQLite-backed Optuna study (study_name keyed on database/task_id)
        so a killed/interrupted run can be resumed by simply calling sweep() again -- it picks up
        from the last completed trial instead of restarting the whole grid.

        :param n_trials: number of trials; defaults to exactly the grid size (rec 5c) so a
                         GridSampler run covers every combination once with no wasted repeats
        :return: Saves the best hyperparameters to a json file
        """
        import optuna
        torch.manual_seed(42)
        random.seed(42)
        np.random.seed(42)

        criterion = torch.nn.L1Loss()
        batch_size = self.path_dict.get('batch_size', 16)

        FIXED_NUM_LAYERS = 2  # matches HOEG's fixed message-passing depth (order_management only)
        FIXED_NUM_HEADS = 2   # no HOEG analogue; fixed rather than tuned with no grounding

        is_logistics = self.database == 'logistics'

        hidden_choices = ([8, 16, 24, 32, 48, 64] if is_logistics
                          else [8, 16, 24, 32, 48, 64, 128, 256])
        lr_choices = [1e-3, 1e-2]
        # logistics tunes num_layers in {3,4,5} (see docstring); order_management keeps it fixed.
        layer_choices = [3, 4, 5] if is_logistics else None
        n_combos = len(hidden_choices) * len(lr_choices) * (len(layer_choices) if is_logistics else 1)
        n_trials = n_combos if n_trials is None else min(n_trials, n_combos)

        def objective(trial):
            hidden_channels = trial.suggest_categorical('hidden_channels', hidden_choices)
            lr = trial.suggest_categorical('lr', lr_choices)
            num_layers = (trial.suggest_categorical('num_layers', layer_choices)
                          if is_logistics else FIXED_NUM_LAYERS)

            trial_params = {'hidden_channels': hidden_channels, 'num_layers': num_layers,
                            'num_heads': FIXED_NUM_HEADS, 'lr': lr}
            model = self._build_model(trial_params).to(self.device)

            train_loader = DataLoader(self.train_data, batch_size=batch_size, shuffle=True)
            val_loader   = DataLoader(self.val_data,   batch_size=batch_size, shuffle=False)
            with torch.no_grad():
                batch = next(iter(train_loader)).to(self.device)
                model(batch.x_dict, batch.edge_index_dict)

            # No LR scheduler here (rec 5e): a ReduceLROnPlateau(patience=10) can never
            # actually fire within a trial that early-stops after patience_count reaches 4 --
            # the trial always ends first, so the scheduler was dead weight, not a real effect.
            optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
            best_val, patience_count = float('inf'), 0

            for epoch in range(1, 31):
                self.het_train(model, train_loader, optimizer, criterion, self.device)
                val_mae = self.het_loss_test(val_loader, model, criterion, self.device)
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

        # GridSampler over the now fully-enumerable (hidden_channels x lr [x num_layers for
        # logistics]) space guarantees every combination is tried exactly once in n_combos
        # trials — no wasted repeats from TPE re-sampling a space this small (rec 5c).
        search_space = {'hidden_channels': hidden_choices, 'lr': lr_choices}
        if is_logistics:
            search_space['num_layers'] = layer_choices

        # SQLite-backed storage makes the study resumable: if this process is killed mid-sweep
        # (has happened on long logistics runs), simply calling sweep() again picks up from the
        # last completed trial instead of losing all progress and restarting the grid.
        db_dir = os.path.dirname(self.model_path)
        os.makedirs(db_dir, exist_ok=True)
        study = optuna.create_study(
            direction='minimize',
            sampler=optuna.samplers.GridSampler(search_space),
            pruner=optuna.pruners.MedianPruner(n_warmup_steps=5),
            storage=f"sqlite:///{db_dir}/sweep_{self.task_id}.db",
            study_name=f"{self.database}_{self.task_id}",
            load_if_exists=True,
        )

        # A killed process leaves its in-flight trial stuck at RUNNING forever (Optuna has no
        # liveness check without heartbeat monitoring configured), and GridSampler treats RUNNING
        # trials as claiming their grid cell -- so on resume it would otherwise skip that
        # combination forever. Since sweep() only ever runs one trial at a time in this project,
        # any RUNNING trial found here on (re)start must be stale from an earlier kill, not an
        # actual concurrent run -- fail it so GridSampler retries that grid cell.
        for _stale in study.trials:
            if _stale.state == optuna.trial.TrialState.RUNNING:
                study.tell(_stale.number, state=optuna.trial.TrialState.FAIL)

        # Seed with an informed prior from HOEG (Smit et al. 2024, Section 6.1): lower learning
        # rate (0.001) generally scored better across their tuning experiment, and the
        # dataset-conditional hidden_dims expectation from the same finding is now baked
        # directly into hidden_choices above rather than only seeded here. Enqueuing this combo
        # first just means the most-likely-good result is available early if the sweep is
        # interrupted; GridSampler still covers the rest of the grid regardless.
        _hidden_prior = 256 if self.database == 'order_management' else 64
        _prior_trial = {'hidden_channels': _hidden_prior, 'lr': 1e-3}
        if is_logistics:
            # Deepest choice first: this session's evidence points toward more depth helping
            # logistics specifically (see docstring).
            _prior_trial['num_layers'] = 5
        study.enqueue_trial(_prior_trial)

        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

        # ── Visualise top-3 trials ────────────────────────────────────────────
        completed = [t for t in study.trials
                     if t.state == optuna.trial.TrialState.COMPLETE]
        top3 = sorted(completed, key=lambda t: t.value)[:3]

        out_dir = f"files/explainer_outputs/{self.database}/validation_{self.cant}"
        os.makedirs(out_dir, exist_ok=True)

        fig, ax = plt.subplots(figsize=(10, 5))
        colors = ['#e15759', '#4e79a7', '#59a14f']
        for rank, (trial_obj, color) in enumerate(zip(top3, colors), start=1):
            epochs = sorted(trial_obj.intermediate_values)
            maes   = [trial_obj.intermediate_values[e] for e in epochs]
            label  = (f"#{rank}  h={trial_obj.params['hidden_channels']}  "
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
        best_params = {'hidden_channels': best['hidden_channels'],
                       'num_layers': best['num_layers'] if is_logistics else FIXED_NUM_LAYERS,
                       'num_heads': FIXED_NUM_HEADS, 'lr': best['lr'], 'weight_decay': 1e-5}
        print(f"Best params for {self.task_id}: {best_params}  (val MAE: {study.best_value:.4f})")
        self._save_params(best_params)
        self.params = best_params
        self.model = self._build_model(best_params).to(self.device)


    """
        ─────────────────────── Homogeneous event graph methods ───────────────────────────────────────
    """
    def _hetero_to_homo(self, graphs):
        """Convert normalised HeteroData prefixes to homogeneous Data (events only)."""
        vp = self.kpi_viewpoint
        et = ('Events', 'to', 'Events')
        result = []
        for g in graphs:
            if g[vp].y.shape[0] == 0:
                # kpi_viewpoint object hasn't appeared yet in this prefix (e.g. before the
                # object-creating event) — no supervision target to convert, skip it.
                continue
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

        # Ensure replicability of results
        torch.manual_seed(42)
        random.seed(42)
        np.random.seed(42)

        # Transform heterogeneous graphs into homogeneous analogs
        homo_train = self._hetero_to_homo(self.train_data)
        homo_val   = self._hetero_to_homo(self.val_data)
        homo_test  = self._hetero_to_homo(self.test_data)

        # Prepare the loaders for the train/test loop
        batch_size  = self.path_dict.get('batch_size', 16)
        train_loader = DataLoader(homo_train, batch_size=batch_size, shuffle=True)
        val_loader   = DataLoader(homo_val,   batch_size=batch_size, shuffle=False)
        test_loader  = DataLoader(homo_test,  batch_size=batch_size, shuffle=False)

        # Prepare the model with best parameters
        in_ch = homo_train[0].x.size(-1)  # Events dim after normalisation (C3 + O1-ext enriched)
        model = REG_GNN.REG_GNN(
            in_channels     = in_ch,
            hidden_channels = self.params.get('hidden_channels', 48),
            num_layers      = self.params.get('num_layers', 2),
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

        max_epochs, early_stop_patience = self.max_epochs, self.early_stop_patience
        best_val_mae, best_state        = float("inf"), None
        epochs_without_improvement      = 0
        log = []

        # Model training loop
        pbar = tqdm(range(1, max_epochs + 1), desc="HomoGNN")
        fit_start = time.time()  # fitting time, HOEG Table 7-style: loop only, not data loading
        for epoch in pbar:
            train_loss = self.homo_train_step(model, train_loader, optimizer, criterion, self.device)
            val_mae    = self.homo_eval(val_loader, model, criterion, self.device)
            scheduler.step(val_mae)

            current_lr = optimizer.param_groups[0]["lr"]
            log.append({'epoch': epoch, 'train_loss': train_loss,
                        'val_mae': val_mae, 'lr': current_lr})

            print(f"Epoch {epoch:03d} | Train Loss: {train_loss:.4f} | "
                  f"Val MAE: {val_mae:.4f} | LR: {current_lr:.2e}")

            if val_mae < best_val_mae:
                print("New Best!")
                best_val_mae = val_mae
                best_state   = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1

            if epochs_without_improvement >= early_stop_patience:
                print(f"\nEarly stopping at epoch {epoch}")
                break
        pbar.close()
        fit_time_s = time.time() - fit_start
        print(f"Fitting time: {fit_time_s:.4f}s")

        if best_state is not None:
            model.load_state_dict(best_state)
        test_loss = self.homo_eval(test_loader, model, criterion, self.device)
        print(f"HomoGNN Final MAE: {test_loss:.4f}")

        torch.save(model.state_dict(), homo_model_path)
        pd.DataFrame(log).to_csv(
            homo_model_path.replace(".pth", "_training_log.csv"), index=False
        )
        with open(homo_model_path.replace(".pth", "_meta.json"), "w") as f:
            json.dump({"fit_time_s": fit_time_s}, f)
        print(f"HomoGNN checkpoint saved to {homo_model_path}")

    """
        Baseline comparison and graph output
    """
    def compare_models(self):
        """Evaluate HGT and HomoGNN side-by-side on the test split and save a comparison plot."""

        vp = self.kpi_viewpoint
        # _hetero_to_homo() skips prefixes where the kpi_viewpoint object hasn't appeared yet;
        # apply the same filter here so het_test and homo_test stay aligned for zip() below.
        het_test        = [g for g in self.test_data if g[vp].y.shape[0] > 0]
        homo_test       = self._hetero_to_homo(self.test_data)
        homo_model_path = self.model_path.replace(".pth", "_homo.pth")

        # ── Load HGT ────────────────────────────────────────────────────────
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        # ── Load HomoGNN ─────────────────────────────────────────────────────
        in_ch = homo_test[0].x.size(-1)
        homo_model = REG_GNN.REG_GNN(
            in_channels     = in_ch,
            hidden_channels = self.params.get('hidden_channels', 48),
            num_layers      = self.params.get('num_layers', 2),
        ).to(self.device)
        homo_model.load_state_dict(torch.load(homo_model_path, weights_only=False))
        homo_model.eval()

        # ── Collect predictions (timed separately per model, HOEG Table 7-style) ─
        denorm = lambda v: (v * self.target_std.item() + self.target_mean.item()) / 3600.0
        records = []
        hgt_pred_time_s = 0.0
        homo_pred_time_s = 0.0
        with torch.no_grad():
            for g_het, g_hom in zip(het_test, homo_test):
                g_het = g_het.to(self.device)
                g_hom = g_hom.to(self.device)

                _t0 = time.time()
                hgt_pred_n = self.model(g_het.x_dict, g_het.edge_index_dict)[0].item()
                hgt_pred_time_s += time.time() - _t0

                _t0 = time.time()
                homo_pred_n = homo_model(
                    g_hom.x.unsqueeze(0) if g_hom.x.dim() == 1 else g_hom.x,
                    g_hom.edge_index,
                    torch.zeros(g_hom.num_nodes, dtype=torch.long, device=self.device),
                ).item()
                homo_pred_time_s += time.time() - _t0

                true_n = g_het[vp].y[0].item()

                records.append({
                    'true_h':       denorm(true_n),
                    'hgt_pred_h':   denorm(hgt_pred_n),
                    'homo_pred_h':  denorm(homo_pred_n),
                    'n_events':     g_het['Events'].num_nodes,
                    'last_event':   bool(g_het[vp].last_event[0].item()),
                })

        # ── Scalability: read back training-time fitting cost, report alongside
        # the just-measured prediction cost — mirrors HOEG's Table 7 columns.
        def _read_fit_time(meta_path):
            if os.path.exists(meta_path):
                with open(meta_path) as f:
                    return json.load(f).get("fit_time_s")
            return None

        hgt_fit_time_s  = _read_fit_time(self.model_path.replace(".pth", "_norm.json"))
        homo_fit_time_s = _read_fit_time(homo_model_path.replace(".pth", "_meta.json"))

        def _fmt_time(v):
            return f"{v:.4f}" if v is not None else "n/a"

        print(f"\n{'Model':<18}  {'Fitting Time (s)':>18}  {'Prediction Time (s)':>20}")
        print(f"{'HomoGNN (GCN)':<18}  {_fmt_time(homo_fit_time_s):>18}  {homo_pred_time_s:>20.4f}")
        print(f"{'HGT (ours)':<18}  {_fmt_time(hgt_fit_time_s):>18}  {hgt_pred_time_s:>20.4f}")

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
        out_dir = f"files/explainer_outputs/{self.database}/validation_{self.cant}"
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

        out_dir  = f"files/explainer_outputs/{self.database}/validation_{self.cant}"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/training_curves.png"
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Training curves saved to {out_path}")

    def Modelling(self):
        """
            Main function: trains the HGT regression model.
        """
        self.Het_Reg_Modelling(self.train_data, self.val_data, self.test_data)
