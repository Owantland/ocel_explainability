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
            self.model = HGT.HGT(hidden_channels=24, out_channels=1, num_layers=2,
                                 num_heads=2, data=self.train_data[0], viewpoint=self.viewpoint_object)
            test_kpi = f"TimeFrom_{self.viewpoint_object}_to_{self.path_dict['kpi_event']}"

            # Standardize the output values
            train_y_all = torch.cat([g[self.viewpoint_object].y for g in self.train_data])
            target_mean, target_std = train_y_all.mean(), train_y_all.std()
            for m in [self.train_data, self.val_data, self.test_data]:
                for g in m:
                    g[self.viewpoint_object].y = (g[self.viewpoint_object].y - target_mean) / target_std
            print(f"Mean (hours): {round(target_mean.item() / 3600)}, STD (hours): {round(target_std.item() / 3600)}")
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
        model = self.model
        device = torch.device("cpu")
        model = model.to(device)
        data = data.to(device)
        criterion = torch.nn.L1Loss()

        with torch.no_grad():
            batch = next(iter(train_loader))
            model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-5)

        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", factor=0.5, patience=10
        )

        max_epochs = 200
        early_stop_patience = 20

        best_val_mae = float("inf")
        best_state = None
        epochs_without_improvement = 0

        pbar = tqdm(range(1, max_epochs + 1))

        for epoch in pbar:
            train_loss = self.het_train(model, train_loader, optimizer, criterion, device)
            val_mae = self.het_loss_test(val_loader, model, criterion, device)  # Should use a separate validation set loader
            scheduler.step(val_mae)

            if val_mae < best_val_mae:
                print("New Best!")
                best_val_mae = val_mae
                best_state = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1

            if epoch % 10 == 0 or epoch == 1:
                current_lr = optimizer.param_groups[0]["lr"]
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
        test_loss = self.het_loss_test(test_loader, model, criterion, device)
        print(f'Final MAE: {test_loss}')

        # Save best model
        torch.save(self.model.state_dict(), self.model_path)

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