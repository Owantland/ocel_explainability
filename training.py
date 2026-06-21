import torch

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader

from model_classes.REG_GNN import *

import sup_funcs as sf
import pandas as pd
from tqdm import tqdm

class Modelling:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])

    def normalize_target(self, data, mean, std):
        data.y = (data.y - mean) / std
        return data

    def decode_epoch(self, epoch_val):
        timestamp = pd.Timestamp(epoch_val, unit='s')
        return timestamp

    def decode_time(self, total_secs):
        timestamp = pd.Timedelta(round(total_secs, 2), unit='s')
        return timestamp

    def reg_train(self, model, train_loader, train_data, optimizer, criterion, device):
        model.train()
        total_loss = 0
        for data in train_loader:  # Iterate in batches over the training dataset.
            # Move batch to device
            data = data.to(device)
            # Forward pass
            optimizer.zero_grad()  # Clear gradients.
            out = model(data.x, data.edge_index, data.batch)
            # Loss and backpropagation
            loss = criterion(out, data.y)  # Compute the loss.
            loss.backward()  # Derive gradients.
            optimizer.step()  # Update parameters based on gradients.
            # Loss calculation
            total_loss += loss.item() * data.num_graphs
        return total_loss / len(train_data)

    @torch.no_grad()
    def reg_test(self, loader, model, criterion, device, std, mean):
        model.eval()
        total_mae = 0
        for data in loader:
            data.to(device)
            # Forward Pass
            out = model(data.x, data.edge_index, data.batch)
            loss = criterion(out, data.y)
            # De-normalize before computing MAE so it's in original units
            pred = out * std.to(device) + mean.to(device)
            true = data.y  # * target_std.to(device) + target_mean.to(device)
            total_mae += (pred - true).abs().sum().item()
        return total_mae / len(loader.dataset)

    def RegressionModelling(self, train_data, val_data, test_data):
        """
        :param train_data: training dataset
        :param val_data: validation dataset
        :param test_data: testing dataset
        :return: A trained regression model.
        """

        # Standardize the Y value for ease of use in the GNN architecture
        ys = torch.cat([d.y for d in train_data])
        mean, std = ys.mean(), ys.std()
        train_data = [self.normalize_target(d, mean, std) for d in train_data]
        ys = torch.cat([d.y for d in train_data])

        # Create appropriate loaders
        train_loader = DataLoader(train_data, batch_size=64, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=64)
        test_loader = DataLoader(test_data, batch_size=64)

        # Define some variables for the models
        num_node_features = 11
        model = GNN(in_channels=num_node_features, hidden_channels=64, num_layers=3)
        optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)
        criterion = F.mse_loss
        device = torch.device("cpu")
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        model_path = f"{model_path}/TimeUntil_{kpi_event}.pth"

        pbar = tqdm(range(1, 101))
        best_val_mae = float("inf")
        for epoch in pbar:
            train_loss = self.reg_train(model, train_loader, train_data, optimizer, criterion, device)
            val_mae = self.reg_test(val_loader, model, criterion, device, std, mean)
            print(f'Epoch: {epoch:03d}, Train Loss: {train_loss:.4f}, Val MAE: {self.decode_time(val_mae)}')

            if val_mae < best_val_mae:
                print("New best!")
                best_val_mae = val_mae
                torch.save(model.state_dict(), model_path)
        pbar.close()

    def Modelling(self):
        """
            Main function, obtains the relevant files and selects the appropriate training and validation functions to
            run for the chosen KPI
        """
        # Load the data files
        file_path = self.path_dict['pytorch_path']
        train_data = torch.load(f"{file_path}/train_graphs_hom.pt", weights_only=False)
        val_data = torch.load(f"{file_path}/val_graphs_hom.pt", weights_only=False)
        test_data = torch.load(f"{file_path}/test_graphs_hom.pt", weights_only=False)

        kpi_type = self.path_dict['kpi_type']
        if kpi_type == 0: # Regression
            self.RegressionModelling(train_data, val_data, test_data)
        # elif kpi_type == 1: #Binary Classification
        #     self.BinaryModelling(train_data, val_data, test_data)