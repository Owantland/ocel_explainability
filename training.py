import torch

from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader

from model_classes import REG_GNN, CLASS_GNN, REG_GAT, HET_GNN
from torchmetrics import F1Score, ConfusionMatrix, Accuracy

import sup_funcs as sf
import pandas as pd
from tqdm import tqdm
import os

class Modelling:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])
        self.viewpoint_object = self.path_dict['kpi_viewpoint']

    def normalize_target(self, data, mean, std):
        data.y = (data.y - mean) / std
        return data

    def normalize_het(self, data, mean, std):
        data[self.viewpoint_object].y = (data[self.viewpoint_object].y - mean) / std
        return data

    def decode_epoch(self, epoch_val):
        timestamp = pd.Timestamp(epoch_val, unit='s')
        return timestamp

    def decode_time(self, total_secs):
        timestamp = pd.Timedelta(round(total_secs, 2), unit='s')
        return timestamp

    """
        Heterogeneous training and validation functions
    """
    def het_train(self, model, train_loader, optimizer, criterion, device):
        model.train()

        total_examples = total_loss = 0
        for batch in train_loader:
            optimizer.zero_grad()
            batch = batch.to(device)
            batch_size = len(batch[self.viewpoint_object].batch)
            out = model(batch.x_dict, batch.edge_index_dict)
            loss = criterion(out[:batch_size], batch[self.viewpoint_object].y[:batch_size])
            loss.backward()
            optimizer.step()

            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def het_loss_test(self, loader, model, criterion, device):
        model.eval()

        total_examples = total_loss = 0
        for data in loader:
            data = data.to(device)
            out = model(data.x_dict, data.edge_index_dict)
            batch_size = len(data[self.viewpoint_object].batch)
            loss = criterion(out[:batch_size], data[self.viewpoint_object].y[:batch_size])
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    """
        Homogeneous training and validation functions
    """
    def train(self, model, train_loader, train_data, optimizer, criterion, device):
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
    def loss_test(self, loader, model, criterion, device):
        model.eval()
        total_loss = 0
        for data in loader:
            data.to(device)
            out = model(data.x, data.edge_index, data.batch)
            loss = criterion(out, data.y)  # Compute the loss.
            total_loss += loss.item() * data.num_graphs
        return total_loss / len(loader.dataset)

    @torch.no_grad()
    def acc_test(self, loader, model, device):
        model.eval()
        f1 = F1Score("binary")
        total_correct = 0
        for data in loader:  # Iterate in batches over the training/test dataset.
            data = data.to(device)
            out = model(data.x, data.edge_index, data.batch)
            pred = out.argmax(dim=1)  # Use the class with highest probability.
            total_correct += int((pred == data.y).sum())  # Check against ground-truth labels.
            f1(pred, data.y)
        return f1.compute().item()

    def  Het_Reg_Modelling(self, training_data, val_data, test_data):
        """
        :param het_train_data:
        :param het_val_data:
        :param het_test_data:
        :return: Trains and tests a regression model for heterogeneous graph structures for comparison with the
        homogeneous model predictions and explanations.
        """
        viewpoint_object = self.viewpoint_object

        # Standardize the Y value for ease of use in the GNN architecture
        ys = torch.cat([d[viewpoint_object].y for d in training_data])
        mean, std = ys.mean(), ys.std()

        training_data = [self.normalize_het(d, mean, std) for d in training_data]
        val_data =      [self.normalize_het(d, mean, std) for d in val_data]
        test_data =     [self.normalize_het(d, mean, std) for d in test_data]

        # Create the loaders for training and validation
        train_loader = DataLoader(training_data, batch_size=128, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=64)
        test_loader = DataLoader(test_data, batch_size=64)

        # Define save path for the models
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        test_kpi = f"TimeFrom_{self.viewpoint_object}_to_{kpi_event}"

        if not os.path.exists(f"{model_path}/Hetero"):
            os.makedirs(f"{model_path}/Hetero")
        model_path = f"{model_path}/Hetero/{test_kpi}"

        # Model values
        device = torch.device("cpu")
        num_layers = 5
        width_layers = 8
        num_heads = 3
        hidden_channels = [width_layers] * num_layers
        criterion = torch.nn.L1Loss()

        learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
        patience = 5
        epochs_sg = 10

        """
            Heterogeneous model training loop:
            Trains 5 different instances of the model with decreasing learning rates in each epoch.
        """
        to_train = [i for i in range(1, 6)]
        flag = True
        while flag:
            for i in to_train:
                model = HET_GNN.HeteroGNN(hidden_channels=hidden_channels, out_channels=1, num_layers=num_layers,
                                          num_heads=num_heads, data=training_data[0], viewpoint=viewpoint_object)
                model = model.to(device)
                best_val = 10e7
                counter = 0
                for epoch, lr in enumerate(learning_rates):
                    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
                    loss = self.het_train(model, train_loader, optimizer, criterion, device)
                    val_loss = self.het_loss_test(val_loader, model, criterion, device)
                    print(f"{i} - Epoch: {epoch:03d}, LR: {lr}, Loss: {loss:.4f}, Val Loss: {val_loss}")
                    if val_loss < best_val:
                        print('New Best')
                        best_val = val_loss
                        torch.save(model.state_dict(),
                                   f"{model_path}_{i}.pth")
                        counter = 0
                    else:
                        counter += 1

                    if counter > patience:
                        print('---')
                        break
                if epoch + 1 >= epochs_sg:
                    to_train.remove(i)
            if len(to_train) == 0:
                flag = False

        test_loss = self.het_loss_test(test_loader, model, criterion, device)
        print(f'Final MAE: {test_loss} \nMean: {mean.item()}\nSTD: {std.item()}')

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
        val_data = [self.normalize_target(d, mean, std) for d in val_data]
        test_data = [self.normalize_target(d, mean, std) for d in test_data]

        # Create appropriate loaders
        train_loader = DataLoader(train_data, batch_size=64, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=64)
        test_loader = DataLoader(test_data, batch_size=64)

        # Define save path for the models
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        test_kpi = f"TimeFrom_{self.viewpoint_object}_to_{kpi_event}"

        if not os.path.exists(f"{model_path}/Homo"):
            os.makedirs(f"{model_path}/Homo")
        model_path = f"{model_path}/Homo/{test_kpi}"

        # Model values
        num_node_features = 11 if self.database == 'order_management' else 14
        device = torch.device("cpu")
        num_layers = 5
        width_layers = 8
        num_heads = 3
        hidden_channels = [width_layers] * num_layers
        criterion = torch.nn.L1Loss()

        learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
        patience = 5
        epochs_sg = 10
        """
            Homogeneous model training loop:
            Trains 5 different instances of the model with decreasing learning rates in each epoch.
        """
        to_train = [i for i in range(1, 6)]
        flag = True
        while flag:
            for i in to_train:
                model = REG_GNN.REG_GNN(in_channels=num_node_features, hidden_channels=64, num_layers=3)
                model = model.to(device)
                best_val = float("inf")
                counter = 0
                for epoch, lr in enumerate(learning_rates):
                    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
                    loss = self.train(model, train_loader, train_data, optimizer, criterion, device)
                    val_loss = self.loss_test(val_loader, model, criterion, device)
                    print(f"{i} - Epoch: {epoch:03d}, LR: {lr}, Loss: {loss:.4f}, Val Loss: {val_loss}")
                    if val_loss < best_val:
                        print('New Best')
                        best_val = val_loss
                        torch.save(model.state_dict(),
                                   f"{model_path}_{i}.pth")
                        counter = 0
                    else:
                        counter += 1

                    if counter > patience:
                        print('---')
                        break
                if epoch + 1 >= epochs_sg:
                    to_train.remove(i)
            if len(to_train) == 0:
                flag = False

        # pbar = tqdm(range(1, 101))
        # best_val_loss = float("inf")
        # for epoch in pbar:
        #     train_loss = self.train(model, train_loader, train_data, optimizer, criterion, device)
        #     val_loss = self.loss_test(val_loader, model, criterion, device)
        #     print(f'Epoch: {epoch:03d}, Train Loss: {train_loss:.4f}, Val Loss: {val_loss}')
        #
        #     if val_loss < best_val_loss:
        #         print("New best!")
        #         best_val_loss = val_loss
        #         torch.save(model.state_dict(), model_path)
        # pbar.close()

        test_mae = self.loss_test(test_loader, model, criterion, device)
        print(f'Final MAE: {test_mae} \nMean: {mean.item()}\nSTD: {std.item()}')

        # Save the results in a result file
        result = {"Graph Type": ["Homogeneous"], "KPI": [test_kpi], "Metric": [test_mae],
                  "Mean": [mean.item()], "STD": [std.item()]}
        results_df = pd.DataFrame(result)
        results_df.to_csv(self.path_dict['results_path'], index=False)
        return std, mean

    def BinaryModelling(self, train_data, val_data, test_data):
        # Create appropriate loaders
        train_loader = DataLoader(train_data, batch_size=64, shuffle=True)
        val_loader = DataLoader(val_data, batch_size=64)
        test_loader = DataLoader(test_data, batch_size=64)

        # Define variables for the model
        num_node_features = 11
        model = CLASS_GNN.GNN(in_channels=num_node_features, hidden_channels=64, out_channels=2)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
        criterion = F.cross_entropy
        device = torch.device("cpu")
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        model_path = f"{model_path}/BinaryClass_{kpi_event}.pth"

        min_val = 0.5
        pbar = tqdm(range(1, 301))
        for epoch in pbar:
            train_loss = self.train(model, train_loader, train_data, optimizer, criterion, device)
            train_acc = self.acc_test(train_loader, model, device)
            val_acc = self.acc_test(val_loader, model, device)
            print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Val Acc: {val_acc:.4f}')
            if val_acc > min_val:
                print("New best!")
                min_val = val_acc
                torch.save(model.state_dict(), model_path)
        pbar.close()

        test_acc = self.acc_test(test_loader, model, device)
        print(f'Final Test Acc: {test_acc:.4f}')

    def Modelling(self):
        """
            Main function, obtains the relevant files and selects the appropriate training and validation functions to
            run for the chosen KPI
        """
        # Load homogeneous data sets
        file_path = self.path_dict['pytorch_path']
        hom_train_data = torch.load(f"{file_path}/train_graphs_hom.pt", weights_only=False)
        hom_val_data = torch.load(f"{file_path}/val_graphs_hom.pt", weights_only=False)
        hom_test_data = torch.load(f"{file_path}/test_graphs_hom.pt", weights_only=False)

        # Load heterogeneous data sets
        het_train_data = torch.load(f"{file_path}/train_graphs_sg.pt", weights_only=False)
        het_val_data = torch.load(f"{file_path}/val_graphs_sg.pt", weights_only=False)
        het_test_data = torch.load(f"{file_path}/test_graphs_sg.pt", weights_only=False)

        kpi_type = self.path_dict['kpi_type']
        if kpi_type == 0: # Regression
            std, mean = self.RegressionModelling(hom_train_data, hom_val_data, hom_test_data)
            self.Het_Reg_Modelling(het_train_data, het_val_data, het_test_data)
            return std, mean
        elif kpi_type == 1: #Binary Classification
            self.BinaryModelling(hom_train_data, hom_val_data, hom_test_data)