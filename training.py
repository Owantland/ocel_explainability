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
            batch = batch.to(device)
            optimizer.zero_grad()
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
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x_dict, batch.edge_index_dict)
            batch_size = len(batch[self.viewpoint_object].batch)
            loss = criterion(out[:batch_size], batch[self.viewpoint_object].y[:batch_size])
            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    """
        Hetero Classifier training and validation functions
    """
    def class_train(self, model, train_loader, optimizer, criterion, device):
        model.train()

        total_loss = total_examples = 0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            batch_size = len(batch[self.viewpoint_object].batch)
            out = model(batch.x_dict, batch.edge_index_dict)
            seed_out = out[:batch_size]
            seed_y = batch[self.viewpoint_object].y[:batch_size]

            loss = criterion(seed_out, seed_y)
            loss.backward()
            optimizer.step()

            total_loss += loss.item() * batch_size
            total_examples += batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def class_eval(self, loader, model, device):
        model.eval()
        total_correct = total_examples = 0
        f1 = F1Score("binary")
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x_dict, batch.edge_index_dict)
            batch_size = len(batch[self.viewpoint_object].batch)
            seed_out = out[:batch_size].argmax(dim=-1)
            seed_y = batch[self.viewpoint_object].y[:batch_size]

            total_correct += (seed_out == seed_y).sum().item()
            f1(seed_out, seed_y)
            total_examples += batch_size

        return total_correct / total_examples, f1.compute().item()

    def  Het_Reg_Modelling(self, training_data, val_data, test_data):
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

        # Define save path for the models
        model_path = self.path_dict['model_path']
        kpi_event = self.path_dict['kpi_event']
        test_kpi = f"Classifier_{kpi_event}"

        if not os.path.exists(f"{model_path}/Hetero"):
            os.makedirs(f"{model_path}/Hetero")
        model_path = f"{model_path}/Hetero/{test_kpi}.pth"

        # Create model
        data = training_data[0]
        model = HGT_CLASS.HGT_CLASS(hidden_channels=64, out_channels=2, num_heads=2,
                                    num_layers=2, data=data, viewpoint=viewpoint_object)
        device = torch.device("cpu")
        model = model.to(device)
        data = data.to(device)
        criterion = F.cross_entropy

        # Materialize HGTConv's lazy linear layers with one real forward pass
        # before constructing the optimizer (same reason as the regression example).
        with torch.no_grad():
            batch = next(iter(train_loader))
            model(batch.x_dict, batch.edge_index_dict)

        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)#, weight_decay=1e-5)

        # Run training loop
        best_val = 0.0
        pbar = tqdm(range(1, 51))

        for epoch in pbar:
            train_loss = self.class_train(model, train_loader, optimizer, criterion, device)
            val_acc, val_f1 = self.class_eval(val_loader, model, device)

            print(f'Epoch: {epoch:03d}, Loss: {train_loss:.4f}, Val ACC: {val_acc:.4f} | Val F1: {val_f1:.4f}')
            if val_f1 > best_val:
                best_val = val_acc
                print("New best!")
                torch.save(model.state_dict(), model_path)

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

    def Modelling(self):
        """
            Main function, obtains the relevant files and selects the appropriate training and validation functions to
            run for the chosen KPI
        """
        # Load homogeneous data sets
        file_path = self.path_dict['pytorch_path']
        # Load heterogeneous data sets
        het_train_data = torch.load(f"{file_path}/train_graphs_sg.pt", weights_only=False)
        het_val_data = torch.load(f"{file_path}/val_graphs_sg.pt", weights_only=False)
        het_test_data = torch.load(f"{file_path}/test_graphs_sg.pt", weights_only=False)

        kpi_type = self.path_dict['kpi_type']
        if kpi_type == 0: # Regression
            self.Het_Reg_Modelling(het_train_data, het_val_data, het_test_data)
        elif kpi_type == 1: #Binary Classification
            self.BinaryModelling(het_train_data, het_val_data, het_test_data)