import torch
import json
from model_class import OrderPredictionHeteroGNN_2
import pandas as pd
from torch_geometric.loader import DataLoader
import sup_funcs as sf
import os

class Trainer:
    def __init__(self, database):
        self.database = database
        self.funcs = sf.SupportFunctions(database)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])

        # Add this to config file
        kpi_event = [x for x in self.path_dict['kpis'].keys()]
        self.kpi_event = kpi_event[0]

        self.train_graphs_sg = torch.load(f"{self.path_dict['hetero_path']}/train_graphs_sg.pt", weights_only=False)
        self.val_graphs_sg = torch.load(f"{self.path_dict['hetero_path']}/val_graphs_sg.pt", weights_only=False)

        self.criterion = torch.nn.L1Loss()
        # self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.device = torch.device('cpu')

        self.model_params = pd.read_csv("files/model_parameters.csv", delimiter=',')

        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json") as json_file:
            self.tensor_dict = json.load(json_file)

    def training_loop(self, model, train_loader, val_loader, viewpoint, optimizer):
        model.to(self.device)  # Move the model to the GPU (or CPU)
        model.train()

        to_div = 0
        train_loss = 0
        for idx, batch in enumerate(train_loader):
            # Move the batch to the appropriate device
            batch = batch.to(self.device)

            optimizer.zero_grad()
            out = model(batch)
            mask = batch[viewpoint].mask
            loss = self.criterion(out[mask], batch.y_dict[viewpoint][mask])

            loss.backward()
            optimizer.step()

            to_div += sum(mask)
            train_loss += loss.item() * sum(mask)

        test_loss = 0
        to_div_2 = 0

        with torch.no_grad():
            model.eval()
            for idx, batch in enumerate(val_loader):
                batch = batch.to(self.device)
                # Move the batch to the appropriate device

                out = model(batch)
                mask = batch[viewpoint].mask
                loss = self.criterion(out[mask], batch.y_dict[viewpoint][mask])

                to_div_2 += sum(mask)
                test_loss += loss.item() * sum(mask)

        return (train_loss / to_div).item(), (test_loss / to_div_2).item()

    def trainer(self):
        for key in self.path_dict['kpis'].keys():
            ob_index = self.path_dict['kpis'][key]
            for kpi_ob in self.path_dict['kpis'][key]:
                flag = True

                # Validate model path exists
                model_path = f"{self.path_dict['model_output_path']}/{kpi_ob}"
                if not os.path.exists(model_path):
                    os.makedirs(model_path)

                parameters = self.model_params[self.model_params['kpi_ob']==kpi_ob]
                num_layers = int(parameters['num_layers'].values[0])
                width_layers = int(parameters['width_layers'].values[0])
                heads = int(parameters['heads'].values[0])
                batch_size = int(parameters['batch_size'].values[0])
                epochs_sg = int(parameters['epochs_sg'].values[0])

                learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
                patience = 5

                train_loader_sg = DataLoader(self.train_graphs_sg, shuffle=True)
                val_loader_sg = DataLoader(self.val_graphs_sg, batch_size=len(self.val_graphs_sg))

                to_train = [i for i in range(1, 6)]
                while flag:
                    for i in to_train:
                        model_sg = OrderPredictionHeteroGNN_2([width_layers] * num_layers, 1, num_layers,
                                                              heads, self.tensor_dict, kpi_ob)
                        min_loss = 10e7
                        counter = 0

                        for epoch, lr in enumerate(learning_rates):
                            optimizer = torch.optim.Adam(model_sg.parameters(), lr=lr)
                            temp = self.training_loop(model_sg, train_loader_sg, val_loader_sg, kpi_ob, optimizer)
                            print('Got temp')
                            if temp[1] < min_loss:
                                min_loss = temp[1]
                                torch.save(model_sg.state_dict(), f"{model_path}/GAT_sg_{i}.pth")
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