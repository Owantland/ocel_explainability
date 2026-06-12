import torch
import json
from model_class import OrderPredictionHeteroGNN_2
import pandas as pd
from torch_geometric.loader import DataLoader
import sup_funcs as sf
import os

class Trainer:
    def __init__(self, database, cant):
        self.database = database
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])

        # Add this to config file
        kpi_event = [x for x in self.path_dict['kpis'].keys()]
        self.kpi_event = kpi_event[0]

        self.train_graphs_sg = torch.load(f"{self.path_dict['hetero_path']}/train_graphs_sg.pt", weights_only=False)
        self.val_graphs_sg = torch.load(f"{self.path_dict['hetero_path']}/val_graphs_sg.pt", weights_only=False)

        if self.path_dict['kpi_type'] == 0:
            self.criterion = torch.nn.L1Loss()
        else:
            self.criterion = torch.nn.functional.cross_entropy

        # self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.device = 'cpu'
        self.model_params = pd.read_csv("files/model_parameters.csv", delimiter=',')

        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json") as json_file:
            self.tensor_dict = json.load(json_file)

    def train(self, model, train_loader,optimizer, kpi_ob, batch_size):
        model.train()

        total_examples = total_loss = 0
        for idx, batch in enumerate(train_loader):
            # Move the batch to the appropriate device
            batch = batch.to(self.device)

            optimizer.zero_grad()
            out = model(batch)
            mask = batch[kpi_ob].mask
            loss = self.criterion(out[:batch_size],batch[kpi_ob].y[:batch_size])
            loss.backward()
            optimizer.step()

            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def loss_test(self, model, val_loader, kpi_ob, batch_size):
        model.eval()

        test_loss = 0
        to_div_2 = 0
        for idx, batch in enumerate(val_loader):
            batch = batch.to(self.device)
            # Move the batch to the appropriate device

            out = model(batch)
            mask = batch[kpi_ob].mask
            loss = self.criterion(out[:batch_size], batch.y_dict[kpi_ob][:batch_size])

            to_div_2 += sum(mask)
            test_loss += loss.item() * sum(mask)
        return (test_loss / to_div_2).item()

    @torch.no_grad()
    def acc_test(self, model, val_loader, kpi_ob, batch_size):
        model.eval()

        total_examples = total_right = 0
        for idx, batch in enumerate(val_loader):
            batch = batch.to(self.device)
            # Move the batch to the appropriate device

            out = model(batch).argmax(dim=-1)
            correct = (out[:batch_size] == batch[kpi_ob].y[:batch_size]).sum()
            total_examples += batch_size
            total_right += correct
        return (total_right/total_examples).item()

    def training_loop(self, model, train_loader, val_loader, optimizer, kpi_ob, train_batch_size, val_batch_size):
        model.to(self.device)  # Move the model to the GPU (or CPU)

        # Train the model and obtain the training loss
        loss = self.train(model, train_loader, optimizer, kpi_ob, train_batch_size)

        if self.path_dict['kpi_type'] == 0:
            test_loss = self.loss_test(model, val_loader, kpi_ob, val_batch_size)
        elif self.path_dict['kpi_type'] == 1 or self.path_dict['kpi_type'] == 2:
            test_loss = self.acc_test(model, val_loader, kpi_ob, val_batch_size)
        return loss, test_loss

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
                train_batch_size = int(parameters['batch_size'].values[0])
                val_batch_size = len(self.val_graphs_sg)
                epochs_sg = int(parameters['epochs_sg'].values[0])

                learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
                patience = 5

                train_loader_sg = DataLoader(self.train_graphs_sg, batch_size=train_batch_size, shuffle=True)
                val_loader_sg = DataLoader(self.val_graphs_sg, batch_size=val_batch_size)

                to_train = [i for i in range(1, 6)]
                while flag:
                    for i in to_train:
                        # Assign a minimum loss or minimum accuracy score for the model to find
                        if self.path_dict['kpi_type'] == 0:
                            min_val = 10e7
                            num_variables = 1
                        elif self.path_dict['kpi_type'] == 1:
                            min_val = 0.2
                            num_variables = 2
                        elif self.path_dict['kpi_type'] == 2:
                            min_val = 0.2
                            num_variables = 4

                        model_sg = OrderPredictionHeteroGNN_2([width_layers] * num_layers, num_variables, num_layers,
                                                              heads, self.tensor_dict, kpi_ob)
                        counter = 0

                        for epoch, lr in enumerate(learning_rates):
                            optimizer = torch.optim.Adam(model_sg.parameters(), lr=lr)
                            temp = self.training_loop(model_sg, train_loader_sg, val_loader_sg, optimizer, kpi_ob,
                                                      train_batch_size, val_batch_size)

                            if self.path_dict['kpi_type'] == 0:
                                if temp[1] < min_val:
                                    print('New model found')
                                    print(temp[1])
                                    min_val = temp[1]
                                    torch.save(model_sg.state_dict(), f"{model_path}/GAT_sg_{i}.pth")
                                    counter = 0
                                else:
                                    counter += 1
                            else:
                                if temp[1] > min_val:
                                    print('New model found')
                                    print(temp[1])
                                    min_val = temp[1]
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