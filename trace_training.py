"""
    Trace Training
    A more streamlined version of the training script used by Pietro with access to different models
"""

import torch
import torch.nn.functional as F
from torchmetrics import F1Score, ConfusionMatrix, Accuracy
import torch_geometric
import torch_geometric.transforms as T
from torch_geometric.datasets import DBLP, IMDB
from torch_geometric.loader import DataLoader, RandomNodeLoader

import model_classes.HGT as HGT
import model_classes.GAT_custom as GAT
from tqdm import tqdm
import os

import sup_funcs as sf
import pandas as pd
import json


class Trainer:
    def __init__(self, database, cant):
        # Obtain the path dictionary for finding all important files
        self.database = database
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])

        # Define the KPI event name
        self.kpi_event = self.path_dict['kpi_event']

        # Define important model values that rely on wether it's a classification problem or a regression
        if self.path_dict['kpi_type'] == 0:
            self.process_type = 'Regression'
            self.criterion = torch.nn.L1Loss()
            self.out_channels = 1
            self.min_val = 10e7
        elif self.path_dict['kpi_type'] == 1:
            self.process_type = 'Binary Classification'
            self.criterion = torch.nn.functional.cross_entropy
            self.out_channels = 2
            self.min_val = 0.4
        elif self.path_dict['kpi_type'] == 2:
            self.process_type = 'Multiclass Classification'
            self.criterion = torch.nn.functional.cross_entropy
            self.out_channels = 4
            self.min_val = 0.2

        # Set the proper device for my computer
        self.model_params = pd.read_csv("files/model_parameters.csv", delimiter=',')

        # Select the proper device for running the model
        # self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.device = 'cpu'

        # Open the tensor dictionary (I think this will get taken out with the new implementation of Pietros model)
        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json") as json_file:
            self.tensor_dict = json.load(json_file)

    def train(self, model, train_loader, batch_size, optimizer):
        model.train()

        total_examples = total_loss = 0
        for batch in train_loader:
            optimizer.zero_grad()
            batch = batch.to(self.device)
            out = model(batch.x_dict, batch.edge_index_dict, self.path_dict['viewpoint'])
            loss = self.criterion(out[:batch_size],
                                   batch[self.path_dict['viewpoint']].y[:batch_size])
            loss.backward()
            optimizer.step()

            total_examples += batch_size
            total_loss += float(loss) * batch_size
        return total_loss / total_examples

    @torch.no_grad()
    def test(self, model, loader, batch_size):
        model.eval()
        acc = Accuracy(task="multiclass", num_classes=4)
        f1 = F1Score(task="multiclass", num_classes=4)
        total_examples = total_right = 0
        for batch in loader:
            batch = batch.to(self.device)
            pred = model(batch.x_dict, batch.edge_index_dict, self.path_dict['viewpoint']).argmax(dim=-1)
            correct = (pred[:batch_size] == batch[self.path_dict['viewpoint']].y[:batch_size]).sum()
            acc(pred[:batch_size], batch[self.path_dict['viewpoint']].y[:batch_size])
            f1(pred[:batch_size], batch[self.path_dict['viewpoint']].y[:batch_size])
            total_examples += batch_size
            total_right += correct
        return f1.compute().item()

    def train_model(self):
        print(f'Training {self.process_type} Model for {self.path_dict["kpi_type"]} KPIs')

        # Create the loaders for the relevant heterogeneous graphs
        train_graphs = torch.load(f"{self.path_dict['hetero_path']}/train_graphs_sg.pt", weights_only=False)
        val_graphs = torch.load(f"{self.path_dict['hetero_path']}/val_graphs_sg.pt", weights_only=False)
        test_graphs = torch.load(f"{self.path_dict['hetero_path']}/test_graphs_sg.pt", weights_only=False)

        train_batch_size = 4
        val_batch_size   = 4
        test_batch_size  = 4

        train_loader = DataLoader(train_graphs, batch_size=4, shuffle=True)
        val_loader = DataLoader(val_graphs, batch_size=4, shuffle=True)
        test_loader = DataLoader(test_graphs, batch_size=4, shuffle=True)

        # Define variables for repeated training
        learning_rates = [0.01] * 1 + [0.0075] * 1 + [0.005] * 1 + [0.0025] * 11 + [0.001] * 10 + [0.0005] * 26
        patience = 5
        epochs_sg = 10
        to_train = [i for i in range(1, 6)]

        # Define the model
        model = HGT.HGT(train_graphs[0], hidden_channels=128, out_channels= self.out_channels, num_heads=2, num_layers=1)
        model = model.to(self.device)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.005, weight_decay=0.001)

        # Validate model path exists
        model_path = f"{self.path_dict['model_output_path']}/{self.path_dict['viewpoint']}"
        if not os.path.exists(model_path):
            os.makedirs(model_path)

        pbar = tqdm(range(1, 101))
        for epoch in pbar:
            loss = self.train(model, train_loader, train_batch_size, optimizer)
            if epoch % 10 == 0:
                train_acc = self.test(model, train_loader, train_batch_size)
                val_acc = self.test(model, val_loader, val_batch_size)
                test_acc = self.test(model, test_loader, test_batch_size)
                print(f'Loss: {loss:.4f}, Train: {train_acc:.4f}, Val: {val_acc:.4f}'
                      f'Test: {test_acc:.4f}')
                if test_acc > self.min_val:
                    print("New best!")
                    min_val = test_acc
                    torch.save(model.state_dict(), f"{model_path}/HGT_sg.pth")
        pbar.close()
        model.eval()