import torch
import json
import pandas as pd
from torch_geometric.loader import DataLoader
import sup_funcs as sf
import numpy as np
from gcn_classifier import *
from torchmetrics import F1Score, ConfusionMatrix, Accuracy
from torch_geometric.datasets import DBLP
import torch_geometric.transforms as T
from model_class import OrderPredictionHeteroGNN_2
import os

class TraceTrainer:
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
        self.train_loader_sg = DataLoader(self.train_graphs_sg, shuffle=True)
        self.val_loader_sg = DataLoader(self.val_graphs_sg, batch_size=len(self.val_graphs_sg))

        # self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.device = torch.device('cpu')

        self.model_params = pd.read_csv("files/model_parameters.csv", delimiter=',')

        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json") as json_file:
            self.tensor_dict = json.load(json_file)

    def tester(self):
        print(self.train_graphs_sg)