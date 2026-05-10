import sqlite3
import pandas as pd
import pm4py
import numpy as np
import yaml
from collections import defaultdict
import copy
from itertools import permutations
from torch_geometric.data import HeteroData
import torch
from torch_geometric.loader import DataLoader
import torch_geometric.transforms as T
import warnings
warnings.filterwarnings("ignore")

class HeteroGraphsGenerator():
    def __init__(self, database, CANT, all_graphs, train_sampled_timestamps, val_sampled_timestamps,
                 test_sampled_timestamps):
        self.database = database
        self.cant = CANT
        self.num_vp_obj = self.cant

        self.train_sampled_timestamps = train_sampled_timestamps
        self.val_sampled_timestamps = val_sampled_timestamps
        self.test_sampled_timestamps = test_sampled_timestamps
        self.get_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()

        self.all_graphs = all_graphs

        # Creates a variety of dictionaries of relationships between objects
        self.preprocessing_steps()

        # # Creates lists of items, packages and orders and their delivery times
        # self.response_generation()

    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.output_path = db_configs[self.database]['ev_output_path']
        self.ocel_path = db_configs[self.database]['ocel_path']
        self.pd_df = pd.read_csv(self.output_path)

    def preprocessing_steps(self):
        # For each order finds its start time and end time and orders them in relation to which process
        # finished first
        active_orders = []
        for i in range(self.num_vp_obj):
            temp = self.pd_df[self.pd_df['vwpnt_id'] == i + 1]
            active_orders.append([i + 1, temp.iloc[0, 2], temp.iloc[-1, 2]])

        self.pd_active_orders = pd.DataFrame(active_orders)
        self.pd_active_orders.sort_values(by=2, inplace=True)

        # Create a dictionary of delivery times
        self.active_orders_dict = {}
        for idx in range(1, self.num_vp_obj + 1):
            self.active_orders_dict[idx] = self.pd_df[self.pd_df['vwpnt_id'] == idx].iloc[-1, 2]

    def generate_graphs(self):
        y_train = []
        mask_y = []
        print("Generating Heterogeneous graphs...")

        train_graphs = []
        train_graphs_sg = []
        print('Train:')
        for idx, timestamp in enumerate(self.train_sampled_timestamps):
            if idx % int(len(self.train_sampled_timestamps) / 5) == 0:
                print(int(idx * 20 / int(len(self.train_sampled_timestamps) / 5)), '%')
            print(f'IDX: {idx}/ Timestamp: {timestamp}')
            # temp = [merged_graph, [order, item and package time differences and mask], single graphs]
            # temp = self.builder(timestamp)
            # train_graphs.append(self.hetero_converter(temp[0], temp[1]))
            # train_graphs_sg.extend(self.hetero_converter_sg(temp[-1], temp[1], timestamp))