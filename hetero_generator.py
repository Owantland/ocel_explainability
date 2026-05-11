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
            strt_time = temp.iloc[0, 2]
            end_time = temp.iloc[-1, 2]
            ob_id = temp.iloc[0, 4]
            active_orders.append([ob_id, strt_time, end_time])

        self.pd_active_orders = pd.DataFrame(active_orders)
        self.pd_active_orders.sort_values(by=2, inplace=True)

        # Create a dictionary of delivery times
        self.active_orders_dict = {}
        for idx in range(1, self.num_vp_obj + 1):
            delivery_time = self.pd_df[self.pd_df['vwpnt_id'] == idx].iloc[-1, 2]
            ob_id = self.pd_df[self.pd_df['vwpnt_id'] == idx].iloc[0, 4]
            self.active_orders_dict[ob_id] = delivery_time

    def builder(self, timestamp):
        # Get a list of orders that begin delivery before the timestamp and finish after the timestamp
        # can be complicated by items related to one order that are sent in a later package
        active_orders = self.pd_active_orders[(self.pd_active_orders[1] <= timestamp) & (self.pd_active_orders[2] >= timestamp)][0]

        # Runs a query for finding the products shipped closest to the timestamp and obtains their weight and price
        # Here we can create a function that works for any time-dependent values aside from price.

        # Return the graphs for only the active orders
        active_graphs = [self.all_graphs[order]['graph'] for order in active_orders]

        current_time = pd.to_datetime(timestamp)
        # Calculates the time difference between the selected timestamp and the final delivery time for a package
        # assigned to the order
        # print(f'Active order time: {self.active_orders_dict}')
        y_order = [pd.to_datetime(self.active_orders_dict[order]) - current_time for order in active_orders]
        y_order = [a.total_seconds() for a in y_order]
        mask_order = np.array(y_order) > 0


        # # Calculates the time difference between the selected timestamp and the final delivery time for the package
        # # carrying the item
        # # print(f'Active item time: {self.active_item_dict}')
        # y_item = [pd.to_datetime(self.active_item_dict[item]) - current_time for item in active_items]
        # y_item = [a.total_seconds() for a in y_item]
        # mask_item = np.array(y_item) > 0
        #
        # # Calculates the time difference between the selected timestamp and the final delivery time for the package
        # # print(f'Active package time: {self.active_packages_dict}')
        # y_package = [pd.to_datetime(self.active_packages_dict[package]) - current_time for package in active_packages]
        # y_package = [a.total_seconds() for a in y_package]
        # mask_package = np.array(y_package) > 0
        # return merged_graph, [(y_order, mask_order), (y_item, mask_item), (y_package, mask_package)], single_graphs

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
            temp = self.builder(timestamp)
            # train_graphs.append(self.hetero_converter(temp[0], temp[1]))
            # train_graphs_sg.extend(self.hetero_converter_sg(temp[-1], temp[1], timestamp))