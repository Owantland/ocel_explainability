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
    def __init__(self, database, cant, all_graphs, all_timestamps, all_idx, all_kpis,
                 train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps):
        self.database = database
        self.cant = cant
        self.num_vp_obj = self.cant

        self.train_sampled_timestamps = train_sampled_timestamps
        self.val_sampled_timestamps = val_sampled_timestamps
        self.test_sampled_timestamps = test_sampled_timestamps

        self.get_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()

        self.all_graphs = all_graphs
        self.all_timestamps = all_timestamps
        self.all_idx = all_idx
        self.all_kpis = all_kpis

        # Creates a variety of dictionaries of relationships between objects
        self.pd_active_orders, self.active_orders_dict = self.preprocessing_steps()

        # # Creates lists of items, packages and orders and their delivery times
        # self.response_generation()

    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.output_path = db_configs[self.database]['ev_output_path']
        self.ocel_path = db_configs[self.database]['ocel_path']
        self.kpis = db_configs[self.database]['kpis']
        self.pd_df = pd.read_csv(self.output_path)

    def preprocessing_steps(self):
        # For each order finds its start time and end time and orders them in relation to which process
        # finished first
        active_orders = []
        for i in range(self.num_vp_obj):
            temp = self.pd_df[self.pd_df['vwpnt_id'] == i + 1]
            strt_time = temp.iloc[0, 2]
            end_time = temp.iloc[-1, 2]
            ob_id = temp.iloc[0, 3]
            active_orders.append([ob_id, strt_time, end_time])

        pd_active_orders = pd.DataFrame(active_orders)
        pd_active_orders.sort_values(by=2, inplace=True)

        # Create a dictionary of delivery times
        active_orders_dict = {}
        for idx in range(1, self.num_vp_obj + 1):
            delivery_time = self.pd_df[self.pd_df['vwpnt_id'] == idx].iloc[-1, 2]
            ob_id = self.pd_df[self.pd_df['vwpnt_id'] == idx].iloc[0, 3]
            active_orders_dict[ob_id] = delivery_time
        return pd_active_orders, active_orders_dict


    def builder(self, timestamp):
        num_order = 22
        # Get a list of processes that begin before the timestamp and finish after the timestamp
        active_orders = self.pd_active_orders[(self.pd_active_orders[1] <= timestamp) &
                                              (self.pd_active_orders[2] >= timestamp)][0]

        # Return the graphs for only the active orders
        active_graphs = [self.all_graphs[(self.all_timestamps <= timestamp) & (self.all_idx == order)][-1] for order
                         in active_orders]
        active_graphs = [copy.deepcopy(graph) for graph in active_graphs]

        # Obtain the Y values for each item in the KPI section
        current_time = pd.to_datetime(timestamp)
        y_vals = {}
        for order in active_orders:
            kpi_df = self.all_kpis[(self.all_kpis['viewpoint_id'] == order)]
            for kpi in self.kpis.keys():
                try:
                    len(y_vals[kpi]) > 0
                except KeyError:
                    y_vals[kpi] = {}
                kpi_df = kpi_df[kpi_df['kpi_type'] == kpi]
                kpi_ob_types = self.kpis[kpi]
                for ob_type in kpi_ob_types:
                    try:
                        len(y_vals[kpi][ob_type]) > 0
                    except KeyError:
                        y_vals[kpi][ob_type] = []

                    kpi_ts = kpi_df[kpi_df['ob_type'] == ob_type]['timestamp'].values
                    kpi_ts = [pd.to_datetime(ts) - current_time for ts in kpi_ts]
                    # kpi_ts = [ts.total_seconds() for ts in kpi_ts]
                    active_graph = self.all_graphs[(self.all_timestamps <= timestamp) & (self.all_idx == order)][-1]
                    try:
                        ob_cnt = len(active_graph[ob_type])
                        y = kpi_ts[:ob_cnt]
                        y_vals[kpi][ob_type].extend(y)

                        if order == num_order:
                            print(f'For {order} at timestamp {current_time} the kpi for {ob_type} time is {kpi_ts[:ob_cnt]}')
                            # print(f'Active Graph: {self.all_graphs[(self.all_timestamps <= timestamp) & (self.all_idx == num_order)][-1]}')
                    except KeyError:
                        pass
        # y_masks = {}
        # for kpi in y_vals.keys():
        #     y_masks[kpi] = {}
        #     for ob_type in y_vals[kpi].keys():
        #         y = y_vals[kpi][ob_type]
        #         y_vals[kpi][ob_type] = np.array(y)
        #         y_masks[kpi][ob_type] = np.array(y) > 0
        # return y_vals, y_masks, active_graphs

    def generate_graphs(self):
        y_train = []
        mask_y = []
        print("Generating Heterogeneous graphs...")

        train_graphs = []
        train_graphs_sg = []
        print('Train:')
        for idx, timestamp in enumerate(self.train_sampled_timestamps):
            # if idx % int(len(self.train_sampled_timestamps) / 5) == 0:
            #     print(int(idx * 20 / int(len(self.train_sampled_timestamps) / 5)), '%')
            # print(f'IDX: {idx}/ Timestamp: {timestamp}')
            self.builder(timestamp)
            # temp = self.builder(timestamp)
            # train_graphs.append(self.hetero_converter(temp[0], temp[1]))
            # train_graphs_sg.extend(self.hetero_converter_sg(temp[-1], temp[1], timestamp))