import sqlite3
from dataclasses import replace

import pandas as pd
import numpy as np
import copy
from torch_geometric.data import HeteroData, Data
import torch
from torch_geometric.loader import DataLoader
import torch_geometric.transforms as T
import json
import warnings
import os
import sup_funcs as sf
warnings.filterwarnings("ignore")
import pandas as pd
import datetime as dt
import ast

class HeteroGraphsGenerator:
    def __init__(self, database, cant, train_sample, val_sample, test_sample):
        self.database = database
        self.cant = cant
        self.num_vp_obj = self.cant

        self.train_sample = train_sample
        self.val_sample = val_sample
        self.test_sample = test_sample

        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        self.pd_df = pd.read_csv(self.path_dict['ev_log_path'])
        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()

        # Open Relevant Files
        with open(f'{self.path_dict["graph_output_path"]}ocel.csv') as csv_file:
            self.ocel_df = pd.read_csv(csv_file)

        with open(f'{self.path_dict["graph_output_path"]}all_kpis.csv') as csv_file:
            self.all_kpis = pd.read_csv(csv_file)

        with open(f'{self.path_dict["graph_output_path"]}edges.csv') as csv_file:
            self.edges = pd.read_csv(csv_file)

        with open(f'{self.path_dict["graph_output_path"]}ev_log.csv') as csv_file:
            self.ev_log = pd.read_csv(csv_file)

        # Open relevant files
        with open(f'{self.path_dict["graph_output_path"]}tensor_dict.json') as json_file:
            self.tensor_dict = json.load(json_file)

        # Creates a variety of dictionaries of relationships between objects
        self.pd_active_orders, self.active_orders_dict = self.preprocessing_steps()

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

    def get_learning_set(self, sample):
        set_ys = []
        set_graphs = []
        num_order = 95

        for process in sample:
            # Go row for row obtaining the relevant values
            start_time = self.ev_log[self.ev_log['vwpnt_id'] == process]['timestamp'].values[0]
            end_time = self.ev_log[self.ev_log['vwpnt_id'] == process]['timestamp'].values[-1]
            active_df = self.ocel_df[self.ocel_df['vwpnt_id'] == process]
            active_df = active_df[active_df['timestamp'] >= start_time]
            active_df = active_df[active_df['timestamp'] < end_time]
            edges = self.edges[self.edges['vwpnt_id'] == process]
            edges = edges[edges['timestamp'] <= end_time]
            y_df = self.all_kpis[self.all_kpis['viewpoint_id'] == process]
            ys = y_df['kpi_val'].to_list()

            # Construct the collection of prefixes that make up the training sample
            active_events = []
            active_graph = {}
            cnt = 0
            for i, row in active_df.iterrows():
                last_event = True if cnt == len(active_df) - 1 else False
                # Add Events
                ev_type = ast.literal_eval(row['ev_type'])
                ev_id = row['ev_id']
                active_events.append(ev_type)
                active_graph['Events'] = active_events

                # Add nodes
                ev_cols = ['ev_id', 'ev_type', 'ev_idx', 'timestamp', 'vwpnt_id']
                cols = [col for col in active_df.columns if col not in ev_cols]
                cols = [col for col in cols if "::idx" in col]
                for col in cols:
                    col_attr = col.replace("idx", "attributes")
                    col_name = col.replace("::idx", "")
                    attrs = ast.literal_eval(row[col_attr])
                    active_graph[col_name] = attrs

                # Add edges
                active_edges = edges.loc[edges['ev_id'] == ev_id]

                cols = [col for col in active_edges.columns if col not in ev_cols]
                for col in cols:
                    edge = active_edges[col].values[0]
                    edge = ast.literal_eval(edge)
                    active_graph[col] = edge

                y_val = ys[cnt]
                cnt += 1

                # Create heterogeneous graph
                data = HeteroData()

                # Add the nodes
                edge_list = []
                for key in active_graph.keys():
                    if "_to_" in key and key:
                        edge_list.append(key)
                    else:
                        # Obtains the length of each node to ensure proper reshape
                        ob_len = self.tensor_dict[key]
                        try:
                            data[key].x = torch.tensor(active_graph[key], dtype=torch.float32).reshape(-1, ob_len)
                        except TypeError:
                            print(key)
                            print(active_graph[key])
                for edge in edge_list:
                    split = edge.split("_to_")
                    data[split[0], 'to', split[1]].edge_index = torch.tensor(active_graph[edge],
                                                                             dtype=torch.int64).reshape(2, -1)
                # Adds the kpi values for the kpi object
                kpi_ob = self.path_dict['kpi_viewpoint']

                if self.path_dict['kpi_type'] == 0:
                    data[kpi_ob].y = torch.tensor(y_val, dtype=torch.float32).reshape(-1, 1)
                    data[kpi_ob].mask = torch.tensor(True, dtype=torch.bool).reshape(-1, 1)

                else:
                    data[kpi_ob].y = torch.tensor([y_val], dtype=torch.long)
                    data[kpi_ob].mask = torch.tensor([True], dtype=torch.bool)

                data[kpi_ob].id = torch.tensor(process, dtype=torch.float32).reshape(-1, 1)
                data[kpi_ob].last_event = torch.tensor(last_event, dtype=torch.bool).reshape(-1, 1)

                # Add return indexes to all edges
                data = T.ToUndirected()(data)
                set_graphs.append(data)
        return set_graphs

    def trace_kpi(self):
        train_graphs_sg = []
        val_graphs_sg = []
        test_graphs_sg = []

        train_graphs_sg.extend(self.get_learning_set(self.train_sample))
        val_graphs_sg.extend(self.get_learning_set(self.val_sample))
        test_graphs_sg.extend(self.get_learning_set(self.test_sample))

        # Loading Heterogeneous datasets
        print("Saving heterographs...")
        train_loader_sg = DataLoader(train_graphs_sg, batch_size=len(train_graphs_sg), shuffle=True)
        val_loader_sg = DataLoader(val_graphs_sg, batch_size=len(val_graphs_sg))
        test_loader_sg = DataLoader(test_graphs_sg, batch_size=len(test_graphs_sg))

        graphs = [data for data in train_loader_sg.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/train_graphs_sg.pt")

        graphs = [data for data in val_loader_sg.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/val_graphs_sg.pt")

        graphs = [data for data in test_loader_sg.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/test_graphs_sg.pt")
        print("Done!")