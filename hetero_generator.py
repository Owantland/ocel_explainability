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
    def __init__(self, database, cant, all_graphs, all_timestamps, all_idx, all_kpis, tensor_dict,
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
        self.tensor_dict = tensor_dict

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
        self.viewpoint = db_configs[self.database]['viewpoint']
        self.to_encode = db_configs[self.database]['encoding']

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

    def tensor_maker(self, single_graphs, y_vals, timestamp):
        all_graphs = []
        kpi_obs = self.kpis['PackageDelivered']
        y_vals = y_vals[y_vals['kpi_id'] == 'PackageDelivered']

        for idx, graph in enumerate(single_graphs):
            vwpnt_id = graph[self.viewpoint][0][-1]
            vwpnt_val = graph[self.viewpoint][0][0]
            vwpnt_ys = y_vals[y_vals['vwpnt_id'] == vwpnt_id]

            # Initiate the tensor with the viewpoint object
            data = HeteroData()
            data[self.viewpoint].x = torch.tensor(vwpnt_val, dtype=torch.float32).reshape(-1, 1)

            # Adds the kpi values for the kpi objects
            for kpi_ob in kpi_obs:
                # Assign the y and mask values related to the selected viewpoint and object type
                try:
                    vwpnt_y = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_val'].to_numpy()
                    vwpnt_mask = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_mask'].to_numpy()

                    vwpnt_y = [float(x) for x in vwpnt_y[0]]
                    vwpnt_mask = vwpnt_mask[0].tolist()

                    # Assign values to the proper tensor
                    data[kpi_ob].y = torch.tensor(vwpnt_y, dtype=torch.float32).reshape(-1, 1)
                    data[kpi_ob].mask = torch.tensor(vwpnt_mask, dtype=torch.bool).reshape(-1, 1)

                    # print(f'For viewpoint {vwpnt_id} at {timestamp} the y values for {kpi_ob} with graph {ob_graph} are \n{vwpnt_y} and {vwpnt_mask}')
                except IndexError:
                    pass

            # Add the remaining nodes
            edges = []
            for key in graph.keys():
                if key != self.viewpoint:
                    # Edges are added after object nodes
                    if "_to_" in key and key != 'event_to_event':
                        edges.append(key)
                    else:
                        if key != 'event_to_event':
                            # Obtains the length of each node to ensure proper reshape
                            ob_len = self.tensor_dict[key]
                            data[key].x = torch.tensor(graph[key], dtype=torch.float32).reshape(-1, ob_len)
            for edge in edges:
                split = edge.split("_to_")
                if split[1] != 'event':
                    data[split[0], 'to', split[1]].edge_index = torch.tensor(graph[edge],
                                                                             dtype=torch.int64).reshape(2, -1)
                else:
                    split[1] = 'Events'
                    data[split[0], 'to', split[1]].edge_index = torch.tensor(graph[edge],
                                                                             dtype=torch.int64).reshape(2, -1)
            data['Events', 'to', 'Events'].edge_index = torch.tensor(graph['event_to_event'], dtype=torch.int64).reshape(
                2, -1)
            data = T.ToUndirected()(data)
            all_graphs.append(data)
        return all_graphs

    def builder(self, timestamp):
        num_order = 67
        # Get a list of processes that begin before the timestamp and finish after the timestamp
        active_orders = self.pd_active_orders[(self.pd_active_orders[1] <= timestamp) &
                                              (self.pd_active_orders[2] >= timestamp)][0]

        # Return the graphs for only the active orders
        active_graphs = [self.all_graphs[(self.all_timestamps <= timestamp) & (self.all_idx == order)][-1] for order
                         in active_orders]
        active_graphs = [copy.deepcopy(graph) for graph in active_graphs]

        # Obtain the Y values for each item in the KPI section
        current_time = pd.to_datetime(timestamp)
        y_vals = []
        for order in active_orders:
            kpi_df = self.all_kpis[(self.all_kpis['viewpoint_id'] == order)]
            for kpi in self.kpis.keys():
                kpi_df = kpi_df[kpi_df['kpi_type'] == kpi]
                kpi_ob_types = self.kpis[kpi]
                for ob_type in kpi_ob_types:
                    kpi_ts = kpi_df[kpi_df['ob_type'] == ob_type]['timestamp'].values
                    kpi_ts = [pd.to_datetime(ts) - current_time for ts in kpi_ts]
                    kpi_ts = [ts.total_seconds() for ts in kpi_ts]
                    active_graph = self.all_graphs[(self.all_timestamps <= timestamp) & (self.all_idx == order)][-1]
                    try:
                        ob_cnt = len(active_graph[ob_type])
                        y = kpi_ts[:ob_cnt]
                        y_vals.append([kpi, order, ob_type, y])
                    except KeyError:
                        pass

        for idx, y_val in enumerate(y_vals):
            kpi_id = y_val[0]
            vwpnt_id = y_val[1]
            ob_type = y_val[2]
            vals = np.array(y_val[3])
            mask = vals > 0
            y_vals[idx] = [kpi_id, vwpnt_id, ob_type, vals, mask]
        y_vals = pd.DataFrame(y_vals, columns=['kpi_id', 'vwpnt_id', 'ob_type', 'y_val', 'y_mask'])
        return y_vals, active_graphs

    def generate_graphs(self):
        print("Generating Heterogeneous graphs...")

        train_graphs_sg = []
        print('Train:')
        for idx, timestamp in enumerate(self.train_sampled_timestamps):
            if idx % int(len(self.train_sampled_timestamps) / 5) == 0:
                print(int(idx * 20 / int(len(self.train_sampled_timestamps) / 5)), '%')
            print(f'IDX: {idx}/ Timestamp: {timestamp}')
            y_vals, graphs = self.builder(timestamp)
            train_graphs_sg.extend(self.tensor_maker(graphs, y_vals, timestamp))

        val_graphs_sg = []
        print('Validation:')
        for idx, timestamp in enumerate(self.val_sampled_timestamps):
            if idx % int(len(self.val_sampled_timestamps) / 5) == 0:
                print(int(idx * 20 / int(len(self.val_sampled_timestamps) / 5)), '%')
            print(f'IDX: {idx}/ Timestamp: {timestamp}')
            y_vals, graphs = self.builder(timestamp)
            val_graphs_sg.extend(self.tensor_maker(graphs, y_vals, timestamp))

        test_graphs_sg = []
        print('Testing:')
        for idx, timestamp in enumerate(self.test_sampled_timestamps):
            if idx % int(len(self.test_sampled_timestamps) / 5) == 0:
                print(int(idx * 20 / int(len(self.test_sampled_timestamps) / 5)), '%')
            print(f'IDX: {idx}/ Timestamp: {timestamp}')
            y_vals, graphs = self.builder(timestamp)
            test_graphs_sg.extend(self.tensor_maker(graphs, y_vals, timestamp))

        # KPI Standardization process
        kpi_obs = self.kpis['PackageDelivered']
        for kpi_ob in kpi_obs:
            y_train = []
            mask_y = []
            for graph in train_graphs_sg:
                try:
                    y_train.extend(graph[kpi_ob]['y'])
                    mask_y.extend(graph[kpi_ob]['mask'].reshape(-1))
                except KeyError:
                    pass

            y_train = [a.item() for a in y_train]
            mask_y = [a.item() for a in mask_y]
            y_train = np.array(y_train)
            mask_y = np.array(mask_y)
            mean = np.mean(y_train[mask_y])
            std = np.std(y_train[mask_y])

            for graphs in [train_graphs_sg, val_graphs_sg, test_graphs_sg]:
                for graph in graphs:
                    try:
                        graph[kpi_ob]['y'] = (graph[kpi_ob]['y'] - mean) / std
                    except KeyError:
                        pass

        # Loading
        train_loader_sg = DataLoader(train_graphs_sg, batch_size=len(train_graphs_sg), shuffle=True)
        val_loader_sg = DataLoader(val_graphs_sg, batch_size=len(val_graphs_sg))
        test_loader_sg = DataLoader(test_graphs_sg, batch_size=len(test_graphs_sg))

        print("Saving heterographs...")
        graphs = [data for data in train_loader_sg.dataset]
        torch.save(graphs, f'files/hetero_structures/train_graphs_sg.pt')

        graphs = [data for data in val_loader_sg.dataset]
        torch.save(graphs, f'files/hetero_structures/val_graphs_sg.pt')

        graphs = [data for data in test_loader_sg.dataset]
        torch.save(graphs, f'files/hetero_structures/test_graphs_sg.pt')
        print("Done!")