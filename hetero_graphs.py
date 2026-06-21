import sqlite3
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

        # Open relevant files
        with open(f'{self.path_dict["graph_output_path"]}tensor_dict.json') as json_file:
            self.tensor_dict = json.load(json_file)

        with open(f'{self.path_dict["graph_output_path"]}all_graphs.json') as json_file:
            all_graphs = json.load(json_file)
            self.all_graphs = np.array(all_graphs)

        with open(f'{self.path_dict["graph_output_path"]}all_timestamps.json') as json_file:
            all_timestamps = json.load(json_file)
            self.all_timestamps = np.array(all_timestamps)

        with open(f'{self.path_dict["graph_output_path"]}all_idx.json') as json_file:
            all_idx = json.load(json_file)
            self.all_idx = np.array(all_idx)

        with open(f'{self.path_dict["graph_output_path"]}all_kpis.csv') as csv_file:
            self.all_kpis = pd.read_csv(csv_file)

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

    def homogeneous_loader(self, graph_set, y_set, timestamp):
        """
        :param graph_set:
        :param y_set:
        :return: homogeneous graph of the event log
        """

        all_graphs = []
        y_vals = y_set[y_set['kpi_id'] == self.path_dict['kpi_event']]

        for idx, graph in enumerate(graph_set):
            vwpnt_id = graph[self.path_dict['viewpoint']][0][-1]
            vwpnt_val = graph[self.path_dict['viewpoint']][0][0]
            vwpnt_ys = y_vals[y_vals['vwpnt_id'] == vwpnt_id]

            ob_len = self.tensor_dict['Events']
            x = torch.tensor(graph['Events'], dtype=torch.float32).reshape(-1, ob_len)
            edge_index = torch.tensor(graph['Events_to_Events'],dtype=torch.int64)
            vwpnt_tensor = torch.tensor(vwpnt_id,dtype=torch.long)
            timestamp = pd.Timestamp(timestamp)
            start = dt.datetime(1970,1,1)
            timestamp_tensor = (timestamp-start).total_seconds()
            timestamp_tensor = torch.tensor(timestamp_tensor,dtype=torch.float)

            # Adds the kpi values for the kpi objects
            kpi_ob = self.path_dict['kpi_viewpoint']
            # Assign the y and mask values related to the selected viewpoint and object type
            try:
                vwpnt_y = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_val'].to_numpy()
                try:
                    vwpnt_y = [int(x) for x in vwpnt_y[0]]
                except TypeError:
                    vwpnt_y = [int(x) for x in vwpnt_y]

                if self.path_dict['kpi_type'] == 0:
                    y = torch.tensor(vwpnt_y, dtype=torch.float)
                else:
                    y = torch.tensor(vwpnt_y, dtype=torch.long)

                if len(graph['Events_to_Events'][0]) > 0:
                    data = Data(x=x, y=y, edge_index=edge_index, vwpnt_id=vwpnt_tensor, timestamp=timestamp_tensor)
                    all_graphs.append(data)
            except IndexError:
                pass
        return all_graphs

    def tensor_loader(self, graph_set, y_set):
        all_graphs = []
        y_vals = y_set[y_set['kpi_id'] == self.path_dict['kpi_event']]

        for idx, graph in enumerate(graph_set):
            vwpnt_id = graph[self.path_dict['viewpoint']][0][-1]
            vwpnt_val = graph[self.path_dict['viewpoint']][0][0]
            vwpnt_ys = y_vals[y_vals['vwpnt_id'] == vwpnt_id]

            # Initiate the tensor with the viewpoint object
            data = HeteroData()
            data[self.path_dict['viewpoint']].x = torch.tensor(vwpnt_val, dtype=torch.float32).reshape(-1, 1)

            # Adds the kpi values for the kpi objects
            kpi_ob = self.path_dict['kpi_viewpoint']
            # Assign the y and mask values related to the selected viewpoint and object type
            try:
                vwpnt_y = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_val'].to_numpy()
                vwpnt_mask = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_mask'].to_numpy()

                try:
                    vwpnt_y = [int(x) for x in vwpnt_y[0]]
                    vwpnt_mask = vwpnt_mask[0].tolist()
                except TypeError:
                    vwpnt_y = [int(x) for x in vwpnt_y]

                # Assign values to the proper tensor
                data[kpi_ob].y = torch.tensor(vwpnt_y, dtype=torch.long)
                data[kpi_ob].mask = torch.tensor(vwpnt_mask, dtype=torch.bool)

            except IndexError:
                pass

            # Add the remaining nodes
            edges = []
            for key in graph.keys():
                if key != self.path_dict['viewpoint']:
                    # Edges are added after object nodes
                    if "_to_" in key and key:
                        edges.append(key)
                    else:
                        # Obtains the length of each node to ensure proper reshape
                        ob_len = self.tensor_dict[key]
                        data[key].x = torch.tensor(graph[key], dtype=torch.float32).reshape(-1, ob_len)
            for edge in edges:
                split = edge.split("_to_")
                data[split[0], 'to', split[1]].edge_index = torch.tensor(graph[edge],dtype=torch.int64).reshape(2, -1)
            # Add return indexes to all edges
            data = T.ToUndirected()(data)
            all_graphs.append(data)
        return all_graphs

    def get_learning_set(self, timestamp):
        set_ys = []
        set_graphs = []
        num_order = 95

        """
            Identify which processes are active at the chosen time by checking whether they started before the timestamp
            and will end after the timestamp.
            For each of those active processes:
                * Obtain the relevant KPI value or values
                * Obtain the prefix subgraph for the timestamp
                * Add each of the obtained values to the list of all KPIs and the list of all graphs to create the 
                  pyg dataset.
        """
        active_processes = self.pd_active_orders[(self.pd_active_orders[1] <= timestamp) &
                                              (self.pd_active_orders[2] >= timestamp)][0]
        for process in active_processes:
            y_df = self.all_kpis[self.all_kpis['timestamp'] <= timestamp]
            y_df = y_df[y_df['viewpoint_id'] == process]
            y = y_df['kpi_val'].values[-1]
            active_graph = self.all_graphs[(self.all_timestamps <= timestamp) & (self.all_idx == process)][-1]
            kpi = self.path_dict['kpi_event']

            ob_ids = [x[0] for x in active_graph[self.path_dict['kpi_viewpoint']]]
            if len(ob_ids) > 0:
                ys = []
                for ob_id in ob_ids:
                    ys.append(y)
                set_ys.append([kpi, process, self.path_dict['kpi_viewpoint'], ys])
            else:
                set_ys.append([kpi, process, self.path_dict['kpi_viewpoint'], []])
            set_graphs.append(active_graph)

        set_graphs = [copy.deepcopy(graph) for graph in set_graphs]
        # Clean up the unnecessary identifiers in each object
        for graph in set_graphs:
            for key in graph.keys():
                if "_to_" not in key and key != 'Events':
                    graph[key] = [x[:-1] for x in graph[key]]
                    for index, x in enumerate(graph[key]):
                        if len(x) > 1:
                            graph[key][index] = x[1:]

        # Create the Y value dataframe
        for idx, y_val in enumerate(set_ys):
            kpi_id = y_val[0]
            vwpnt_id = y_val[1]
            ob_type = y_val[2]
            vals = np.array(y_val[3][0])
            mask = vals >= 0
            set_ys[idx] = [kpi_id, vwpnt_id, ob_type, vals, mask]
        set_ys = pd.DataFrame(set_ys, columns=['kpi_id', 'vwpnt_id', 'ob_type', 'y_val', 'y_mask'])
        return set_ys, set_graphs

    def trace_kpi(self):
        train_graphs_sg = []
        train_graphs_hom = []
        for timestamp in self.train_sample:
            train_ys, train_graphs = self.get_learning_set(timestamp)
            train_graphs_hom.extend(self.homogeneous_loader(train_graphs, train_ys, timestamp))
            train_graphs_sg.extend(self.tensor_loader(train_graphs, train_ys))

        val_graphs_sg = []
        val_graphs_hom = []
        for timestamp in self.val_sample:
            val_ys, val_graphs = self.get_learning_set(timestamp)
            val_graphs_sg.extend(self.tensor_loader(val_graphs, val_ys))
            val_graphs_hom.extend(self.homogeneous_loader(val_graphs, val_ys, timestamp))

        test_graphs_sg = []
        test_graphs_hom = []
        for timestamp in self.test_sample:
            test_ys, test_graphs = self.get_learning_set(timestamp)
            test_graphs_sg.extend(self.tensor_loader(test_graphs, test_ys))
            test_graphs_hom.extend(self.homogeneous_loader(test_graphs, test_ys, timestamp))

        # Loading Heterogeneous datasets
        # DataLoader lets us use the list of data objects as a batch for training
        train_loader_sg = DataLoader(train_graphs_sg, batch_size=len(train_graphs_sg), shuffle=True)
        val_loader_sg = DataLoader(val_graphs_sg, batch_size=len(val_graphs_sg))
        test_loader_sg = DataLoader(test_graphs_sg, batch_size=len(test_graphs_sg))

        print("Saving heterographs...")
        graphs = [data for data in train_loader_sg.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/train_graphs_sg.pt")

        graphs = [data for data in val_loader_sg.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/val_graphs_sg.pt")

        graphs = [data for data in test_loader_sg.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/test_graphs_sg.pt")
        print("Done!")

        # Loading Homogeneous datasets
        # DataLoader lets us use the list of data objects as a batch for training
        train_loader_hom = DataLoader(train_graphs_hom, batch_size=len(train_graphs_hom), shuffle=True)
        val_loader_hom = DataLoader(val_graphs_hom, batch_size=len(val_graphs_hom))
        test_loader_hom = DataLoader(test_graphs_hom, batch_size=len(test_graphs_hom))
        exp_loader_hom = DataLoader(train_graphs_hom, batch_size=len(train_graphs_hom))

        print("Saving homographs...")
        graphs = [data for data in train_loader_hom.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/train_graphs_hom.pt")

        graphs = [data for data in val_loader_hom.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/val_graphs_hom.pt")

        graphs = [data for data in test_loader_hom.dataset]
        torch.save(graphs, f"{self.path_dict['pytorch_path']}/test_graphs_hom.pt")

        torch.save(exp_loader_hom, f"{self.path_dict['pytorch_path']}/exp_graphs_hom.pt")
        print("Done!")