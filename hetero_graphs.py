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

        # Open relevant files
        with open(f'{self.path_dict["graph_output_path"]}tensor_dict.json') as json_file:
            self.tensor_dict = json.load(json_file)

        # with open(f'{self.path_dict["graph_output_path"]}all_graphs.json') as json_file:
        #     all_graphs = json.load(json_file)
        #     self.all_graphs = np.array(all_graphs)
        #
        # with open(f'{self.path_dict["graph_output_path"]}all_timestamps.json') as json_file:
        #     all_timestamps = json.load(json_file)
        #     self.all_timestamps = np.array(all_timestamps)
        #
        # with open(f'{self.path_dict["graph_output_path"]}all_idx.json') as json_file:
        #     all_idx = json.load(json_file)
        #     self.all_idx = np.array(all_idx)
        #
        # with open(f'{self.path_dict["graph_output_path"]}all_kpis.csv') as csv_file:
        #     self.all_kpis = pd.read_csv(csv_file)

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
                    vwpnt_y = [float(x) for x in vwpnt_y[0]]
                except TypeError:
                    vwpnt_y = [float(x) for x in vwpnt_y]

                if self.path_dict['kpi_type'] == 0:
                    y = torch.tensor(vwpnt_y, dtype=torch.float32)
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
            vwpnt_id = graph[self.path_dict['viewpoint']][-1]
            vwpnt_val = graph[self.path_dict['viewpoint']][0][0]
            vwpnt_ys = y_vals[y_vals['vwpnt_id'] == vwpnt_id]

            # Initiate the tensor with the viewpoint object
            data = HeteroData()
            data[self.path_dict['viewpoint']].x = torch.tensor(vwpnt_val, dtype=torch.float32).reshape(-1, 1)
            data[self.path_dict['viewpoint']].id = torch.tensor(vwpnt_id, dtype=torch.long)

            print(vwpnt_ys)
            print(vwpnt_ys[['y_val', 'y_mask']].iloc[idx])
            # Adds the kpi values for the kpi object
            kpi_ob = self.path_dict['kpi_viewpoint']
            # Assign the y and mask values related to the selected viewpoint and object type
            # try:
            #     vwpnt_y = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_val'].to_numpy()
            #     vwpnt_mask = vwpnt_ys[y_vals['ob_type'] == kpi_ob]['y_mask'].to_numpy()

        #         try:
        #             vwpnt_y = [float(x) for x in vwpnt_y[0]]
        #             vwpnt_mask = vwpnt_mask[0].tolist()
        #         except TypeError:
        #             vwpnt_y = [float(x) for x in vwpnt_y]
        #
        #         if self.path_dict['kpi_type'] == 0:
        #             data[kpi_ob].y = torch.tensor(vwpnt_y, dtype = torch.float32).reshape(-1,1)
        #             data[kpi_ob].mask = torch.tensor(vwpnt_mask, dtype = torch.bool).reshape(-1,1)
        #         else:
        #             data[kpi_ob].y = torch.tensor(vwpnt_y, dtype=torch.long)
        #             data[kpi_ob].mask = torch.tensor(vwpnt_mask, dtype=torch.bool)
        #     except IndexError:
        #         pass
        #
        #     # Add the remaining nodes
        #     edges = []
        #     for key in graph.keys():
        #         if key != self.path_dict['viewpoint']:
        #             # Edges are added after object nodes
        #             if "_to_" in key and key:
        #                 edges.append(key)
        #             else:
        #                 # Obtains the length of each node to ensure proper reshape
        #                 ob_len = self.tensor_dict[key]
        #                 data[key].x = torch.tensor(graph[key], dtype=torch.float32).reshape(-1, ob_len)
        #     for edge in edges:
        #         split = edge.split("_to_")
        #         data[split[0], 'to', split[1]].edge_index = torch.tensor(graph[edge],dtype=torch.int64).reshape(2, -1)
        #
        #     # Add return indexes to all edges
        #     data = T.ToUndirected()(data)
        #     all_graphs.append(data)
        # return all_graphs

    def get_learning_set(self, sample):
        set_ys = []
        set_graphs = []
        num_order = 95

        for process in sample:
            # Go row for row obtaining the relevant values
            active_df = self.ocel_df[self.ocel_df['vwpnt_id'] == process]
            y_df = self.all_kpis[self.all_kpis['viewpoint_id'] == process]
            ys = y_df['kpi_val'].to_list()

            # Construct the collection of prefixes that make up the training sample
            active_events = []
            active_graph = {}
            cnt = 0
            for i, row in active_df.iterrows():
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
                active_edges = self.edges.loc[self.edges['ev_id'] == ev_id]
                active_edges = active_edges[:1]

                cols = [col for col in active_edges.columns if col not in ev_cols]
                for col in cols:
                    edge = active_edges[col].values[0]
                    edge = ast.literal_eval(edge)
                    active_graph[col] = edge

                try:
                    y_val = ys[cnt]
                except IndexError:
                    break
                cnt += 1

                # Create heterogeneous graph
                data = HeteroData()

                # Add the nodes
                edges = []
                for key in active_graph.keys():
                    if "_to_" in key and key:
                        edges.append(key)
                    else:
                        # Obtains the length of each node to ensure proper reshape
                        ob_len = self.tensor_dict[key]
                        try:
                            data[key].x = torch.tensor(active_graph[key], dtype=torch.float32).reshape(-1, ob_len)
                        except TypeError:
                            print(key)
                            print(active_graph[key])
                for edge in edges:
                    split = edge.split("_to_")
                    data[split[0], 'to', split[1]].edge_index = torch.tensor(active_graph[edge],
                                                                             dtype=torch.int64).reshape(2, -1)
                # Adds the kpi values for the kpi object
                kpi_ob = self.path_dict['kpi_viewpoint']

                if self.path_dict['kpi_type'] == 0:
                    data[kpi_ob].y = torch.tensor(y_val, dtype=torch.float32).reshape(-1, 1)
                    data[kpi_ob].mask = torch.tensor(True, dtype=torch.bool).reshape(-1, 1)
                else:
                    data[kpi_ob].y = torch.tensor(y_val, dtype=torch.long)
                    data[kpi_ob].mask = torch.tensor(True, dtype=torch.bool)

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

        # # # Loading Homogeneous datasets
        # # # DataLoader lets us use the list of data objects as a batch for training
        # # train_loader_hom = DataLoader(train_graphs_hom, batch_size=len(train_graphs_hom), shuffle=True)
        # # val_loader_hom = DataLoader(val_graphs_hom, batch_size=len(val_graphs_hom))
        # # test_loader_hom = DataLoader(test_graphs_hom, batch_size=len(test_graphs_hom))
        # #
        # # print("Saving homographs...")
        # # graphs = [data for data in train_loader_hom.dataset]
        # # torch.save(graphs, f"{self.path_dict['pytorch_path']}/train_graphs_hom.pt")
        # #
        # # graphs = [data for data in val_loader_hom.dataset]
        # # torch.save(graphs, f"{self.path_dict['pytorch_path']}/val_graphs_hom.pt")
        # #
        # # graphs = [data for data in test_loader_hom.dataset]
        # # torch.save(graphs, f"{self.path_dict['pytorch_path']}/test_graphs_hom.pt")
        # # print("Done!")

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