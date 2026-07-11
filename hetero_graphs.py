import sqlite3

import pandas as pd
import math
from torch_geometric.data import HeteroData
import torch
from torch_geometric.loader import DataLoader
import torch_geometric.transforms as T
import json
import warnings
import sup_funcs as sf
warnings.filterwarnings("ignore")
import ast


def _load_required_csv(path, produced_by):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Missing required input '{path}' -- expected to be produced by {produced_by}."
        ) from None


def _load_required_json(path, produced_by):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Missing required input '{path}' -- expected to be produced by {produced_by}."
        ) from None


def _remap_events_edge(edge, remap, src_is_events, tgt_is_events):
    """Translate an edge's Events-side indices from their absolute (pre-windowing)
    position -- baked into edges.csv by ocel_generator.py against each process's
    full, untrimmed event sequence -- to their local position within the current
    (start_time, end_time)-filtered active_df. Drops edges whose Events endpoint
    references an excluded (out-of-window) event entirely."""
    src_list, tgt_list = edge
    new_src, new_tgt = [], []
    for s, t in zip(src_list, tgt_list):
        ns = remap.get(s) if src_is_events else s
        nt = remap.get(t) if tgt_is_events else t
        if ns is None or nt is None:
            continue
        new_src.append(ns)
        new_tgt.append(nt)
    return [new_src, new_tgt]


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
        self.pd_df = _load_required_csv(self.path_dict['ev_log_path'], "process_generation.py's get_ev_log()")
        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()

        # Open relevant files -- ocel.csv/edges.csv/tensor_dict.json come from
        # ocel_generator.py; all_kpis.csv/ev_log.csv come from process_generation.py's
        # get_ev_log(). A missing file here means an earlier pipeline stage hasn't run yet.
        graph_output_path = self.path_dict["graph_output_path"]
        self.ocel_df      = _load_required_csv(f'{graph_output_path}ocel.csv', "ocel_generator.py")
        self.all_kpis     = _load_required_csv(f'{graph_output_path}all_kpis.csv', "process_generation.py's get_ev_log()")
        self.edges        = _load_required_csv(f'{graph_output_path}edges.csv', "ocel_generator.py")
        self.ev_log       = _load_required_csv(f'{graph_output_path}ev_log.csv', "process_generation.py's get_ev_log()")
        self.tensor_dict  = _load_required_json(f'{graph_output_path}tensor_dict.json', "ocel_generator.py")

        # Extend in-memory dims for enriched node types (not written back to JSON;
        # hetero_graphs.py always applies these increments at init time).
        self.tensor_dict['Events'] += 6        # +6 temporal features → 17D
        if 'Orders' in self.tensor_dict:
            self.tensor_dict['Orders'] += 3    # +3 aggregate features → 4D

        # Adams et al. gap features: C3 (activity frequency) + O1-ext (object type counts)
        self.n_ev_types = len(ast.literal_eval(self.ocel_df['ev_type'].iloc[0]))
        self.obj_type_order = [
            c.replace('::ids', '')
            for c in self.ocel_df.columns if c.endswith('::ids')
        ]
        self.tensor_dict['Events'] += self.n_ev_types           # C3: +n_ev_types D
        self.tensor_dict['Events'] += len(self.obj_type_order)  # O1-ext: +n_obj_types D
        if 'Orders' in self.tensor_dict:
            self.tensor_dict['Orders'] += 1                     # n_packages: +1D

        # Creates a variety of dictionaries of relationships between objects
        self.pd_active_orders, self.active_orders_dict = self.preprocessing_steps()

    def preprocessing_steps(self):
        # For each order finds its start time and end time and orders them in relation to which process
        # finished first
        active_orders = []
        for i in range(self.num_vp_obj):
            temp = self.pd_df[self.pd_df['vwpnt_id'] == i + 1]
            strt_time = temp.iloc[0]['timestamp']
            end_time = temp.iloc[-1]['timestamp']
            ob_id = temp.iloc[0]['ob_id']
            active_orders.append([ob_id, strt_time, end_time])
        pd_active_orders = pd.DataFrame(active_orders)
        pd_active_orders.sort_values(by=2, inplace=True)

        # Create a dictionary of delivery times, keyed by the object's actual OCEL ID
        # (ev_log.csv's 'ob_id' column) -- NOT 'vwpnt_id' (the integer trace counter), which
        # this previously read by mistake via positional indexing (column 3 is 'vwpnt_id',
        # not 'ob_id', which is column 4). That bug made get_learning_set()'s
        # `elif ob_id in self.active_orders_dict` lookup unable to ever match a real OCEL ID,
        # silently defeating per-instance y-values for non-primary same-type viewpoint nodes.
        active_orders_dict = {}
        for idx in range(1, self.num_vp_obj + 1):
            rows = self.pd_df[self.pd_df['vwpnt_id'] == idx]
            delivery_time = rows.iloc[-1]['timestamp']
            ob_id = rows.iloc[0]['ob_id']
            active_orders_dict[ob_id] = delivery_time
        return pd_active_orders, active_orders_dict

    def get_learning_set(self, sample):
        set_graphs = []

        for process in sample:
            # Go row for row obtaining the relevant values
            start_time = self.ev_log[self.ev_log['vwpnt_id'] == process]['timestamp'].values[0]
            end_time = self.ev_log[self.ev_log['vwpnt_id'] == process]['timestamp'].values[-1]
            active_df = self.ocel_df[self.ocel_df['vwpnt_id'] == process]
            full_event_ids = active_df['ev_id'].tolist()
            active_df = active_df[active_df['timestamp'] < end_time]
            active_df = active_df[active_df['timestamp'] >= start_time]

            # events_idx_remap: absolute (pre-windowing) event position -> local position
            # within the filtered active_df. Needed because Events_to_Events/*_to_Events
            # edge indices in edges.csv were computed by ocel_generator.py against each
            # process's full, untrimmed event sequence -- excluding leading events here
            # shifts every surviving event's position, so edge indices must follow.
            old_idx_of_evid = {eid: pos for pos, eid in enumerate(full_event_ids)}
            new_idx_of_evid = {eid: pos for pos, eid in enumerate(active_df['ev_id'])}
            events_idx_remap = {old_idx_of_evid[eid]: new_idx_of_evid[eid]
                                 for eid in active_df['ev_id']}

            edges = self.edges[self.edges['vwpnt_id'] == process]
            edges = edges[edges['timestamp'] <= end_time]
            edges = edges[edges['timestamp'] >= start_time]
            y_df    = self.all_kpis[self.all_kpis['viewpoint_id'] == process].reset_index(drop=True)
            evlog_t = self.ev_log[self.ev_log['vwpnt_id'] == process].reset_index(drop=True)
            # Map ev_id → kpi_val via positional alignment of ev_log and all_kpis.
            # Events present in ocel.csv but absent from ev_log (e.g. between two deliveries)
            # are computed on the fly from the remaining time to end_time.
            ev_kpi_map = dict(zip(evlog_t['ocel_id'].astype(str),
                                  y_df['kpi_val'].astype(float)))
            end_ts = pd.to_datetime(end_time)

            # Construct the collection of prefixes that make up the training sample
            active_events = []
            active_graph = {}
            cnt = 0
            start_ts = pd.to_datetime(start_time)
            prev_ts = None
            activity_counts = [0] * self.n_ev_types  # C3 accumulator, reset per trace
            for i, row in active_df.iterrows():
                last_event = True if cnt == len(active_df) - 1 else False
                # Build partial event feature; obj_counts (O1-ext) appended after active_graph update
                ev_type = ast.literal_eval(row['ev_type'])
                current_ts = pd.to_datetime(row['timestamp'])
                elapsed_h = (current_ts - start_ts).total_seconds() / 3600.0
                waiting_h = 0.0 if prev_ts is None else (current_ts - prev_ts).total_seconds() / 3600.0
                prev_ts = current_ts
                h_frac = current_ts.hour + current_ts.minute / 60.0
                dow = current_ts.dayofweek
                activity_counts[ev_type.index(1)] += 1  # C3: inclusive count for this event
                ev_feat_partial = ev_type + [
                    elapsed_h, waiting_h,
                    math.sin(2 * math.pi * h_frac / 24.0), math.cos(2 * math.pi * h_frac / 24.0),
                    math.sin(2 * math.pi * dow / 7.0),     math.cos(2 * math.pi * dow / 7.0),
                ] + list(activity_counts)
                ev_id = row['ev_id']

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
                    src_type, tgt_type = col.split('_to_')
                    if src_type == 'Events' or tgt_type == 'Events':
                        edge = _remap_events_edge(edge, events_idx_remap,
                                                   src_type == 'Events', tgt_type == 'Events')
                    active_graph[col] = edge

                # Order aggregate features: n_items, total_weight, n_distinct_products, n_packages
                # Appended to each Orders node's feature vector whenever Orders is in the prefix.
                if 'Orders' in active_graph:
                    items_feats = active_graph.get('Items', [])  # list of [weight, price] per item
                    n_items = float(len(items_feats))
                    total_weight = float(sum(f[0] for f in items_feats)) if items_feats else 0.0
                    n_products = float(len(active_graph.get('Products', [])))
                    n_packages = float(len(active_graph.get('Packages', [])))
                    active_graph['Orders'] = [
                        order_feats + [n_items, total_weight, n_products, n_packages]
                        for order_feats in active_graph['Orders']
                    ]

                # Complete event feature with O1-ext object type counts, now that active_graph is set
                obj_counts = [float(len(active_graph.get(ot, []))) for ot in self.obj_type_order]
                active_events.append(ev_feat_partial + obj_counts)
                active_graph['Events'] = active_events

                ev_id = row['ev_id']
                if ev_id in ev_kpi_map:
                    y_val = ev_kpi_map[ev_id]
                else:
                    y_val = max(0.0, (end_ts - current_ts).total_seconds())
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
                        except TypeError as e:
                            # A malformed/wrong-shaped node feature here should never be
                            # silently dropped -- it would otherwise leave this node type
                            # missing from the graph entirely, surfacing (if at all) as a
                            # much more confusing failure deep inside model training.
                            raise TypeError(
                                f"Failed to build node type '{key}' for process={process}, "
                                f"event index={cnt}: expected reshape(-1, {ob_len}), got data "
                                f"{active_graph[key]!r}"
                            ) from e
                for edge in edge_list:
                    split = edge.split("_to_")
                    data[split[0], 'to', split[1]].edge_index = torch.tensor(active_graph[edge],
                                                                             dtype=torch.int64).reshape(2, -1)
                # Adds the kpi values for the kpi object
                kpi_ob = self.path_dict['kpi_viewpoint']

                # Build per-instance y-values: each viewpoint node may have a different
                # remaining time (e.g. two Packages delivered at different times).
                # Nodes whose object ID is outside the tracked set are masked out so
                # they do not contribute to the training loss.
                vp_ob_ids  = ast.literal_eval(row[f"{kpi_ob}::ids"])
                primary_id = y_df['ob_id'].iloc[0]
                y_vals, mask_vals = [], []
                for ob_id in vp_ob_ids:
                    if ob_id == primary_id:
                        y_vals.append(float(y_val))
                        mask_vals.append(True)
                    elif ob_id in self.active_orders_dict:
                        end_ts = pd.to_datetime(self.active_orders_dict[ob_id])
                        secs   = max(0.0, (end_ts - current_ts).total_seconds())
                        y_vals.append(secs)
                        mask_vals.append(True)
                    else:
                        y_vals.append(0.0)   # placeholder; excluded by mask
                        mask_vals.append(False)
                n_vp = len(y_vals)

                if self.path_dict['kpi_type'] == 0:
                    data[kpi_ob].y    = torch.tensor(y_vals,    dtype=torch.float32).reshape(-1, 1)
                    data[kpi_ob].mask = torch.tensor(mask_vals, dtype=torch.bool).reshape(-1, 1)
                else:
                    data[kpi_ob].y    = torch.tensor(y_vals,    dtype=torch.long)
                    data[kpi_ob].mask = torch.tensor(mask_vals, dtype=torch.bool)

                data[kpi_ob].id         = torch.full((n_vp, 1), float(process), dtype=torch.float32)
                data[kpi_ob].last_event = torch.full((n_vp, 1), last_event, dtype=torch.bool)

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