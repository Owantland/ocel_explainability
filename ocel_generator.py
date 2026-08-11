import sqlite3
import json
import pandas as pd
import copy
import sup_funcs as sf

from collections import defaultdict
from sklearn.preprocessing import StandardScaler
import numpy as np
import ast

'''
    Creating a unified Event table
'''
class Generator:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sf.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()

        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()
        self.tabl_nms = self.table_names()
        self.o2o_relations = self.get_o2o_relations()
        self.o2o_df = self.get_o2o_df()

        # Fix 7: pre-group o2o_df by src_id for O(1) lookup in edge-building inner loop
        self.o2o_by_src = {}
        for src_id, grp in self.o2o_df.groupby('src_id'):
            self.o2o_by_src[src_id] = list(zip(grp['trgt_id'], grp['trgt_type']))

        self.encodings = self.get_encodings()
        # Role-based encodings for types listed in role_encoding config
        # (e.g. Employees: 'role' → 3D one-hot over distinct roles instead of 18D ID one-hot)
        self.role_encodings = self._build_role_encodings()

        # Fix 8: safe access for time_attributes (handles absent key or None value)
        self._time_attrs = self.path_dict.get('time_attributes') or {}

        # Fix 3: ob_attributes now stored as {ob_id: [attr_vals]} dicts instead of DataFrames
        self.ob_attributes = self.get_attributes()

        # Fix 1: build event-type → one-hot encoding dict once (was 1 SQL query per event)
        self.ev_encodings = self._build_ev_encodings()

        # Fix 2: preload time-attribute tables into memory (was 1 CTE SQL query per object per event)
        self.time_attr_cache = self._build_time_attr_cache()

        # Dictionary of object sizes for future tensor creation
        self.tensor_dict = {}

    def table_names(self):
        self.cursor.execute(f"SELECT name FROM sqlite_master")
        table_names = self.cursor.fetchall()
        table_names = [column[0] for column in table_names]
        return table_names

    def get_o2o_relations(self):
        qry = f'''
                WITH ob2ob AS (
                    SELECT DISTINCT O.OCEL_TYPE AS SOURCE, O3.OCEL_TYPE AS TARGET
                    FROM OBJECT_OBJECT OO
                    JOIN OBJECT O ON O.OCEL_ID = OO.ocel_source_id
                    JOIN OBJECT O3 ON O3.OCEL_ID = OO.OCEL_TARGET_ID
                )
                SELECT M.OCEL_TYPE_MAP AS SOURCE_TYPE, M2.OCEL_TYPE_MAP AS TARGET_TYPE
                FROM OB2OB O
                JOIN OBJECT_MAP_TYPE M ON O.SOURCE = M.OCEL_TYPE
                JOIN OBJECT_MAP_TYPE M2 ON O.TARGET = M2.OCEL_TYPE;
               '''
        self.cursor.execute(qry)
        o2o_relations = self.cursor.fetchall()
        return o2o_relations

    def get_o2o_df(self):
        qry = f'''
                        SELECT  OO.OCEL_SOURCE_ID
                                ,OO.OCEL_TARGET_ID
                                ,M.OCEL_TYPE_MAP AS TARGET_TYPE
                        FROM OBJECT_OBJECT OO
                        JOIN OBJECT O ON OO.ocel_target_id = O.OCEL_ID
                        JOIN OBJECT_MAP_TYPE M ON O.OCEL_TYPE = M.OCEL_TYPE
                       '''
        self.cursor.execute(qry)
        o2o_df = self.cursor.fetchall()
        o2o_df = pd.DataFrame(o2o_df, columns=['src_id', 'trgt_id', 'trgt_type'])
        return o2o_df

    def get_attributes(self):
        """Load static object attributes once, stored as {ob_id: [val1, val2, ...]} dicts."""
        ob_attributes = {}
        for att_type in self.path_dict['attributes'].keys():
            attributes = self.path_dict['attributes'][att_type]
            attributes = [f'MAX({a})' for a in attributes]
            if len(attributes) > 1:
                attributes = ','.join(attributes)
            else:
                attributes = attributes[0]

            table = f'object_{att_type}'
            cols = self.col_names(table)

            if len(cols) == 0:
                table = f'event_{att_type}'
                cols = self.col_names(table)

            qry = f'''
                    SELECT {cols[0]}, {attributes}
                    FROM {table}
                    GROUP BY {cols[0]}
                   '''

            self.cursor.execute(qry)
            attrs = self.cursor.fetchall()
            col_names = ['ob_id'] + self.path_dict['attributes'][att_type]
            attrs_df = pd.DataFrame(attrs, columns=col_names)
            # Fix 3: dict for O(1) lookup (replaces per-event DataFrame equality scan)
            ob_attributes[att_type] = dict(
                zip(attrs_df['ob_id'], attrs_df.iloc[:, 1:].values.tolist())
            )
        return ob_attributes

    def _build_ev_encodings(self):
        """Build event-type → one-hot vector mapping once (replaces per-event SQL query)."""
        self.cursor.execute("SELECT DISTINCT OCEL_TYPE_MAP FROM EVENT_MAP_TYPE ORDER BY 1")
        types = [r[0] for r in self.cursor.fetchall()]
        result = {}
        for i, t in enumerate(types):
            enc = [0] * len(types)
            enc[i] = 1
            result[t] = enc
        return result

    def _build_time_attr_cache(self):
        """Load all time-attribute tables into memory (replaces 1 CTE SQL query per object per event)."""
        cache = {}
        for att_type, attr_cols in self._time_attrs.items():
            fixed_attr, time_attr = attr_cols[0], attr_cols[1]
            table = f'object_{att_type}'
            cols = self.col_names(table)
            if not cols:
                table = f'event_{att_type}'
                cols = self.col_names(table)
            self.cursor.execute(f"SELECT * FROM {table}")
            df = pd.DataFrame(self.cursor.fetchall(), columns=cols)
            cache[att_type] = {
                'df': df,
                'id_col': cols[0],
                'ts_col': cols[1],
                'fixed_attr': fixed_attr,
                'time_attr': time_attr,
            }
        return cache

    def _lookup_time_attrs(self, node_id, att_type, timestamp):
        """In-memory equivalent of get_time_attributes (no SQL per call)."""
        entry = self.time_attr_cache[att_type]
        df = entry['df']
        sub = df[df[entry['id_col']] == node_id].copy()
        if sub.empty:
            return [node_id, None, None]
        sub['_delta'] = (
            pd.to_datetime(sub[entry['ts_col']]) - pd.to_datetime(timestamp)
        ).abs()
        row = sub.loc[sub['_delta'].idxmin()].copy()
        # COALESCE: if the closest row has a null fixed attr, fill from any non-null row
        if pd.isna(row[entry['fixed_attr']]):
            fallback = sub.loc[sub[entry['fixed_attr']].notna(), entry['fixed_attr']]
            if not fallback.empty:
                row[entry['fixed_attr']] = fallback.iloc[0]
        def _py(x):
            """Convert numpy scalar → plain Python float/None so ast.literal_eval works."""
            if pd.isna(x):
                return None
            try:
                return float(x)
            except (TypeError, ValueError):
                return x
        return [row[entry['id_col']], _py(row[entry['fixed_attr']]), _py(row[entry['time_attr']])]

    def get_time_attributes(self, node_id, att_type, fixed_attr, time_attr, timestamp):
        """Original SQL-based time attribute lookup (kept for external callers)."""
        table = f'object_{att_type}'
        cols = self.col_names(table)

        if len(cols) == 0:
            table = f'event_{att_type}'
            cols = self.col_names(table)

        qry = f"""
                WITH RankedData AS (
                SELECT
                    {cols[0]},
                    {fixed_attr},
                    {time_attr},
                    ROW_NUMBER() OVER (
                        PARTITION BY {cols[0]}
                        ORDER BY ABS(STRFTIME('%s', {cols[1]}) - STRFTIME('%s', '{timestamp}')) ASC
                    ) AS rank
                FROM {table}
                ),
                FixedWeights AS (
                    SELECT
                        {cols[0]} AS item,
                        {fixed_attr}
                    FROM {table}
                    WHERE {cols[3]} IS NOT NULL
                    GROUP BY {cols[0]}
                )
                SELECT
                    {cols[0]},
                    COALESCE(RankedData.{fixed_attr}, FixedWeights.{fixed_attr}) AS {fixed_attr},
                    RankedData.{cols[4]}
                FROM RankedData
                LEFT JOIN FixedWeights
                ON RankedData.{cols[0]} = FixedWeights.item
                WHERE RankedData.rank = 1 AND {cols[0]} = '{node_id}';
                """
        self.cursor.execute(qry)
        attrs = self.cursor.fetchall()
        attrs = [attr for attr in attrs[0]]
        return attrs

    def get_encodings(self):
        encodings = {}
        for encoding in self.path_dict['encoding']:
            encod_dict = self.get_1h_encoding(encoding)
            encodings[encoding] = encod_dict
        return encodings

    def get_1h_encoding(self, ob_type):
        oh_dict = {}
        table = f'object_{ob_type}'
        cols = self.col_names(table)

        if len(cols) == 0:
            table = f'event_{ob_type}'

        qry = f'''
                SELECT DISTINCT OCEL_ID
                FROM {table}
                ORDER BY 1;
               '''
        self.cursor.execute(qry)
        types = self.cursor.fetchall()
        types = [ob_type[0] for ob_type in types]

        if len(types) > 50:
            for idx, a in enumerate(types):
                oh_dict[types[idx]] = [1]
        else:
            binary = [[0] * len(types) for _ in range(len(types))]
            for idx, a in enumerate(binary):
                a[idx] = 1
                oh_dict[types[idx]] = a
        return oh_dict

    def _build_role_encodings(self):
        """Build {ob_id: role_one_hot} dicts for types listed in role_encoding config.

        For order_management Employees: maps each employee ID to a 3D one-hot over
        (Sales, Shipment, Warehousing) instead of the default 18D ID one-hot.
        """
        role_enc = {}
        for ob_type, role_col in (self.path_dict.get('role_encoding') or {}).items():
            table = f'object_{ob_type}'
            cols = self.col_names(table)
            if not cols:
                table = f'event_{ob_type}'
                cols = self.col_names(table)
            # Distinct roles in alphabetical order (deterministic)
            self.cursor.execute(
                f"SELECT DISTINCT {role_col} FROM {table} WHERE {role_col} IS NOT NULL ORDER BY 1"
            )
            roles = [r[0] for r in self.cursor.fetchall()]
            role_to_vec = {role: [int(j == i) for j in range(len(roles))]
                           for i, role in enumerate(roles)}
            # Map each employee ID to its role vector
            self.cursor.execute(
                f"SELECT DISTINCT ocel_id, MAX({role_col}) FROM {table} GROUP BY ocel_id"
            )
            id_role_map = {row[0]: row[1] for row in self.cursor.fetchall()}
            zero_vec = [0] * len(roles)
            role_enc[ob_type] = {
                ob_id: role_to_vec.get(role, zero_vec)
                for ob_id, role in id_role_map.items()
            }
        return role_enc

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names

    def generate_ocel(self, nodes):
        all_timestamps = []
        all_idx = []
        vwpnt_cnt = 0
        ocel = []
        all_edges = []

        print("Generating OCEL...")
        for vwpnt_object in nodes.keys():
            vwpnt_cnt += 1

            ob_df = nodes[vwpnt_object]['related_objects'][0]
            ev_by_ob = nodes[vwpnt_object]['events_by_objects'][0]

            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])

            # Fix 5: build ev_by_ob lookup dict once per viewpoint (replaces per-event DataFrame scans)
            ev_by_ob_dict = {
                row['ob_id']: {
                    'events': row['events'],
                    'ob_type': row['ob_type'],
                    'index': row['index'],
                }
                for _, row in ev_by_ob.iterrows()
            }

            # Fix 6: precompute consecutive pairs indexed by dst for O(1) incremental update
            # pairs_by_dst[dst] = set of src events where (src, dst) is a consecutive pair
            pairs_by_dst = defaultdict(set)
            for ob_info in ev_by_ob_dict.values():
                evs = sorted(ob_info['events'])
                for a, b in zip(evs[:-1], evs[1:]):
                    pairs_by_dst[b].add(a)

            # Fix 4: set for O(1) membership testing (was list → O(n))
            past_events = set()
            # Fix 6: incremental adjacency list maintained across events
            active_links = defaultdict(set)

            for _, ev_row in ev_df.iterrows():
                ocel_row = []
                ocel_row_cols = []
                edges = []
                edges_cols = []
                ev_idx = ev_row['index']
                ev_id = ev_row['ocel_id']
                ev_type = ev_row['type']
                timestamp = ev_row['timestamp']
                objects_in_event = []

                # Fix 1: O(1) dict lookup instead of SQL query per event
                encode = self.ev_encodings[ev_type]
                self.tensor_dict['Events'] = len(encode)

                all_timestamps.append(timestamp)
                all_idx.append(vwpnt_cnt)

                # Fix 4: O(1) set add
                past_events.add(ev_idx)

                # Fix 6: extend active_links with pairs whose dst == ev_idx
                for src in pairs_by_dst.get(ev_idx, set()):
                    active_links[src].add(ev_idx)
                adj = [[], []]
                for src, tgts in active_links.items():
                    for tgt in tgts:
                        adj[0].append(src)
                        adj[1].append(tgt)

                ocel_row.append(ev_id)
                ocel_row.append(encode)
                ocel_row.append(pd.to_datetime(timestamp))
                ocel_row.append(vwpnt_cnt)
                ocel_row_cols.append('ev_id')
                ocel_row_cols.append('ev_type')
                ocel_row_cols.append('timestamp')
                ocel_row_cols.append('vwpnt_id')

                edges.append(ev_id)
                edges.append(timestamp)
                edges.append(vwpnt_cnt)
                edges.append(adj)
                edges_cols.append('ev_id')
                edges_cols.append('timestamp')
                edges_cols.append('vwpnt_id')
                edges_cols.append('Events_to_Events')

                self.tensor_dict['Events_to_Events'] = 1

                ob_types = set()
                ob_attrs = {}
                edge_attrs = {}

                # Fix 5: iterate ev_by_ob_dict instead of ev_by_ob.iterrows()
                for ob_id, ob_info in ev_by_ob_dict.items():
                    evs_by_ob = ob_info['events']
                    ob_type = ob_info['ob_type']
                    ob_idx = ob_info['index']
                    edge_type = f"{ob_type}_to_Events"
                    ob_attrs[ob_type] = ob_attrs.get(ob_type, [])
                    edge_attrs[edge_type] = edge_attrs.get(edge_type, [[], []])

                    # Fix 4: O(1) set membership — `any` short-circuits on first match
                    contained = any(ev in past_events for ev in evs_by_ob)
                    if contained:
                        objects_in_event.append(ob_id)
                        ob_types.add(ob_type)

                    if ob_type in self.path_dict['attributes']:
                        # Fix 3: O(1) dict lookup (was O(n) DataFrame equality scan)
                        attr_vals = self.ob_attributes[ob_type].get(ob_id, [])
                        self.tensor_dict[ob_type] = len(attr_vals)
                        if contained:
                            ob_attrs[ob_type].append([ob_id, attr_vals, ob_idx])
                    elif ob_type in self._time_attrs:
                        # Fix 2 & 8: in-memory lookup; safe _time_attrs access handles None/absent key
                        attr = self._time_attrs[ob_type]
                        self.tensor_dict[ob_type] = len(attr)
                        attributes = self._lookup_time_attrs(ob_id, ob_type, timestamp)
                        if contained:
                            ob_attrs[ob_type].append([attributes[0], attributes[1:], ob_idx])
                    elif ob_type in self.role_encodings:
                        # Role-based encoding (e.g. Employees → 3D role one-hot)
                        zero_vec = [0] * len(next(iter(self.role_encodings[ob_type].values())))
                        enc = self.role_encodings[ob_type].get(ob_id, zero_vec)
                        self.tensor_dict[ob_type] = len(enc)
                        if contained:
                            ob_attrs[ob_type].append([ob_id, enc, ob_idx])
                    else:
                        if ob_type in self.path_dict['encoding']:
                            enc = self.encodings[ob_type][ob_id]
                            self.tensor_dict[ob_type] = len(enc)
                            if contained:
                                ob_attrs[ob_type].append([ob_id, enc, ob_idx])

                # Reorganize the index relative to how many items are active at the moment
                rel_indx = {}
                for ob in ob_attrs.keys():
                    ob_attrs[ob] = sorted(ob_attrs[ob], key=lambda x: (x[-1], x[0]))
                    ob_cnt = 0
                    for x in ob_attrs[ob]:
                        x[-1] = ob_cnt
                        rel_indx[x[0]] = ob_cnt
                        ob_cnt += 1

                # Add the object-to-event edges
                for ob_id in objects_in_event:
                    # Fix 5: O(1) dict lookup (was two separate DataFrame scans)
                    ob_info = ev_by_ob_dict[ob_id]
                    evs_by_ob = ob_info['events']
                    ob_type = ob_info['ob_type']
                    # Fix 4: O(1) set membership in list comprehension
                    ob_events = [ev for ev in evs_by_ob if ev in past_events]
                    ob_idx = rel_indx[ob_id]
                    edge_type = f"{ob_type}_to_Events"
                    self.tensor_dict[edge_type] = 1

                    if len(ob_events) > 0:
                        object_indices = [int(ob_idx)] * len(ob_events)
                        edge_attrs[edge_type][0].extend(object_indices)
                        edge_attrs[edge_type][1].extend(ob_events)

                # Add object-to-object edges
                objects_in_event_df = ob_df.loc[ob_df['ocel_id'].isin(objects_in_event)]
                target_ids_by_type = {}  # cache sets per type for O(1) membership test
                for relation in self.o2o_relations:
                    ob_source = relation[0]
                    ob_target = relation[1]
                    sources = objects_in_event_df[objects_in_event_df['type'] == ob_source]
                    targets = objects_in_event_df[objects_in_event_df['type'] == ob_target]
                    edge_name = f'{ob_source}_to_{ob_target}'
                    edge_attrs[edge_name] = edge_attrs.get(edge_name, [[], []])
                    self.tensor_dict[edge_name] = 1

                    if len(sources) > 0 and len(targets) > 0:
                        # Build set of active target ids for this type (O(1) membership below)
                        if ob_target not in target_ids_by_type:
                            target_ids_by_type[ob_target] = set(targets['ocel_id'])
                        target_set = target_ids_by_type[ob_target]

                        for _, src_row in sources.iterrows():
                            src_id = src_row['ocel_id']
                            src_index = rel_indx[src_id]
                            # Fix 7: O(1) grouped-dict lookup (was O(n) DataFrame scan)
                            trgt_ids = [
                                tid for tid, ttype in self.o2o_by_src.get(src_id, [])
                                if ttype == ob_target and tid in target_set
                            ]
                            for trgt_id in trgt_ids:
                                tg_index = rel_indx[trgt_id]
                                edge_attrs[edge_name][0].append(src_index)
                                edge_attrs[edge_name][1].append(tg_index)

                # Assign the dictionary values to the dataframe shape
                for ob in ob_attrs.keys():
                    ob_ids = [x[0] for x in ob_attrs[ob]]
                    ob_attributes = [x[1] for x in ob_attrs[ob]]
                    ob_idx_list = [x[2] for x in ob_attrs[ob]]

                    ocel_row.append(ob_ids)
                    ocel_row.append(ob_attributes)
                    ocel_row.append(ob_idx_list)

                    ocel_row_cols.append(f"{ob}::ids")
                    ocel_row_cols.append(f"{ob}::attributes")
                    ocel_row_cols.append(f"{ob}::idx")

                for edge in edge_attrs.keys():
                    edges.append(edge_attrs[edge])
                    edges_cols.append(f"{edge}")

                ocel.append(ocel_row)
                all_edges.append(edges)

        ocel = pd.DataFrame(ocel, columns=ocel_row_cols)
        edges = pd.DataFrame(all_edges, columns=edges_cols)
        ocel.to_csv(f"files/graph_structures/{self.database}/{self.cant}/ocel.csv", index=False)
        edges.to_csv(f"files/graph_structures/{self.database}/{self.cant}/edges.csv", index=False)
        with open(f"files/graph_structures/{self.database}/{self.cant}/tensor_dict.json", "w") as f:
            json.dump(self.tensor_dict, f)
        print('Done')
