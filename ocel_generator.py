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
        self.o2o_lookup = self._build_o2o_lookup()  # pre-indexed for O(1) edge resolution
        self.encodings = self.get_encodings()
        self.ev_type_encodings = self._build_ev_encodings()  # replaces per-event SQL queries

        # Creates dictionary of selected attributes for chosen object types
        self.ob_attributes = self.get_attributes()
        self.time_attr_cache = self._preload_time_attributes()  # pre-fetched time-sensitive attrs

        # Dictionary of object sizes for future tensor creation
        self.tensor_dict = {}

    def table_names(self):
        self.cursor.execute(f"SELECT name FROM sqlite_master")
        table_names = self.cursor.fetchall()
        table_names = [column[0] for column in table_names]
        return table_names

    # Run a query to find all the object to object relations present in the data
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

    def _build_o2o_lookup(self):
        """Pre-index o2o_df as {src_id: {trgt_type: [trgt_ids]}} for O(1) edge resolution."""
        lookup = defaultdict(lambda: defaultdict(list))
        for _, row in self.o2o_df.iterrows():
            lookup[row['src_id']][row['trgt_type']].append(row['trgt_id'])
        return lookup

    def _build_ev_encodings(self):
        """Pre-compute one-hot encodings for all event types — replaces per-event SQL query."""
        self.cursor.execute("SELECT DISTINCT OCEL_TYPE_MAP FROM EVENT_MAP_TYPE ORDER BY 1")
        types = [row[0] for row in self.cursor.fetchall()]
        result = {}
        for i, t in enumerate(types):
            vec = [0] * len(types)
            vec[i] = 1
            result[t] = vec
        return result

    def _preload_time_attributes(self):
        """Pre-fetch all time-sensitive attribute tables so per-event SQL queries can be avoided."""
        cache = {}
        for att_type, attr_cols in self.path_dict.get('time_attributes', {}).items():
            fixed_attr = attr_cols[0]
            time_attr = attr_cols[1]

            table = f'object_{att_type}'
            cols = self.col_names(table)
            if not cols:
                table = f'event_{att_type}'
                cols = self.col_names(table)

            id_col = cols[0]
            ts_col = cols[1]

            self.cursor.execute(f"SELECT {id_col}, {ts_col}, {fixed_attr}, {time_attr} FROM {table}")
            df = pd.DataFrame(self.cursor.fetchall(), columns=['ob_id', 'ts', 'fixed_val', 'time_val'])
            df['ts'] = pd.to_datetime(df['ts'])

            # Resolve COALESCE: fill nulls in fixed_val from any non-null row per ob_id
            fixed_per_ob = (
                df.dropna(subset=['fixed_val'])
                .groupby('ob_id')['fixed_val']
                .first()
                .to_dict()
            )
            df['fixed_val'] = df['ob_id'].map(fixed_per_ob)

            df = df.sort_values(['ob_id', 'ts']).reset_index(drop=True)
            # Group by ob_id for fast sub-selection
            cache[att_type] = df.groupby('ob_id', sort=False)
        return cache

    # Generate a local dictionary of the desired attributes for the chosen object types
    def get_attributes(self):
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
            cols = ['ob_id']
            cols.extend(self.path_dict['attributes'][att_type])
            attrs = pd.DataFrame(attrs, columns=cols).set_index('ob_id')
            ob_attributes[att_type] = attrs
        return ob_attributes

    def get_time_attributes(self, node_id, att_type, fixed_attr, time_attr, timestamp):
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

        # If the selected object type has too many values to properly encode just assign a 1
        if len(types) > 50:
            for idx, a in enumerate(types):
                oh_dict[types[idx]] = [1]
        else:
            binary = [[0] * len(types) for _ in range(len(types))]
            for idx, a in enumerate(binary):
                a[idx] = 1
                oh_dict[types[idx]] = a
        return oh_dict

    def get_ev_encoding(self, ev_type):
        qry = f'''
               SELECT DISTINCT OCEL_TYPE_MAP
               FROM EVENT_MAP_TYPE
               ORDER BY 1;
               '''
        self.cursor.execute(qry)
        types = self.cursor.fetchall()
        types = [ev_type[0] for ev_type in types]
        events = [[0] * len(types)]
        events[0][types.index(ev_type)] = 1
        return events[0]

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names

    def generate_adjacency_list_with_k(self, ev_ob_df, par):
        """
        Generate an adjacency list from events_by_objects, linking events up to the K-th event.

        Parameters:
        - events_by_objects: dict, mapping objects to their respective events.
        - K: int, maximum index of events to include in the adjacency list.

        Returns:
        - adjacency_list: list of two lists [source_nodes, target_nodes].
        """

        def generate_consecutive_pairs(events):
            """Generate pairs of consecutive events."""
            pairs = []
            n = len(events)
            for i in range(n - 1):
                pairs.append((events[i], events[i + 1]))
            return pairs

        events_by_objects = {}
        for i, row in ev_ob_df.iterrows():
            v = row['events']
            k = row['ob_id']
            events_by_objects[k] = [a for a in v if a <= par]

        subsequences_by_object = {}
        for obj, events in events_by_objects.items():
            subsequences_by_object[obj] = generate_consecutive_pairs(events)

        event_links = defaultdict(set)
        for subseq in subsequences_by_object.values():
            for e1, e2 in subseq:
                event_links[e1].add(e2)

        source_nodes = []
        target_nodes = []

        # Iterate over the event links and create the source-target pairs
        for source, targets in event_links.items():
            for target in targets:
                source_nodes.append(source)
                target_nodes.append(target)
        return [source_nodes, target_nodes]

    def generate_ocel(self, nodes):
        all_timestamps = []
        all_idx = []
        vwpnt_cnt = 0
        ocel = []
        all_edges = []

        print("Generating OCEL...")
        for vwpnt_object in nodes.keys():
            vwpnt_cnt += 1

            # Prepare the dataframes that will be used throughout the process
            past_events = []
            ob_df = nodes[vwpnt_object]['related_objects'][0]
            ev_by_ob = nodes[vwpnt_object]['events_by_objects'][0]

            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])

            # Convert ev_by_ob to fast-access structures (avoids iterrows + linear scans per event)
            ev_by_ob_records = ev_by_ob.to_dict('records')
            ev_by_ob_index = {r['ob_id']: r for r in ev_by_ob_records}

            # Pre-compute all consecutive event pairs across objects; sort by target for incremental growth
            _all_pairs_set = set()
            for r in ev_by_ob_records:
                evs = sorted(r['events'])
                for a, b in zip(evs, evs[1:]):
                    _all_pairs_set.add((a, b))
            _sorted_pairs = sorted(_all_pairs_set, key=lambda p: p[1])
            _pair_ptr = 0
            _active_src: list = []
            _active_dst: list = []

            # Create a new row for every event in the process
            for i, row in ev_df.iterrows():
                ocel_row = []
                ocel_row_cols = []
                edges = []
                edges_cols = []
                ev_idx = row['index']
                ev_id = row['ocel_id']
                ev_type = row['type']
                timestamp = row['timestamp']
                objects_in_event = []

                encode = self.ev_type_encodings[ev_type]
                self.tensor_dict['Events'] = len(encode)

                # Add values for the numpy lists used for filtering
                all_timestamps.append(timestamp)
                all_idx.append(vwpnt_cnt)
                past_events.append(ev_idx)

                # Add event identifiers to the row
                ocel_row.append(ev_id)
                ocel_row.append(encode)
                ocel_row.append(pd.to_datetime(timestamp))
                ocel_row.append(vwpnt_cnt)
                ocel_row_cols.append('ev_id')
                ocel_row_cols.append('ev_type')
                ocel_row_cols.append('timestamp')
                ocel_row_cols.append('vwpnt_id')

                # Add the event_to_event edges to edges_dataframe
                edges.append(ev_id)
                edges.append(timestamp)
                edges.append(vwpnt_cnt)
                # Incrementally extend the adjacency list — only append pairs whose target <= ev_idx
                while _pair_ptr < len(_sorted_pairs) and _sorted_pairs[_pair_ptr][1] <= ev_idx:
                    _active_src.append(_sorted_pairs[_pair_ptr][0])
                    _active_dst.append(_sorted_pairs[_pair_ptr][1])
                    _pair_ptr += 1
                edges.append([list(_active_src), list(_active_dst)])
                edges_cols.append('ev_id')
                edges_cols.append('timestamp')
                edges_cols.append('vwpnt_id')
                edges_cols.append('Events_to_Events')

                self.tensor_dict['Events_to_Events'] = 1

                # Add the objects related to each step
                ob_types = set()

                ob_attrs = {}
                edge_attrs = {}
                past_events_set = set(past_events)  # O(1) membership test
                for row in ev_by_ob_records:
                    ob_id = row['ob_id']
                    evs_by_ob = row['events']
                    ob_type = row['ob_type']
                    ob_idx = row['index']
                    edge_type = f"{ob_type}_to_Events"
                    ob_attrs.setdefault(ob_type, [])
                    edge_attrs.setdefault(edge_type, [[], []])
                    # Identify if the object needs to be added to the graph
                    contained = False
                    for event_by_object in evs_by_ob:
                        if event_by_object in past_events_set:
                            contained = True
                            objects_in_event.append(ob_id)
                            ob_types.add(ob_type)
                            break

                    # Collect the relevant attributes and values for each event and add them to the row
                    if ob_type in self.path_dict['attributes']:
                        attr_df = self.ob_attributes[ob_type]
                        attributes_vals = attr_df.loc[ob_id].tolist()  # O(1) indexed lookup
                        self.tensor_dict[ob_type] = len(attributes_vals)
                        if contained:
                            ob_attrs[ob_type].append([ob_id, attributes_vals, ob_idx])
                    elif ob_type in self.path_dict['time_attributes']:
                        self.tensor_dict[ob_type] = len(self.path_dict['time_attributes'][ob_type])
                        # In-memory nearest-timestamp lookup instead of per-call SQL query
                        grp = self.time_attr_cache[ob_type]
                        try:
                            sub = grp.get_group(ob_id).reset_index(drop=True)
                            ts = pd.Timestamp(timestamp)
                            pos = max(0, sub['ts'].searchsorted(ts, side='right') - 1)
                            r = sub.iloc[pos]
                            attributes = [ob_id, r['fixed_val'], r['time_val']]
                        except KeyError:
                            attributes = [ob_id, None, None]
                        if contained:
                            ob_attrs[ob_type].append([ob_id, attributes[1:], ob_idx])
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

                # Add the object to event edges
                for ob_id in objects_in_event:
                    ob_row = ev_by_ob_index[ob_id]       # O(1) dict lookup
                    evs_by_ob = ob_row['events']
                    ob_events = [ev for ev in evs_by_ob if ev in past_events_set]
                    ob_idx = rel_indx[ob_id]
                    ob_type = ob_row['ob_type']
                    edge_type = f"{ob_type}_to_Events"
                    self.tensor_dict[edge_type] = 1

                    if len(ob_events) > 0:  # Only add edge if objects are present
                        object = [int(ob_idx) for a in range(len(ob_events))]
                        edge_attrs[edge_type][0].extend(object)
                        edge_attrs[edge_type][1].extend(ob_events)

                # Add object to object edge
                objects_in_event = ob_df.loc[ob_df['ocel_id'].isin(objects_in_event)]
                for relation in self.o2o_relations:
                    ob_source = relation[0]
                    ob_target = relation[1]
                    sources = objects_in_event[objects_in_event['type'] == ob_source]
                    targets = objects_in_event[objects_in_event['type'] == ob_target]
                    edge_name = f'{ob_source}_to_{ob_target}'
                    edge_attrs[edge_name] = [[], []] if edge_name not in edge_attrs.keys() else edge_attrs[edge_name]
                    self.tensor_dict[edge_name] = 1
                    if len(sources) > 0 and len(targets) > 0:
                        target_ids_active = set(targets['ocel_id'])
                        for i, row in sources.iterrows():
                            src_id = row['ocel_id']
                            src_index = rel_indx[src_id]
                            trgt_ids = self.o2o_lookup[src_id][ob_target]  # O(1) dict lookup

                            for trgt_id in trgt_ids:
                                if trgt_id in target_ids_active:
                                    tg_index = rel_indx[trgt_id]
                                    edge_attrs[edge_name][0].append(src_index)
                                    edge_attrs[edge_name][1].append(tg_index)

                # Assign the dictionary values to the dataframe shape
                for ob in ob_attrs.keys():
                    ob_ids = []
                    ob_attributes = []
                    ob_idx = []
                    for x in ob_attrs[ob]:
                        ob_ids.append(x[0])
                        ob_attributes.append(x[1])
                        ob_idx.append(x[2])

                    ocel_row.append(ob_ids)
                    ocel_row.append(ob_attributes)
                    ocel_row.append(ob_idx)

                    ocel_row_cols.append(f"{ob}::ids")
                    ocel_row_cols.append(f"{ob}::attributes")
                    ocel_row_cols.append(f"{ob}::idx")

                for edge in edge_attrs.keys():
                    edges.append(edge_attrs[edge])
                    edges_cols.append(f"{edge}")

                """
                    Convert the row into a dataframe and add it to the whole OCEL
                """
                ocel.append(ocel_row)
                all_edges.append(edges)
        ocel = pd.DataFrame(ocel, columns=ocel_row_cols)
        edges = pd.DataFrame(all_edges, columns=edges_cols)
        ocel.to_csv(f"files/graph_structures/{self.database}/{self.cant}/ocel.csv", index=False)
        edges.to_csv(f"files/graph_structures/{self.database}/{self.cant}/edges.csv", index=False)
        print('Done')

    def encode_events(self):
        ocel = pd.read_csv(f"files/graph_structures/{self.database}/{self.cant}/ocel.csv")
        ocel['ev_type'] = ocel['ev_type'].apply(lambda x: self.get_ev_encoding(x))
        ocel.to_csv(f"files/graph_structures/{self.database}/{self.cant}/ocel.csv", index=False)