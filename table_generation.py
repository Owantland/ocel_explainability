# File for testing the database functions
# The goal is to obtain three main databases: all_objects, all_events, all_object_events

import sqlite3
import json
import numpy as np
import yaml
import pandas as pd
import copy

from collections import defaultdict

'''
    Creating a unified Event table
'''
class generateTables():
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.get_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()
        self.tabl_nms = self.table_names()
        self.o2o_relations = self.get_o2o_relations()
        self.get_encodings()

        # Dictionary of object sizes for future tensor creation
        self.tensor_dict = {}

    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.ocel_path = db_configs[self.database]['ocel_path']
        self.ev_output = db_configs[self.database]['ev_output_path']
        self.filtered_tbls = db_configs[self.database]['filtered_tables']
        self.viewpoint = db_configs[self.database]['viewpoint']
        self.depth = db_configs[self.database]['added_depth']
        self.attributes = db_configs[self.database]['attributes']
        self.time_attributes = db_configs[self.database]['time_attributes']
        self.to_encode = db_configs[self.database]['encoding']
        self.kpis = db_configs[self.database]['kpis']
        self.kpi_event = db_configs[self.database]['kpi_event']
        self.graph_output_path = db_configs[self.database]['graph_output_path']

    def get_encodings(self):
        self.encodings = {}
        for encoding in self.to_encode:
            encod_dict = self.get_1h_encoding(encoding)
            self.encodings[encoding] = encod_dict

    def get_attributes(self, node_id, type, attributes):
        attributes = [f'MAX({a})' for a in attributes]
        if len(attributes) > 1:
            attributes = ','.join(attributes)
        else:
            attributes = attributes[0]

        table = f'object_{type}'
        cols = self.col_names(table)

        if len(cols) == 0:
            table = f'event_{type}'
            cols = self.col_names(table)

        qry = f'''
                SELECT {cols[0]}, {attributes}
                FROM {table}
                WHERE {cols[0]} = '{node_id}'
               '''

        self.cursor.execute(qry)
        attrs = self.cursor.fetchall()
        attrs = [attr for attr in attrs[0]]
        return attrs

    def get_time_attributes(self, node_id, type, fixed_attr, time_attr, timestamp):
        table = f'object_{type}'
        cols = self.col_names(table)

        if len(cols) == 0:
            table = f'event_{type}'
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

    def get_ev_encoding(self, type):
        qry = f'''
               SELECT DISTINCT OCEL_TYPE_MAP
               FROM EVENT_MAP_TYPE
               ORDER BY 1;
               '''
        self.cursor.execute(qry)
        types = self.cursor.fetchall()
        types = [type[0] for type in types]
        events = [[0] * len(types)]
        events[0][types.index(type)] = 1
        return events[0]

    def get_1h_encoding(self, type):
        oh_dict = {}
        table = f'object_{type}'
        cols = self.col_names(table)

        if len(cols) == 0:
            table = f'event_{type}'
            cols = self.col_names(table)

        qry = f'''
                SELECT DISTINCT OCEL_ID
                FROM {table}
                ORDER BY 1;
               '''
        self.cursor.execute(qry)
        types = self.cursor.fetchall()
        types = [type[0] for type in types]

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

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names

    def table_names(self):
        self.cursor.execute(f"SELECT name FROM sqlite_master")
        table_names = self.cursor.fetchall()
        table_names = [column[0] for column in table_names]
        return table_names

    def filter_tables(self, type):
        fltr = type + "_"
        tbl_nms = list(filter(lambda nm: nm.startswith(fltr), self.tabl_nms))
        for table in self.filtered_tbls:
            tbl_nms = list(filter(lambda nm: nm != table, tbl_nms))
        return tbl_nms

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

    # Run a query to find all the object to object relations present in the data
    def get_o2o_relations(self):
        qry = f'''
                WITH ob2ob AS (
                    SELECT DISTINCT O.OCEL_TYPE AS SOURCE, O3.OCEL_TYPE AS TARGET
                    FROM OBJECT_OBJECT OO
                    JOIN OBJECT O ON O.OCEL_ID = OO.ocel_source_id
                    JOIN OBJECT O3 ON O3.OCEL_ID = OO.OCEL_TARGET_ID
                )
                SELECT M.OCEL_TYPE_MAP AS SOURCE, M2.OCEL_TYPE_MAP AS TARGET
                FROM OB2OB O
                JOIN OBJECT_MAP_TYPE M ON O.SOURCE = M.OCEL_TYPE
                JOIN OBJECT_MAP_TYPE M2 ON O.TARGET = M2.OCEL_TYPE;
               '''
        self.cursor.execute(qry)
        o2o_relations = self.cursor.fetchall()
        return o2o_relations

    # Obtain the timestamped series of events present in the event data set
    def event_log(self):
        tables = self.filter_tables('event')
        cols = set()

        # Create a list of all columns for the union table
        for table in tables:
            columns = self.col_names(table)
            for column in columns:
                cols.add(column)

        # Check each table for which columns they have
        col_names = list(cols)
        col_names.append('type')
        ev_df = pd.DataFrame(columns=col_names)

        for table in tables:
            qry_cols = ""
            columns = self.col_names(table)
            for column in cols:
                if column in columns:
                    qry_cols += f"{table}.'{column}',\n"
                else:
                    qry_cols += f"NULL as '{column}',\n"

            query = f'''
                        SELECT DISTINCT
                            {qry_cols}
                            event.ocel_type
                        FROM {table}
                        JOIN event ON {table}.ocel_id = event.ocel_id
                        ORDER BY 1;
                    '''
            self.cursor.execute(query)
            columns_info = self.cursor.fetchall()
            for column in columns_info:
                ev_df.loc[len(ev_df.index)] = column

        ev_df.to_csv(self.ev_output, sep=',', index=False)
        return ev_df

    def create_graph(self, nodes):
        all_timestamps = []
        all_idx = []
        vwpnt_cnt = 0
        all_graphs = []
        all_kpis = []

        print('Creating graphs...')
        for vwpnt_object in nodes.keys():
            vwpnt_cnt += 1
            if vwpnt_cnt % int(len(nodes.keys()) / 5) == 0:
                print(int(vwpnt_cnt * 20 / int(len(nodes.keys()) / 5)), '%')

            # Add all nodes to the graph
            graph = {}
            past_events = []

            ob_df = nodes[vwpnt_object]['related_objects']
            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])
            ev_by_ob = nodes[vwpnt_object]['events_by_objects'][0]

            # Always add the viewpoint object first
            attributes = self.get_attributes(vwpnt_object, self.viewpoint, self.attributes[self.viewpoint])
            self.tensor_dict[self.viewpoint] = len(self.attributes[self.viewpoint])
            attributes.append(vwpnt_cnt)
            attributes.append(vwpnt_object)
            graph[self.viewpoint] = [attributes]

            # Create a new graph for every event in the process
            for i, row in ev_df.iterrows():
                ev_idx = row['index']
                ev_id = row['ocel_id']
                ev_type = row['type']
                timestamp = row['timestamp']
                objects_in_event = []

                # Check if the graph already has a list for the object type and, if not, create an empty list
                try:
                    len(graph['Events']) > 0
                except KeyError:
                    graph['Events'] = []

                # Perform One Hot Encoding on the event type and add it to the graph
                encode = self.get_ev_encoding(ev_type)
                self.tensor_dict['Events'] = len(encode)
                graph['Events'].append(encode)
                all_timestamps.append(timestamp)
                all_idx.append(vwpnt_cnt)
                past_events.append(ev_idx)

                # Add the event_to_event edges to the graph
                graph['Events_to_Events'] = self.generate_adjacency_list_with_k(ev_by_ob, ev_idx)
                self.tensor_dict['Events_to_Events'] = 1

                # Add the objects related to each step
                contained = False
                tmp_graph = copy.deepcopy(graph)
                ob_types = set()

                for i, row in ev_by_ob.iterrows():
                    ob_id = row['ob_id']
                    evs_by_ob = row['events']
                    ob_type = row['ob_type']
                    ob_idx = row['index']
                    edge_type = f"{ob_type}_to_Events"

                    # Identify if the object needs to be added to the graph
                    for event_by_object in evs_by_ob:
                        if event_by_object in past_events:
                            contained = True
                            objects_in_event.append(ob_id)
                            ob_types.add(ob_type)
                            break
                        else:
                            contained = False

                    # Check if the graph already has a list for the object type and, if not, create an empty list
                    if ob_type not in tmp_graph.keys():
                        tmp_graph[ob_type] = []

                    # Create the object to event edge type
                    if edge_type not in tmp_graph.keys():
                        tmp_graph[edge_type] = [[], []]

                    # If the object needs to be added to the graph it selects how to add it
                    if contained:
                        # If the object is of the viewpoint type it is skipped
                        if ob_type != self.viewpoint:
                            # The attributes are added to the graph depending on their type.
                            if ob_type in self.attributes.keys():
                                attr = self.attributes[ob_type]
                                attributes = self.get_attributes(ob_id, ob_type, attr)
                                self.tensor_dict[ob_type] = len(attr)
                                attributes.append(ob_idx)
                                tmp_graph[ob_type].append(attributes)
                            elif ob_type in self.time_attributes.keys():
                                attr = self.time_attributes[ob_type]
                                self.tensor_dict[ob_type] = len(attr)
                                time_attr = attr[1]
                                fixed_attrs = attr[0]
                                attributes = self.get_time_attributes(ob_id, ob_type, fixed_attrs, time_attr, timestamp)
                                attributes.append(ob_idx)
                                tmp_graph[ob_type].append(attributes)
                            else:
                                if ob_type in self.to_encode:
                                    ob_id = [ob_id, self.encodings[ob_type][ob_id], ob_idx]
                                    self.tensor_dict[ob_type] = len(ob_id[1])
                                    tmp_graph[ob_type].append(ob_id)
                                else:
                                    tmp_graph[ob_type].append([ob_id, ob_idx])
                if vwpnt_cnt == 95:
                    print(tmp_graph)

        #         # Order the added objects and assign a relative index
        #         rel_indx = {}
        #         for ob_type in ob_types:
        #             tmp_graph[ob_type] = sorted(tmp_graph[ob_type], key=lambda x: (x[-1], x[0]))
        #             ob_cnt = 0
        #             for x in tmp_graph[ob_type]:
        #                 x[-1] = ob_cnt
        #                 rel_indx[x[0]] = ob_cnt
        #                 ob_cnt += 1
        #
        #         # Add the object to event edges
        #         for ob_id in objects_in_event:
        #             evs_by_ob = ev_by_ob[ob_id]['Events']
        #             ob_events = [ev for ev in evs_by_ob if ev in past_events]
        #             ob_idx = rel_indx[ob_id]
        #             ob_type = ev_by_ob[ob_id]['Type'][0]
        #             edge_type = f"{ob_type}_to_event"
        #             self.tensor_dict[edge_type] = 1
        #
        #             if len(ob_events) > 0: # Only add edge if objects are present
        #                 object = [int(ob_idx) for a in range(len(ob_events))]
        #                 tmp_graph[edge_type][0].extend(object)
        #                 tmp_graph[edge_type][1].extend(ob_events)
        #
        #         # Add object to object edge
        #         objects_in_event= ob_df.loc[ob_df['ocel_id'].isin(objects_in_event)]
        #         for relation in self.o2o_relations:
        #             ob_source = relation[0]
        #             ob_target = relation[1]
        #             sources = objects_in_event[objects_in_event['type'] == ob_source]
        #             targets = objects_in_event[objects_in_event['type'] == ob_target]
        #             edge_name = f'{ob_source}_to_{ob_target}'
        #             tmp_graph[edge_name] = [[], []]
        #             self.tensor_dict[edge_name] = 1
        #             if len(sources) > 0 and len(targets) > 0:
        #                 for i, row in sources.iterrows():
        #                     src_id = row['ocel_id']
        #                     src_index = rel_indx[src_id]
        #                     qry = f'''
        #                             SELECT
        #                                 -- OO.OCEL_SOURCE_ID,
        #                                 OO.OCEL_TARGET_ID
        #                                 -- ,M.OCEL_TYPE_MAP
        #                             FROM OBJECT_OBJECT OO
        #                             JOIN OBJECT O ON OO.ocel_target_id = O.OCEL_ID
        #                             JOIN OBJECT_MAP_TYPE M ON O.OCEL_TYPE = M.OCEL_TYPE
        #                             WHERE
        #                                 ocel_source_id = '{src_id}' AND
        #                                 M.OCEL_TYPE_MAP = '{ob_target}'
        #                            '''
        #                     self.cursor.execute(qry)
        #                     trgt_ids = self.cursor.fetchall()
        #
        #                     for trgt_id in trgt_ids:
        #                         trgt_id = trgt_id[0]
        #                         tg_info = targets[targets['ocel_id'] == trgt_id].values
        #                         if len(tg_info) > 0:
        #                             tg_index = rel_indx[trgt_id]
        #                             tmp_graph[edge_name][0].append(src_index)
        #                             tmp_graph[edge_name][1].append(tg_index)
        #         all_graphs.append(tmp_graph)
        #
        #     # Create the kpi dataframe by checking the objects to events dictionary
        #     for kpi_type in self.kpis.keys():
        #         ob_cnt = {}
        #         kpi_events = ev_df[ev_df['type'] == kpi_type]['index']
        #         kpi_ob_types = self.kpis[kpi_type]
        #
        #         for key in ev_by_ob.keys():
        #             ob_id = key
        #             evs_by_ob = ev_by_ob[ob_id]['Events']
        #             ob_type = ev_by_ob[ob_id]['Type'][0]
        #             ob_idx = int(ob_df[ob_df['ocel_id'] == ob_id]['index'].values[0])
        #
        #             for ev in evs_by_ob:
        #                 if ev in kpi_events and ob_type in kpi_ob_types:
        #                     ts = ev_df[(ev_df['type'] == kpi_type) & (ev_df['index'] == ev)]['timestamp'].values[0]
        #                     kpi = [vwpnt_cnt, kpi_type, ob_id, ob_type, ob_idx, ts]
        #                     all_kpis.append(kpi)
        #
        #                     try:
        #                         pst_cnt = ob_cnt[ob_type]
        #                         ob_cnt[ob_type] = pst_cnt + 1
        #                     except KeyError:
        #                         ob_cnt[ob_type] = 1
        #
        #         # If an object type has no direct relation to any particular event of the chosen type
        #         # assign the latest possible timestamp for that event type.
        #         for ob_type in kpi_ob_types:
        #             if ob_type not in ob_cnt.keys():
        #                 ts = ev_df[ev_df['type'] == kpi_type]['timestamp'].values[-1]
        #                 kpi = [vwpnt_cnt, kpi_type, '', ob_type, 0, ts]
        #                 all_kpis.append(kpi)
        #
        # # Export the generated files for future use
        # with open(f'{self.graph_output_path}tensor_dict.json', "w") as f:
        #     json.dump(self.tensor_dict, f)
        #
        # with open(f'{self.graph_output_path}all_graphs.json', "w") as f:
        #     json.dump(all_graphs, f)
        #
        # with open(f'{self.graph_output_path}all_timestamps.json', "w") as f:
        #     json.dump(all_timestamps, f)
        #
        # with open(f'{self.graph_output_path}all_idx.json', "w") as f:
        #     json.dump(all_idx, f)
        #
        # # Convert the lists into Numpy Arrays to make it easier to filter them later
        # all_graphs = np.array(all_graphs)
        # all_timestamps = np.array(all_timestamps)
        # all_idx = np.array(all_idx)
        # all_kpis = pd.DataFrame(all_kpis, columns=['viewpoint_id', 'kpi_type', 'ob_id', 'ob_type', 'index', 'timestamp'])
        # all_kpis.to_csv(f'{self.graph_output_path}all_kpis.csv', index=False)
        #
        # return all_graphs, all_timestamps, all_idx, all_kpis, self.tensor_dict