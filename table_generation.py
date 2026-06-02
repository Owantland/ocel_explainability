# File for testing the database functions
# The goal is to obtain three main databases: all_objects, all_events, all_object_events

import sqlite3
import json
import pandas as pd
import copy
import sup_funcs as sf

from collections import defaultdict

'''
    Creating a unified Event table
'''
class GenerateTables:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sf.SupportFunctions(database)
        self.path_dict = self.funcs.get_paths()

        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()
        self.tabl_nms = self.table_names()
        self.o2o_relations = self.get_o2o_relations()
        self.encodings = self.get_encodings()

        # Creates dictionary of selected attributes for chosen object types
        self.ob_attributes = self.get_attributes()

        # Creates the object to object dataframe
        self.o2o_df =  self.get_o2o_df()

        # Dictionary of object sizes for future tensor creation
        self.tensor_dict = {}

    def get_encodings(self):
        encodings = {}
        for encoding in self.path_dict['encoding']:
            encod_dict = self.get_1h_encoding(encoding)
            encodings[encoding] = encod_dict
        return encodings

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
            attrs = pd.DataFrame(attrs, columns=cols)
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
            ob_df = nodes[vwpnt_object]['related_objects'][0]
            ev_by_ob = nodes[vwpnt_object]['events_by_objects'][0]

            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])

            # Always add the viewpoint object first
            attr_cols = self.path_dict['attributes'][self.path_dict['viewpoint']]
            attr_df = self.ob_attributes[self.path_dict['viewpoint']]
            attributes = list(attr_df[attr_df['ob_id'] == vwpnt_object].values[0])
            self.tensor_dict[self.path_dict['viewpoint']] = len(attr_cols)
            attributes.append(vwpnt_cnt)
            attributes.append(vwpnt_object)
            graph[self.path_dict['viewpoint']] = [attributes]

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
                        if ob_type != self.path_dict['viewpoint']:
                            # The attributes are added to the graph depending on their type.
                            if ob_type in self.path_dict['attributes'].keys():
                                attr_df = self.ob_attributes[ob_type]
                                attributes = list(attr_df[attr_df['ob_id'] == ob_id].values[0])
                                self.tensor_dict[ob_type] = len(attributes) - 1
                                attributes.append(ob_idx)
                                tmp_graph[ob_type].append(attributes)
                            elif ob_type in self.path_dict['time_attributes'].keys():
                                attr = self.path_dict['time_attributes'][ob_type]
                                self.tensor_dict[ob_type] = len(attr)
                                time_attr = attr[1]
                                fixed_attrs = attr[0]
                                attributes = self.get_time_attributes(ob_id, ob_type, fixed_attrs, time_attr, timestamp)
                                attributes.append(ob_idx)
                                tmp_graph[ob_type].append(attributes)
                            else:
                                if ob_type in self.path_dict['encoding']:
                                    ob_id = [ob_id, self.encodings[ob_type][ob_id], ob_idx]
                                    self.tensor_dict[ob_type] = len(ob_id[1])
                                    tmp_graph[ob_type].append(ob_id)
                                else:
                                    tmp_graph[ob_type].append([ob_id, ob_idx])

                # Order the added objects and assign a relative index
                rel_indx = {}
                for ob_type in ob_types:
                    tmp_graph[ob_type] = sorted(tmp_graph[ob_type], key=lambda x: (x[-1], x[0]))
                    ob_cnt = 0
                    for x in tmp_graph[ob_type]:
                        x[-1] = ob_cnt
                        rel_indx[x[0]] = ob_cnt
                        ob_cnt += 1

                # Add the object to event edges
                for ob_id in objects_in_event:
                    evs_by_ob = list(ev_by_ob[ev_by_ob['ob_id'] == ob_id]['events'].values[0])
                    ob_events = [ev for ev in evs_by_ob if ev in past_events]
                    ob_idx = rel_indx[ob_id]
                    ob_type = ev_by_ob[ev_by_ob['ob_id'] == ob_id]['ob_type'].values[0]
                    edge_type = f"{ob_type}_to_Events"
                    self.tensor_dict[edge_type] = 1

                    if len(ob_events) > 0: # Only add edge if objects are present
                        object = [int(ob_idx) for a in range(len(ob_events))]
                        tmp_graph[edge_type][0].extend(object)
                        tmp_graph[edge_type][1].extend(ob_events)

                # Add object to object edge
                objects_in_event= ob_df.loc[ob_df['ocel_id'].isin(objects_in_event)]
                for relation in self.o2o_relations:
                    ob_source = relation[0]
                    ob_target = relation[1]
                    sources = objects_in_event[objects_in_event['type'] == ob_source]
                    targets = objects_in_event[objects_in_event['type'] == ob_target]
                    edge_name = f'{ob_source}_to_{ob_target}'
                    tmp_graph[edge_name] = [[], []]
                    self.tensor_dict[edge_name] = 1
                    if len(sources) > 0 and len(targets) > 0:
                        for i, row in sources.iterrows():
                            src_id = row['ocel_id']
                            src_index = rel_indx[src_id]
                            trgt_ids = self.o2o_df[self.o2o_df['src_id'] == src_id]
                            trgt_ids = trgt_ids[trgt_ids['trgt_type'] == ob_target]['trgt_id'].tolist()

                            for trgt_id in trgt_ids:
                                tg_info = targets[targets['ocel_id'] == trgt_id].values
                                if len(tg_info) > 0:
                                    tg_index = rel_indx[trgt_id]
                                    tmp_graph[edge_name][0].append(src_index)
                                    tmp_graph[edge_name][1].append(tg_index)
                all_graphs.append(tmp_graph)

            # Create the kpi dataframe by checking the objects to events dictionary
            for kpi_type in self.path_dict['kpis'].keys():
                ob_cnt = {}
                kpi_events = ev_df[ev_df['type'] == kpi_type]['index']
                kpi_ob_types = self.path_dict['kpis'][kpi_type]

                for i, row in ev_by_ob.iterrows():
                    ob_id = row['ob_id']
                    evs_by_ob = row['events']
                    ob_type = row['ob_type']
                    ob_idx = row['index']

                    for ev in evs_by_ob:
                        if ev in kpi_events and ob_type in kpi_ob_types:
                            ts = ev_df[(ev_df['type'] == kpi_type) & (ev_df['index'] == ev)]['timestamp'].values[0]
                            kpi = [vwpnt_cnt, kpi_type, ob_id, ob_type, ob_idx, ts]
                            all_kpis.append(kpi)

                            try:
                                pst_cnt = ob_cnt[ob_type]
                                ob_cnt[ob_type] = pst_cnt + 1
                            except KeyError:
                                ob_cnt[ob_type] = 1

                # If an object type has no direct relation to any particular event of the chosen type
                # assign the latest possible timestamp for that event type.
                for ob_type in kpi_ob_types:
                    if ob_type not in ob_cnt.keys():
                        ts = ev_df[ev_df['type'] == kpi_type]['timestamp'].values[-1]
                        kpi = [vwpnt_cnt, kpi_type, '', ob_type, 0, ts]
                        all_kpis.append(kpi)

        # Export the generated files for future use
        with open(f"{self.path_dict['graph_output_path']}tensor_dict.json", "w") as f:
            json.dump(self.tensor_dict, f)

        with open(f"{self.path_dict['graph_output_path']}all_graphs.json", "w") as f:
            json.dump(all_graphs, f)

        with open(f"{self.path_dict['graph_output_path']}all_timestamps.json", "w") as f:
            json.dump(all_timestamps, f)

        with open(f"{self.path_dict['graph_output_path']}all_idx.json", "w") as f:
            json.dump(all_idx, f)

        # Convert the lists into Numpy Arrays to make it easier to filter them later
        all_kpis = pd.DataFrame(all_kpis, columns=['viewpoint_id', 'kpi_type', 'ob_id', 'ob_type', 'index', 'timestamp'])
        all_kpis.to_csv(f"{self.path_dict['graph_output_path']}all_kpis.csv", index=False)