# File for testing the database functions
# The goal is to obtain three main databases: all_objects, all_events, all_object_events

import sqlite3

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

    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.ocel_path = db_configs[self.database]['ocel_path']
        self.ob_output = db_configs[self.database]['ob_output_path']
        self.ev_output = db_configs[self.database]['ev_output_path']
        self.ocel_output = db_configs[self.database]['ocel_output_path']
        self.filtered_tbls = db_configs[self.database]['filtered_tables']
        self.viewpoint = db_configs[self.database]['viewpoint']
        self.depth = db_configs[self.database]['added_depth']
        self.attributes = db_configs[self.database]['attributes']
        self.time_attributes = db_configs[self.database]['time_attributes']
        self.to_encode = db_configs[self.database]['encoding']
        self.kpis = db_configs[self.database]['kpis']

    def get_encodings(self):
        self.encodings = {}
        for encoding in self.to_encode:
            encod_dict = self.get_1h_encoding(encoding)
            self.encodings[encoding] = encod_dict

    def get_attributes(self, node_id, type, attributes):
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
                SELECT {attributes}
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

    def generate_adjacency_list_with_k(self, events_by_objects_copy, par):
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
        for k, v in events_by_objects_copy.items():
            v = v['Events']
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

    # Obtain the objects associated with each event in a column wise placement
    def generate_ocel(self):
        qry = "SELECT * FROM OBJECT_MAP_TYPE"
        self.cursor.execute(qry)
        ob_types = self.cursor.fetchall()

        # Get a list of all possible objects in the database
        ev_ob = {}
        ev_ob['ocel_id'] = []
        ev_ob['timestamp'] = []
        for column in ob_types:
            ev_ob[column[1]] = []

        # Create a list of all events
        ev_log = self.event_log()
        # ev_log  = pd.read_csv(self.ev_output)
        events = ev_log['ocel_id']
        timestamps = ev_log['ocel_time']


        for idx, ev in enumerate(events):
            timestamp = timestamps[idx]
            ev_ob['ocel_id'].append(ev)
            ev_ob['timestamp'].append(timestamp)

            ev_dict = {}
            for ob in ob_types:
                ev_dict[ob[1]] = []

            qry = f'''
                    SELECT DISTINCT
                        EO.OCEL_EVENT_ID,
                        O.OCEL_ID,
                        OCEL_TYPE_MAP
                    FROM event_object EO
                    JOIN OBJECT O ON EO.ocel_object_id = O.ocel_id
                    JOIN object_map_type OM ON O.OCEL_TYPE = OM.OCEL_TYPE
                    WHERE EO.OCEL_EVENT_ID = '{ev}'
                    '''
            self.cursor.execute(qry)
            columns_info = self.cursor.fetchall()

            seen_events = []
            for column in columns_info:
                ob_id = column[1]
                ob_type = column[2]

                ev_dict[ob_type].append(ob_id)

            for ob in ob_types:
                ob_type = ob[1]
                ev_ob[ob_type].append(ev_dict[ob_type])

        ev_ob = pd.DataFrame.from_dict(ev_ob)
        ev_ob.to_csv(self.ocel_output, sep=',', index=False)

    def generate_table(self, type):
        # Get a list of all object tables
        tables = self.filter_tables(type)
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
                                SELECT
                                    {qry_cols}
                                    {type}.ocel_type
                                FROM {table}
                                JOIN {type} ON {table}.ocel_id = {type}.ocel_id
                                ORDER BY 1;
                            '''
            self.cursor.execute(query)
            columns_info = self.cursor.fetchall()
            for column in columns_info:
                ev_df.loc[len(ev_df.index)] = column
            if type == 'event':
                ev_df.to_csv(self.ev_output, sep=',', index=False)
            else:
                ev_df.to_csv(self.ob_output, sep=',', index=False)

    def generate_join_tables(self):
        # List the tables for relations
        tables = ['object_object', 'event_object']
        cols = set()

        # Get the set of columns that the big table needs to have
        for table in tables:
            columns = self.col_names(table)
            for column in columns:
                cols.add(column)

        # Check each table for which columns they have
        col_names = list(cols)
        col_names.append('type')
        ob_df = pd.DataFrame(columns=col_names)
        for table in tables:
            qry_cols = ""
            columns = self.col_names(table)
            for column in cols:
                if column in columns:
                    qry_cols += f"{table}.'{column}',\n"
                else:
                    qry_cols += f"NULL as '{column}',\n"

            query = f'''
                                SELECT
                                    {qry_cols}
                                    object.ocel_type
                                FROM {table}
                                JOIN OBJECT ON {table}.ocel_id = OBJECT.ocel_id
                                ORDER BY 1;
                            '''
            self.cursor.execute(query)
            columns_info = self.cursor.fetchall()
            for column in columns_info:
                ob_df.loc[len(ob_df.index)] = column
        ob_df.to_csv(self.ob_output, sep=',', index=False)

    def related_nodes(self):
        # Generate a list of all objects of the chosen viewpoint
        qry = f'''
                    SELECT *
                    FROM OBJECT_{self.viewpoint}
                    ORDER BY 1
                    LIMIT {self.cant};
               '''
        self.cursor.execute(qry)
        vwpnt_objects = self.cursor.fetchall()

        rltd_nodes = {}
        event_log = []
        # For each viewpoint object obtain a list of related objects
        for vwpnt_object in vwpnt_objects:
            rltd_objects = set()
            rltd_objects.add(vwpnt_object[0])
            rltd_nodes[vwpnt_object[0]] = {'related_objects':[], 'related_events':[], 'events_by_objects':[]}
            cols = self.col_names('object_object')
            qry = f'''
                        SELECT *
                        FROM OBJECT_OBJECT
                        WHERE {cols[0]} = '{vwpnt_object[0]}' 
                        ORDER BY 1;
                   '''
            self.cursor.execute(qry)
            objects = self.cursor.fetchall()
            for rltd_object in objects:
                rltd_objects.add(rltd_object[1])
                rltd_objects.add(rltd_object[0])

                if self.depth:
                    qry = f'''
                                SELECT *
                                FROM OBJECT_OBJECT
                                WHERE {cols[0]} = '{rltd_object[1]}'
                                      OR {cols[1]} = '{rltd_object[1]}'
                                ORDER BY 1;
                           '''
                    self.cursor.execute(qry)
                    objects = self.cursor.fetchall()
                    for rltd_object in objects:
                        rltd_objects.add(rltd_object[1])
                        rltd_objects.add(rltd_object[0])

            # Generate a list of related events to the viewpoint object
            rltd_events = set()
            for rltd_object in rltd_objects:
                cols = self.col_names('event_object')
                ev_cols = self.col_names('event')
                mp_cols = self.col_names('event_map_type')
                # Obtain the event_id and its type as well as it's index in the timeline
                qry = f'''
                            SELECT EO.{cols[0]}, M.{mp_cols[1]} 
                            FROM EVENT_OBJECT EO
                            JOIN EVENT E ON EO.{cols[0]} = E.{ev_cols[0]}
                            JOIN EVENT_MAP_TYPE M ON E.{ev_cols[1]} = M.{mp_cols[0]}
                            WHERE EO.{cols[1]} = '{rltd_object}'
                            ORDER BY 1;
                       '''
                self.cursor.execute(qry)
                events = self.cursor.fetchall()

                # Add a timestamp to each event
                for event in events:
                    ev_id = event[0]
                    ev_type = event[1]
                    ev_table = f'event_{ev_type}'
                    cols = self.col_names(ev_table)

                    qry = f'''
                                SELECT {cols[-1]}
                                FROM {ev_table} E
                                WHERE E.{cols[0]} = '{ev_id}'
                           '''
                    self.cursor.execute(qry)
                    timestamp = self.cursor.fetchall()
                    timestamp = timestamp[0][0]
                    event = (ev_id, ev_type, timestamp)
                    rltd_events.add(event)

            # Sort the events chronologically and add an index
            rltd_events = sorted(rltd_events, key=lambda x: x[2])
            for idx, event in enumerate(rltd_events):
                ev_id = event[0]
                ev_type = event[1]
                ev_timestamp = event[2]
                rltd_events[idx] = (idx, ev_id, ev_type, ev_timestamp)

            # Obtain a list of all objects related to the events
            rltd_objects = set()
            events_by_objects = {}
            for event in rltd_events:
                ev_idx = event[0]
                ev_id = event[1]
                cols = self.col_names('event_object')
                ob_cols = self.col_names('object')
                mp_cols = self.col_names('object_map_type')

                qry = f'''
                            SELECT DISTINCT EO.{cols[1]}, M.{mp_cols[1]}
                            FROM EVENT_OBJECT EO
                            JOIN OBJECT O ON EO.{cols[1]} = O.{ob_cols[0]}
                            JOIN OBJECT_MAP_TYPE M ON O.{ob_cols[1]} = M.{mp_cols[0]}
                            WHERE {cols[0]} = '{ev_id}'
                            ORDER BY 1;
                       '''
                self.cursor.execute(qry)
                objects = self.cursor.fetchall()
                for object in objects:
                    rltd_objects.add(object)

                    # # Get a list of related events to each object
                    obj = object[0]
                    obj_type = object[1]
                    if obj not in events_by_objects:
                        events_by_objects[obj] = {}
                        events_by_objects[obj]['Events'] = [ev_idx]
                        events_by_objects[obj]['Type'] = [obj_type]
                    else:
                        events_by_objects[obj]['Events'].append(ev_idx)

            # Order the object list and add an index
            sorted_objects = sorted(rltd_objects, key=lambda x: (x[1], x[0]))
            pst_type = ''
            ob_index = 0
            rltd_objects = set()
            for rltd_object in sorted_objects:
                ob_type = rltd_object[1]
                if ob_type != pst_type:
                    ob_index = 0
                    pst_type = ob_type
                else:
                    ob_index += 1
                object = (ob_index, rltd_object[0], rltd_object[1])
                rltd_objects.add(object)
            rltd_objects = sorted(rltd_objects, key=lambda x: (x[2], x[1]))

            # Update the dictionary
            rltd_nodes[vwpnt_object[0]]['related_events'].extend(rltd_events)
            rltd_nodes[vwpnt_object[0]]['related_objects'].extend(rltd_objects)
            rltd_nodes[vwpnt_object[0]]['events_by_objects'].append(events_by_objects)
        return rltd_nodes

    def create_graph(self):
        nodes = self.related_nodes()
        all_timestamps = []
        all_idx = []
        vwpnt_cnt = 0
        log_frames = []
        all_graphs = []
        kpi_dict = {}

        for vwpnt_object in nodes.keys():
            # Add all nodes to the graph
            graph = {}
            past_events = []

            rltd_objects = nodes[vwpnt_object]['related_objects']
            ob_df = pd.DataFrame(rltd_objects, columns=['index', 'ocel_id', 'type'])

            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])

            ev_by_ob = nodes[vwpnt_object]['events_by_objects'][0]

            # Create the event log file
            vwpnt_cnt += 1
            id_col = [vwpnt_cnt for _ in range(len(ev_df.index))]
            ob_id = [vwpnt_object for _ in range(len(ev_df.index))]
            ev_log = ev_df[['ocel_id', 'type', 'timestamp']]
            ev_log['vwpnt_id'] = id_col
            ev_log['ob_id'] = ob_id
            log_frames.append(ev_log)

            # Always add the viewpoint object first
            attributes = self.get_attributes(vwpnt_object, self.viewpoint, self.attributes[self.viewpoint])
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
                # encode = self.get_ev_encoding(ev_type)
                # encode.append(ev_id)
                encode = [ev_type, timestamp]
                graph['Events'].append(encode)
                all_timestamps.append(timestamp)
                all_idx.append(vwpnt_cnt)
                past_events.append(ev_idx)

                # Add the event_to_event edges to the graph
                graph['event_to_event'] = self.generate_adjacency_list_with_k(ev_by_ob, ev_idx)

                # We've got the events step by step, now we add the objects related to each step
                contained = False
                tmp_graph = copy.deepcopy(graph)
                ob_types = set()
                for key in ev_by_ob.keys():
                    ob_id = key
                    evs_by_ob = ev_by_ob[ob_id]['Events']
                    ob_type = ev_by_ob[ob_id]['Type'][0]
                    ob_idx = int(ob_df[ob_df['ocel_id'] == ob_id]['index'].values[0])

                    # Add the objects related to the event step
                    for event_by_object in evs_by_ob:
                        if event_by_object in past_events:
                            contained = True
                            objects_in_event.append(ob_id)
                            ob_types.add(ob_type)
                            break
                        else:
                            contained = False

                    if contained:
                        if ob_type == self.viewpoint:
                            pass
                        else:
                            # Check if the graph already has a list for the object type and, if not, create an empty list
                            try:
                                len(tmp_graph[ob_type]) > 0
                            except KeyError:
                                tmp_graph[ob_type] = []

                            # Add the desired attributes for each object type
                            # Need to add time sensitive attributes like the one for product
                            try:
                                attr = self.attributes[ob_type]
                                attributes = self.get_attributes(ob_id, ob_type, attr)
                                attributes.append(ob_idx)
                                tmp_graph[ob_type].append(attributes)
                            except KeyError:
                                try:
                                    attr = self.time_attributes[ob_type]
                                    time_attr = attr[1]
                                    fixed_attrs = attr[0]
                                    attributes = self.get_time_attributes(ob_id, ob_type, fixed_attrs, time_attr, timestamp)
                                    attributes.append(ob_idx)
                                    tmp_graph[ob_type].append(attributes)
                                except KeyError:
                                    if ob_type in self.to_encode:
                                        ob_id = self.encodings[ob_type][ob_id]
                                        tmp_graph[ob_type].extend([ob_id, ob_idx])
                                    else:
                                        tmp_graph[ob_type].append([ob_id, ob_idx])

                    # Add the object to event edges
                    ob_events = [ev for ev in evs_by_ob if ev in past_events]
                    if len(ob_events) > 0: # Only add edge if objects are present
                        object = [int(ob_idx) for a in range(len(ob_events))]
                        edge_type = f"{ob_type}_to_event"

                        # If edge type doesn't exist then it's created
                        try:
                            len(tmp_graph[edge_type]) > 0
                        except KeyError:
                            tmp_graph[edge_type] = [[], []]

                        tmp_graph[edge_type][0].extend(object)
                        tmp_graph[edge_type][1].extend(ob_events)

                # Sort the objects according to their index
                for ob_type in ob_types:
                    tmp_graph[ob_type] = sorted(tmp_graph[ob_type], key=lambda x: (x[-1], x[0]))
                    tmp_graph[ob_type] = [x[:-1] for x in tmp_graph[ob_type]]

                # Add object to object edge
                objects_in_event= ob_df.loc[ob_df['ocel_id'].isin(objects_in_event)]
                for relation in self.o2o_relations:
                    ob_source = relation[0]
                    ob_target = relation[1]
                    sources = objects_in_event[objects_in_event['type'] == ob_source]
                    targets = objects_in_event[objects_in_event['type'] == ob_target]

                    if len(sources) > 0 and len(targets) > 0:
                        edge_name = f'{ob_source}_to_{ob_target}'
                        tmp_graph[edge_name] = [[],[]]

                        for i, row in sources.iterrows():
                            src_id = row['ocel_id']
                            src_index = row['index']
                            qry = f'''
                                    SELECT
                                        -- OO.OCEL_SOURCE_ID,
                                        OO.OCEL_TARGET_ID
                                        -- ,M.OCEL_TYPE_MAP
                                    FROM OBJECT_OBJECT OO
                                    JOIN OBJECT O ON OO.ocel_target_id = O.OCEL_ID
                                    JOIN OBJECT_MAP_TYPE M ON O.OCEL_TYPE = M.OCEL_TYPE
                                    WHERE
                                        ocel_source_id = '{src_id}' AND
                                        M.OCEL_TYPE_MAP = '{ob_target}'
                                   '''
                            self.cursor.execute(qry)
                            trgt_ids = self.cursor.fetchall()

                            for trgt_id in trgt_ids:
                                trgt_id = trgt_id[0]
                                tg_info = targets[targets['ocel_id'] == trgt_id].values
                                if len(tg_info) > 0:
                                    tg_index = tg_info[0][0]
                                    tmp_graph[edge_name][0].append(src_index)
                                    tmp_graph[edge_name][1].append(tg_index)
                # Add the event as a step
                all_graphs.append(tmp_graph)

            # Create the kpi dictionary by checking the objects to events dictionary
            kpi_dict[vwpnt_cnt] = {}
            for kpi_type in self.kpis.keys():
                try:
                    len(kpi_dict[vwpnt_cnt][kpi_type]) > 0
                except KeyError:
                    kpi_dict[vwpnt_cnt][kpi_type] = []
                kpi_events = ev_df[ev_df['type'] == kpi_type]['index']
                kpi_ob_types = self.kpis[kpi_type]

                for key in ev_by_ob.keys():
                    ob_id = key
                    evs_by_ob = ev_by_ob[ob_id]['Events']
                    ob_type = ev_by_ob[ob_id]['Type'][0]
                    ob_idx = int(ob_df[ob_df['ocel_id'] == ob_id]['index'].values[0])

                    for ev in evs_by_ob:
                        if ev in kpi_events and ob_type in kpi_ob_types:
                            ts = ev_df[(ev_df['type'] == kpi_type) & (ev_df['index'] == ev)]['timestamp'].values[0]
                            kpi = {'Type': ob_type, 'Index': ob_idx, 'Timestamp': ts}
                            kpi_dict[vwpnt_cnt][kpi_type].append(kpi)


        ev_log = pd.concat(log_frames)
        ev_log.to_csv(self.ev_output, index=False)

        # Convert the lists into Numpy Arrays to make it easier to filter them later
        all_graphs = np.array(all_graphs)
        all_timestamps = np.array(all_timestamps)
        all_idx = np.array(all_idx)
        return all_graphs, all_timestamps, all_idx, kpi_dict