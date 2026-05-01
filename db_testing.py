# File for testing the database functions
# The goal is to obtain three main databases: all_objects, all_events, all_object_events

import sqlite3
import yaml
import pandas as pd

'''
    Creating a unified Event table
'''
class generateTables():
    def __init__(self, database):
        self.database = database
        self.obtain_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()
        self.tabl_nms = self.table_names()

    def obtain_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.ocel_path = db_configs[self.database]['ocel_path']
        self.ob_output = db_configs[self.database]['ob_output_path']
        self.ev_output = db_configs[self.database]['ev_output_path']
        self.ocel_output = db_configs[self.database]['ocel_output_path']
        self.filtered_tbls = db_configs[self.database]['filtered_tables']
        self.viewpoint = db_configs[self.database]['viewpoint']
        self.depth = db_configs[self.database]['added_depth']

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
        return attrs[0]

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
                                ORDER BY 1
                                LIMIT 10;
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
                                ORDER BY 1
                                LIMIT 10;
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
                    LIMIT 10;
               '''
        self.cursor.execute(qry)
        vwpnt_objects = self.cursor.fetchall()

        rltd_nodes = {}
        # For each viewpoint object obtain a list of related objects
        for vwpnt_object in vwpnt_objects:
            rltd_objects = set()
            rltd_objects.add(vwpnt_object[0])
            rltd_nodes[vwpnt_object[0]] = {'related_objects':[], 'related_events':[]}
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
                # Obtain the event_id and its type
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

            # Obtain a list of all objects related to the events
            rltd_objects = set()
            for event in rltd_events:
                ev_id = event[0]
                cols = self.col_names('event_object')
                ob_cols = self.col_names('object')
                mp_cols = self.col_names('object_map_type')

                qry = f'''
                            SELECT EO.{cols[1]}, M.{mp_cols[1]}
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
            rltd_nodes[vwpnt_object[0]]['related_events'].extend(rltd_events)
            rltd_nodes[vwpnt_object[0]]['related_objects'].extend(rltd_objects)
        return rltd_nodes

    def create_graph(self):
        nodes = self.related_nodes()

        for vwpnt_object in nodes.keys():
            graph = {}
            rltd_objects = nodes[vwpnt_object]['related_objects']
            rltd_events = nodes[vwpnt_object]['related_events']

            for rltd_object in rltd_objects:
                ob_id = rltd_object[0]
                ob_type = rltd_object[1]

                # Check if the graph already has a list for the object type and, if not, create an empty list
                try:
                    len(graph[ob_type]) > 0
                except KeyError:
                    graph[ob_type] = []

                # Add the desired attributes for each object type
                if ob_type == 'Items':
                    attributes = self.get_attributes(ob_id, ob_type, ['weight', 'price'])
                    ob = []
                    ob.extend(attributes)
                    ob.append(ob_id)
                    graph[ob_type].append(ob)
                elif ob_type == 'Orders':
                    attributes = self.get_attributes(ob_id, ob_type, ['price', 'ocel_time'])
                    ob = []
                    ob.extend(attributes)
                    ob.append(ob_id)
                    graph[ob_type].append(ob)
                else:
                    graph[ob_type].append([ob_id])


            for rltd_event in sorted(rltd_events, key=lambda x: x[2]):
                ev_id = rltd_event[0]
                ev_type = rltd_event[1]
                timestamp = rltd_event[2]

                # Check if the graph already has a list for the object type and, if not, create an empty list
                try:
                    len(graph['Events']) > 0
                except KeyError:
                    graph['Events'] = []

                # Perform One Hot Encoding on the event type and add it to the graph
                encode = self.get_ev_encoding(ev_type)
                encode.append(ev_id)
                encode.append(timestamp)
                graph['Events'].append(encode)
            print(graph)

# MAIN
database = 'order_management'
tbl = generateTables(database)
# tbl.related_nodes()
tbl.create_graph()
# tbl.generate_ocel()