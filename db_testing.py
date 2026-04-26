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
        self.ocel_path, self.ob_output, self.ev_output, self.filtered_tbls = self.obtain_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()
        self.tabl_nms = self.table_names()

    def obtain_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        ocel_path = db_configs[self.database]['ocel_path']
        ob_output = db_configs[self.database]['ob_output_path']
        ev_output = db_configs[self.database]['ev_output_path']
        filtrd_tbls = db_configs[self.database]['filtered_tables']
        return ocel_path, ob_output, ev_output, filtrd_tbls

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
            print(query)
            self.cursor.execute(query)
            columns_info = self.cursor.fetchall()
            for column in columns_info:
                ev_df.loc[len(ev_df.index)] = column

            ev_df.to_csv(self.ev_output, sep=',', index=False)

    # Obtain the objects associated with each event in a column wise placement
    def object_columns(self):
        qry = "SELECT * FROM OBJECT_MAP_TYPE"
        self.cursor.execute(qry)
        ob_types = self.cursor.fetchall()

        # Get a list of all possible objects in the database
        ev_ob = {}
        ev_ob['ocel_event_id'] = []
        for column in ob_types:
            ev_ob[column[1]] = []

        # Create a list of all events
        qry = f'''
                SELECT DISTINCT
                    E.ocel_id
                FROM EVENT E
                ORDER BY 1;
                '''
        self.cursor.execute(qry)
        columns_info = self.cursor.fetchall()

        for column in columns_info:
            ev = column[0]
            ev_ob['ocel_event_id'].append(ev)

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

        evob = pd.DataFrame.from_dict(ev_ob)

        # Now, we join this table with the event log timestamps to get the whole table

        evob.to_csv("tst.csv", sep=',', index=False)


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

# MAIN
database = 'iot'
tbl = generateTables(database)
tbl.object_columns()
# tbl.event_log()
# tbl.generate_table('event')
# tbl.generate_table('object')