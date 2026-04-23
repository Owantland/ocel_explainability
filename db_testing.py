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

    def generate_event_objects_table(self):
        # Get a list of all object tables
        tabl_nms = self.table_names()
        tabl_nms = list(filter(lambda nm: nm.startswith('object_'), tabl_nms))
        tabl_nms = list(filter(lambda nm: nm != 'object_map_type', tabl_nms))
        tables = list(filter(lambda nm: nm != 'object_object', tabl_nms))
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
database = 'logistics'
tbl = generateTables(database)
tbl.generate_table('event')
tbl.generate_table('object')
# tbl.generate_events_table()
# tbl.generate_objects_table()