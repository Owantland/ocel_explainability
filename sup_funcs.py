# A collection of support functions used by multiple scripts in the project
import sqlite3
import yaml
import pandas as pd

class support_functions():
    def __init__(self, database):
        self.database = database
        self.get_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()

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
        self.kpi_event = db_configs[self.database]['kpi_event']

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names
