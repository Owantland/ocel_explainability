# A collection of support functions used by multiple scripts in the project
import sqlite3
import yaml
import os

class SupportFunctions:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.path_dict = self.get_paths()
        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()

    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)
        path_dict = {'ocel_path': db_configs[self.database]['ocel_path'],
                     'graph_output_path': db_configs[self.database]['graph_output_path'],
                     'pytorch_path': db_configs[self.database]['pytorch_path'],
                     'model_path': db_configs[self.database]['model_output_path'],
                     'viewpoint': db_configs[self.database]['viewpoint'],
                     'depth': db_configs[self.database]['added_depth'],
                     'unique_ids': db_configs[self.database]['unique_ids'],
                     'kpi_event': db_configs[self.database]['kpi_event'],
                     'kpi_viewpoint': db_configs[self.database]['kpi_viewpoint'],
                     'filtered_tables': db_configs[self.database]['filtered_tables'],
                     'attributes': db_configs[self.database]['attributes'],
                     'time_attributes': db_configs[self.database]['time_attributes'],
                     'encoding': db_configs[self.database]['encoding'],
                     'kpi_type': db_configs[self.database]['kpi_type'],}

        # Generate the appropriate path for saving the process execution log
        path_dict['graph_output_path'] = f"{path_dict['graph_output_path']}{self.cant}/"
        if not os.path.exists(path_dict['graph_output_path']):
            os.makedirs(path_dict['graph_output_path'])
        path_dict['ev_log_path'] = f"{path_dict['graph_output_path']}/ev_log.csv"
        return path_dict

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names
