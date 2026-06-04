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
                     'kpis': db_configs[self.database]['kpis'],
                     'filtered_tables': db_configs[self.database]['filtered_tables'],
                     'attributes': db_configs[self.database]['attributes'],
                     'time_attributes': db_configs[self.database]['time_attributes'],
                     'encoding': db_configs[self.database]['encoding'],
                     'kpi_type': db_configs[self.database]['kpi_type'],}

        # Calculated values
        kpi_event = [x for x in path_dict['kpis'].keys()]
        path_dict['kpi_event'] = kpi_event[0]

        # Generate the appropriate path for saving the process execution log
        path_dict['graph_output_path'] = f"{path_dict['graph_output_path']}{self.cant}/"
        path_dict['ev_log_path'] = f"{path_dict['graph_output_path']}/{path_dict['kpi_event']}"
        if not os.path.exists(path_dict['ev_log_path']):
            os.makedirs(path_dict['ev_log_path'])
        path_dict['ev_log_path'] = f"{path_dict['ev_log_path']}/ev_table.csv"

        # Generate appropriate path for saving the heterographs
        path = f"{path_dict['pytorch_path']}{path_dict['kpi_event']}"
        if not os.path.exists(path):
            os.makedirs(path)
        path_dict['hetero_path'] = path

        # Generate appropriate path for saving models
        path = f"{path_dict['model_path']}{path_dict['kpi_event']}"
        # Save the model into the appropriate directory for the KPI type selected
        if path_dict['kpi_type'] == 0:
            path = f"{path}/trace"
        else:
            path = f"{path}/prefixes"
        if not os.path.exists(path):
            os.makedirs(path)
        path_dict['model_output_path'] = path
        return path_dict

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names
