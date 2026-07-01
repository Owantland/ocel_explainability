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
        db_cfg = db_configs[self.database]
        path_dict = {'ocel_path': db_cfg['ocel_path'],
                     'graph_output_path': db_cfg['graph_output_path'],
                     'pytorch_path': db_cfg['pytorch_path'],
                     'model_path': db_cfg['model_output_path'],
                     'explainer_path': db_cfg['explainer_output_path'],
                     'results_path': f"files/results.csv",
                     'viewpoint': db_cfg['viewpoint'],
                     'depth': db_cfg['added_depth'],
                     'unique_ids': db_cfg['unique_ids'],
                     'kpi_event': db_cfg['kpi_event'],
                     'kpi_viewpoint': db_cfg['kpi_viewpoint'],
                     'filtered_tables': db_cfg['filtered_tables'],
                     'attributes': db_cfg['attributes'],
                     'time_attributes': db_cfg['time_attributes'],
                     'encoding': db_cfg['encoding'],
                     'kpi_type': db_cfg['kpi_type'],
                     'role_encoding': db_cfg.get('role_encoding') or {},}

        # Generate the appropriate path for saving the process execution log
        path_dict['graph_output_path'] = f"{path_dict['graph_output_path']}{self.cant}/"
        if not os.path.exists(path_dict['graph_output_path']):
            os.makedirs(path_dict['graph_output_path'])
        path_dict['ev_log_path'] = f"{path_dict['graph_output_path']}/ev_log.csv"

        # Validate that explainer path exists
        if not os.path.exists(path_dict['explainer_path']):
            os.makedirs(path_dict['explainer_path'])
        return path_dict

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names
