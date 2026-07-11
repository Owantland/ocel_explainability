# A collection of support functions used by multiple scripts in the project
import re
import sqlite3
import yaml
import os

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def _validate_identifier(name):
    """SQLite can only bind values, not table/column identifiers, so any name pulled from the
    database (event types, config-driven viewpoints) and interpolated into a query string is
    checked against an allow-list pattern first."""
    if not _IDENTIFIER_RE.match(str(name)):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name

_REQUIRED_KEYS = ['ocel_path', 'graph_output_path', 'pytorch_path', 'model_output_path',
                  'explainer_output_path', 'viewpoint', 'added_depth', 'unique_ids',
                  'kpi_event', 'kpi_viewpoint', 'filtered_tables', 'attributes',
                  'time_attributes', 'encoding', 'kpi_type']

def _require(cfg, key, database):
    if key not in cfg:
        raise KeyError(f"config.yml['{database}'] is missing required key '{key}'")
    return cfg[key]

class SupportFunctions:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.path_dict = self.get_paths()
        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()

    def get_paths(self):
        """Builds the path/config dict for this database. Side effect: also creates the
        graph_output_path/pytorch_path/explainer_path directories on disk if they don't
        already exist, every time this is called -- every caller in the codebase relies
        on this happening as part of construction."""
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)
        db_cfg = db_configs[self.database]
        path_dict = {key: _require(db_cfg, key, self.database) for key in _REQUIRED_KEYS}
        path_dict['model_path'] = path_dict.pop('model_output_path')
        path_dict['explainer_path'] = path_dict.pop('explainer_output_path')
        path_dict['depth'] = path_dict.pop('added_depth')
        path_dict['results_path'] = "files/results.csv"
        path_dict['role_encoding'] = db_cfg.get('role_encoding') or {}

        # Generate the appropriate path for saving the process execution log
        path_dict['graph_output_path'] = f"{path_dict['graph_output_path']}{self.cant}/"
        if not os.path.exists(path_dict['graph_output_path']):
            os.makedirs(path_dict['graph_output_path'])
        path_dict['ev_log_path'] = f"{path_dict['graph_output_path']}/ev_log.csv"

        # Namespace hetero-graph and model artifacts by cant too, so rerunning at a
        # different dataset size doesn't silently overwrite a previous size's outputs.
        path_dict['pytorch_path'] = f"{path_dict['pytorch_path']}{self.cant}/"
        if not os.path.exists(path_dict['pytorch_path']):
            os.makedirs(path_dict['pytorch_path'])
        path_dict['model_path'] = f"{path_dict['model_path']}{self.cant}/"

        # Validate that explainer path exists
        if not os.path.exists(path_dict['explainer_path']):
            os.makedirs(path_dict['explainer_path'])
        return path_dict

    def col_names(self, table_name):
        self.cursor.execute(f"PRAGMA table_info({_validate_identifier(table_name)});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names
