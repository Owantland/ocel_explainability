import sqlite3

import numpy as np
import yaml
import pandas as pd
import table_generation as tg

from collections import defaultdict


# MAIN
database = 'order_management'
tbl = tg.generateTables(database)
all_graphs = tbl.create_graph()
print(f'All Graphs: {all_graphs}')