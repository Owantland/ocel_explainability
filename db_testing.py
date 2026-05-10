import sqlite3

import numpy as np
import yaml
import pandas as pd
import table_generation as tg
# import train_test_builder as tb
from collections import defaultdict


# MAIN
CANT = 200
database = 'order_management'
tbl = tg.generateTables(database)
all_graphs = tbl.create_graph()
# print(f'All Graphs: {all_graphs}')

# ttb = tb.TrainTestBuilder(CANT)
# train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()