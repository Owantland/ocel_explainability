import sqlite3

import numpy as np
import yaml
import pandas as pd
import table_generation as tg
import train_test_builder as tb
import hetero_generator as hg

from collections import defaultdict

# MAIN
CANT = 5
database = 'order_management'
tbl = tg.generateTables(database, CANT)
# all_graphs = tbl.create_graph()
tbl.create_graph()

# ttb = tb.TrainTestBuilder(database, CANT)
# train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()
#
# hgg = hg.HeteroGraphsGenerator(database, CANT, all_graphs, train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps)
# hgg.generate_graphs()
