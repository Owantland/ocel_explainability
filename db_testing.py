import process_generation as pg
import table_generation as tg
import train_test_builder as tb
import hetero_graphs as hg
import training as t
import evaluation as e

import json
import numpy as np
# MAIN
cant = 800
database = 'order_management' #'order_management'

# # Obtains all related nodes and arcs in the dataset and then generates the list of process executions
# p = pg.ProcessGeneration(database, cant)
# nodes = p.related_nodes()
# p.get_ev_log(nodes)
#
# # Obtains the graph for each process execution and its associated prefixes
# tbl = tg.GenerateTables(database, cant)
# tbl.create_graph(nodes)

# with open(f'files/graph_structures/order_management/800/all_graphs.json') as json_file:
#     all_graphs = json.load(json_file)
#     all_graphs = np.array(all_graphs)
#
# with open("files/graph_structures/order_management/800/all_timestamps.json") as json_file:
#     all_timestamps = json.load(json_file)
#     all_timestamps = np.array(all_timestamps)
#
# with open("files/graph_structures/order_management/800/all_idx.json") as json_file:
#     all_idx = json.load(json_file)
#     all_idx = np.array(all_idx)
#
# active_graph = all_graphs[all_idx == 79]
# print(active_graph)

# Apply the train test split to the set of process executions to obtain the relevant sets for learning set generation
ttb = tb.TrainTestBuilder(database, cant)
train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()

# Obtains the learning set for training, testing and validation and converts it into pytorch tensors
hgg = hg.HeteroGraphsGenerator(database, cant, train_sampled_timestamps,
                               val_sampled_timestamps, test_sampled_timestamps)
hgg.trace_kpi()

# #Model
# trainer = t.Trainer(database)
# trainer.trainer()

# # Evaluation of the model
# eval = e.Evaluation(database)
# eval.evalutaion()