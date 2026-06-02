import process_generation as pg
import table_generation as tg
import train_test_builder as tb
import hetero_generator as hg
import training as t
import evaluation as e

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

# # Apply the train test split to the set of process executions to obtain the relevant sets for learning set generation
# ttb = tb.TrainTestBuilder(database, cant)
# train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()
#
# # Obtains the learning set for training, testing and validation and converts it into pytorch tensors
# hgg = hg.HeteroGraphsGenerator(database, cant, train_sampled_timestamps,
#                                val_sampled_timestamps, test_sampled_timestamps)
# hgg.generate_graphs()
#
# # #Model
# trainer = t.Trainer(database)
# trainer.trainer()

# Evaluation of the model
eval = e.Evaluation(database)
eval.evalutaion()