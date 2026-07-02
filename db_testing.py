import process_generation as pg
import table_generation as tg
import train_test_builder as tb
import hetero_graphs as hg
import explainer as exp
import ocel_generator as og
import training as t

import json
import numpy as np
# MAIN
cant = 2000
database = 'order_management' #'order_management'

# # Obtains all related nodes and arcs in the dataset and then generates the list of process executions
p = pg.ProcessGeneration(database, cant)
nodes = p.related_nodes()
p.get_ev_log(nodes)

# # Generate the OCEL file with relevant attributes (required when role_encoding config changes)
# g = og.Generator(database, cant)
# g.generate_ocel(nodes)

# # Apply the train test split to the set of process executions to obtain the relevant sets for learning set generation
ttb = tb.TrainTestBuilder(database, cant)
train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()

# # Obtains the learning set for training, testing and validation and converts it into pytorch tensors
hgg = hg.HeteroGraphsGenerator(database, cant, train_sampled_timestamps,
                               val_sampled_timestamps, test_sampled_timestamps)
hgg.trace_kpi()

m = t.Modelling(database, cant)
# m.sweep()
m.Modelling()

# e = exp.Explainer(database, cant)
# e.explain_feature_attribution()