import table_generation as tg
import train_test_builder as tb
import hetero_generator as hg
import training as t

# MAIN
CANT = 800
database = 'order_management'

tbl = tg.generateTables(database, CANT)
all_graphs, all_timestamps, all_idx, all_kpis, tensor_dict = tbl.create_graph()

# timestamp = '2023-05-13 16:52:54'
# print(all_graphs[(all_timestamps <= timestamp) & (all_idx == 95)])

ttb = tb.TrainTestBuilder(database, CANT)
train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()

hgg = hg.HeteroGraphsGenerator(database, CANT, all_graphs, all_timestamps, all_idx, all_kpis, tensor_dict,
                               train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps)
hgg.generate_graphs()

# trainer = t.Trainer()
# trainer.trainer('Order')