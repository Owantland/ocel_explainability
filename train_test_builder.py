import pandas as pd
import yaml

class TrainTestBuilder():
    def __init__(self, database, CANT):
        self.database = database
        self.cant = CANT
        self.num_vp_obj = self.cant
        self.get_paths()

        self.pd_active_orders = self.get_active_orders()

        # Assigns train/test split values
        self.index_train = round(0.6 * self.cant)
        self.split_test_index = .5
        self.step_size = round(0.1 * self.cant) #Step size is about 10% of the total data

    def get_active_orders(self):
        # Finds the timeframe for each order and puts them in chronological order
        # depending on when their last related event occurs
        active_orders = []
        for i in range(self.num_vp_obj):
            vwpnt_id = i+1
            temp = self.pd_df[self.pd_df['vwpnt_id'] == vwpnt_id]
            start_time = temp.iloc[0,2]

            # Select the final event of the chosen type for marking the end time of the process
            temp = temp[temp['type'] == self.end_event]
            end_time = temp.iloc[-1, 2]
            active_orders.append([vwpnt_id, start_time, end_time])

            # Curtail the dataframe to avoid events that happen after our chosen end event
            del_index = self.pd_df[(self.pd_df['vwpnt_id'] == vwpnt_id) & (self.pd_df['timestamp'] > end_time)].index
            self.pd_df = self.pd_df.drop(del_index, inplace=False)

        pd_active_orders = pd.DataFrame(active_orders)
        pd_active_orders.sort_values(by=2, inplace=True)
        return pd_active_orders

    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.output_path = db_configs[self.database]['ev_output_path']
        self.end_event = db_configs[self.database]['end_event']
        self.pd_df = pd.read_csv(self.output_path)

    def sample_equally(self, input_list, num_samples):
        # Handle edge cases
        if num_samples <= 0:
            return []
        if num_samples >= len(input_list):
            return input_list
        step = int((len(input_list) - 1) / (num_samples - 1))
        sampled_list = [input_list[i * step] for i in range(num_samples)]
        return sampled_list

    def timestamps_generator(self):
        # Finds the timestamp related to the chosen train/test split
        split_timestamp = self.pd_active_orders.iloc[self.index_train, 2]
        print(split_timestamp)

        # Finds the timestamp for the last finished event among the training data.
        # Because the testing data will only include elements from processes that occur
        # after the end of the last training data
        last_timestamp = max(self.pd_active_orders.iloc[:self.index_train, 2])
        print(last_timestamp)

        # Perform the train/test split on the data
        # Train orders are those that occur our split value
        train_orders = self.pd_active_orders[self.pd_active_orders[2] <= split_timestamp][0]
        # Test orders are those that begin after the end of the last event contained in the training data
        test_orders = self.pd_active_orders[self.pd_active_orders[1] > last_timestamp][0]

        # Splits the test orders into test and validation groups given a chosen percentage
        index_test = self.index_train + int(len(test_orders) * self.split_test_index)
        split_timestamp_val = self.pd_active_orders.iloc[index_test, 2]
        last_timestamp_val = max(self.pd_active_orders.iloc[self.index_train: index_test, 2])

        val_orders = self.pd_active_orders[
            (self.pd_active_orders[1] > last_timestamp) & (self.pd_active_orders[2] <= split_timestamp_val)][0]
        test_orders = self.pd_active_orders[self.pd_active_orders[1] > last_timestamp_val][0]

        train_timestamps = self.pd_df[self.pd_df['vwpnt_id'].isin(train_orders.values)]['timestamp'].values
        val_timestamps = self.pd_df[self.pd_df['vwpnt_id'].isin(val_orders.values)]['timestamp'].values
        test_timestamps = self.pd_df[self.pd_df['vwpnt_id'].isin(test_orders.values)]['timestamp'].values

        tot = len(train_timestamps) + len(val_timestamps) + len(test_timestamps)

        train_sampled_timestamps = self.sample_equally(train_timestamps, int(len(train_timestamps) / self.step_size))
        val_sampled_timestamps = self.sample_equally(val_timestamps, int(len(val_timestamps) / self.step_size))
        test_sampled_timestamps = self.sample_equally(test_timestamps, int(len(test_timestamps) / self.step_size))
        return train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps