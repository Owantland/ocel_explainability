# Given an OCEL 2.0 standard database and a viewpoint, obtain all relevant objects and events for each viewpoint object
# and create a simplified list of process executions.

import sqlite3
import pandas as pd
import sup_funcs as sup
import os

'''
    Creating a unified Event table
'''
class ProcessGeneration:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sup.SupportFunctions(database)
        self.path_dict = self.funcs.get_paths()
        conn = sqlite3.connect(self.path_dict['ocel_path'])
        self.cursor = conn.cursor()

    def get_data_dictionaries(self):
        # Generate a list of object to object relationships
        cols = self.funcs.col_names('object_object')
        qry = f'''
                    SELECT O_O.{cols[0]}, O_O.{cols[1]}, OM.OCEL_TYPE_MAP, TM.OCEL_TYPE_MAP
                    FROM OBJECT_OBJECT O_O
                    JOIN OBJECT O ON O.OCEL_ID = O_O.ocel_source_id
                    JOIN OBJECT T ON T.OCEL_ID = O_O.ocel_target_id
                    JOIN OBJECT_MAP_TYPE OM ON O.OCEL_TYPE = OM.OCEL_TYPE
                    JOIN OBJECT_MAP_TYPE TM ON T.OCEL_TYPE = TM.OCEL_TYPE
                    ORDER BY 1;
               '''
        self.cursor.execute(qry)
        objects = self.cursor.fetchall()
        o2o_table = pd.DataFrame.from_records(objects, columns=['src_id', 'trgt_id', 'src_type', 'trgt_type'])

        # Obtain events to objects table
        cols = self.funcs.col_names('event_object')
        ev_cols = self.funcs.col_names('event')
        me_cols = self.funcs.col_names('event_map_type')
        ob_cols = self.funcs.col_names('object')
        mo_cols = self.funcs.col_names('object_map_type')

        # Obtain the event_id and its type as well as it's index in the timeline
        qry = f'''
                    SELECT DISTINCT EO.{cols[0]}, EO.{cols[1]}, ME.{me_cols[1]}, MO.{mo_cols[1]}
                    FROM EVENT_OBJECT EO
                    JOIN EVENT E ON EO.{cols[0]} = E.{ev_cols[0]}
                    JOIN EVENT_MAP_TYPE ME ON E.{ev_cols[1]} = ME.{me_cols[0]}
                    JOIN OBJECT O ON EO.{cols[1]} = O.{ob_cols[0]}
                    JOIN OBJECT_MAP_TYPE MO ON O.{ob_cols[1]} = MO.{mo_cols[0]}
                    ORDER BY 1;
               '''
        self.cursor.execute(qry)
        events = self.cursor.fetchall()
        e20_table = pd.DataFrame(events, columns=['ev_id', 'ob_id', 'ev_type', 'ob_type'])

        # Obtain all relevant timestamps for every event type
        ev_tables = {}
        for i, row in e20_table.iterrows():
            ev_type = row['ev_type']
            if ev_type not in ev_tables.keys():
                ev_tables[ev_type] = []
                ev_table = f'event_{ev_type}'
                cols = self.funcs.col_names(ev_table)

                qry = f'''
                            SELECT E.{cols[0]}, {cols[-1]}
                            FROM {ev_table} E
                       '''
                self.cursor.execute(qry)
                timestamp = self.cursor.fetchall()
                timestamp = [(t[0], ev_type, t[-1]) for t in timestamp]
                timestamp = pd.DataFrame(timestamp, columns=['ev_id', 'ev_type', 'timestamp'])
                ev_tables[ev_type] = timestamp
        return o2o_table, e20_table, ev_tables

    def related_nodes(self):
        print("Obtaining all related nodes and arcs")

        # Generate a list of all objects of the chosen viewpoint
        qry = f'''
                    SELECT *
                    FROM OBJECT_{self.path_dict['viewpoint']}
                    ORDER BY 2
                    LIMIT {self.cant};
               '''
        self.cursor.execute(qry)
        vwpnt_objects = self.cursor.fetchall()
        o2o_table, e20_table, ev_tables = self.get_data_dictionaries()

        # For each viewpoint object obtain a list of related objects
        rltd_nodes = {}
        vwpnt_cnt = 0
        for vwpnt_object in vwpnt_objects:
            vwpnt_cnt += 1
            if vwpnt_cnt % int(len(vwpnt_objects) / 20) == 0:
                print(int(vwpnt_cnt / int(len(vwpnt_objects))*100), '%')
            ob_list = set()

            # Add the viewpoint object to the list and initiate the relevant dictionaries
            ob_list.add(vwpnt_object[0])
            rltd_nodes[vwpnt_object[0]] = {'related_objects':[], 'related_events':[], 'events_by_objects':[]}

            # Find related objects
            private_types = set()
            rltd_objects = o2o_table[o2o_table['src_id'] == vwpnt_object[0]]
            for i, row in rltd_objects.iterrows():
                ob_list.add(row['src_id'])
                ob_list.add(row['trgt_id'])
                private_types.add(row['src_type'])
                private_types.add(row['trgt_type'])

            for i in range(self.path_dict['depth']):
                src_objects = o2o_table[(o2o_table['src_id'].isin(ob_list))]
                trgt_objects = o2o_table[(o2o_table['trgt_id'].isin(ob_list))]
                rltd_objects = pd.concat([src_objects, trgt_objects])
                for idx, row in rltd_objects.iterrows():
                    if row['src_type'] in self.path_dict['unique_ids']:
                        ob_list.add(row['src_id'])
                    if row['trgt_type'] in self.path_dict['unique_ids']:
                        ob_list.add(row['trgt_id'])

            # Generate a list of related events to the viewpoint object
            rltd_events = set()
            events = e20_table[(e20_table['ob_id'].isin(ob_list))]

            # Add a timestamp to each event
            for i, row in events.iterrows():
                ev_id = row['ev_id']
                ev_type = row['ev_type']
                table = ev_tables[ev_type]
                timestamp = table[table['ev_id'] == ev_id]['timestamp'].values[0]
                event = (ev_id, ev_type, timestamp)
                rltd_events.add(event)

            # Sort the events chronologically and add an index
            rltd_events = sorted(rltd_events, key=lambda x: x[2])
            for idx, event in enumerate(rltd_events):
                ev_id = event[0]
                ev_type = event[1]
                ev_timestamp = event[2]
                rltd_events[idx] = (idx, ev_id, ev_type, ev_timestamp)

            # Obtain a list of all objects related to the events
            rltd_objects = set()
            events_by_objects = {}
            ev_ids = [ev[1] for ev in rltd_events]
            ev_obs = e20_table[e20_table['ev_id'].isin(ev_ids)]

            for i, row in ev_obs.iterrows():
                ob_id = row['ob_id']
                ob_type = row['ob_type']
                ev_id = row['ev_id']
                ev_idx = [e[0] for e in rltd_events if e[1] == ev_id]
                ev_idx = int(ev_idx[0])

                # Checks if the object should be added to the related object list or not
                ob = (ob_id, ob_type)
                if ob_id in ob_list:
                    rltd_objects.add(ob)
                    cont = True
                elif ob_id not in ob_list and ob_type not in private_types:
                    rltd_objects.add(ob)
                    cont = True
                else:
                    cont = False

                # Gets a list of related events to each object
                if cont:
                    if ob_id not in events_by_objects:
                        events_by_objects[ob_id] = {}
                        events_by_objects[ob_id]['Events'] = [ev_idx]
                        events_by_objects[ob_id]['Type'] = [ob_type]
                    else:
                        events_by_objects[ob_id]['Events'].append(ev_idx)

            # Order the object list and add an index
            sorted_objects = sorted(rltd_objects, key=lambda x: (x[1], x[0]))
            pst_type = ''
            ob_index = 0
            rltd_objects = set()
            for rltd_object in sorted_objects:
                ob_type = rltd_object[1]
                if ob_type != pst_type:
                    ob_index = 0
                    pst_type = ob_type
                else:
                    ob_index += 1
                obj = (ob_index, rltd_object[0], rltd_object[1])
                rltd_objects.add(obj)
            rltd_objects = sorted(rltd_objects, key=lambda x: (x[2], x[1]))
            rltd_objects = pd.DataFrame(rltd_objects, columns=['index', 'ocel_id', 'type'])

            # Convert the event_by_object dictionary into a dataframe for ease of use
            evs_by_obs = []
            for key in events_by_objects.keys():
                ob_id = key
                events = events_by_objects[key]['Events']
                events = sorted(events)
                ob_type = events_by_objects[key]['Type'][0]
                ob_idx = rltd_objects.loc[rltd_objects['ocel_id'] == ob_id, ['index']].values[0]
                ev_ob = (ob_id, events, ob_type, int(ob_idx[0]))
                evs_by_obs.append(ev_ob)
            evs_by_obs = pd.DataFrame(evs_by_obs, columns=['ob_id', 'events', 'ob_type', 'index'])

            # Update the dictionary
            rltd_nodes[vwpnt_object[0]]['related_events'].extend(rltd_events)
            rltd_nodes[vwpnt_object[0]]['related_objects'].append(rltd_objects)
            rltd_nodes[vwpnt_object[0]]['events_by_objects'].append(evs_by_obs)
        return rltd_nodes

    def get_ev_log(self, nodes):
        log_id = 0
        log_frames = []
        for vwpnt_object in nodes.keys():
            log_id += 1

            # Each related element is saved as a dictionary with the viewpoint object as key
            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])

            # Obtain the trace KPI
            kpi_event_time = ev_df[ev_df['type'] == self.path_dict['kpi_event']]['timestamp'].values[-1]
            kpi_event_time = pd.to_datetime(kpi_event_time)
            ev_df['kpi_val'] = kpi_event_time - pd.to_datetime(ev_df['timestamp'])
            trace_kpi = (ev_df['kpi_val'][0]).total_seconds()

            # Add the current viewpoint's elements to the event log
            id_col = [log_id for _ in range(len(ev_df.index))]
            ob_id = [vwpnt_object for _ in range(len(ev_df.index))]
            trace_kpi = [trace_kpi for _ in range(len(ev_df.index))]
            ev_log = ev_df[['ocel_id', 'type', 'timestamp']]
            ev_log['kpi_val'] = trace_kpi
            ev_log['vwpnt_id'] = id_col
            ev_log['ob_id'] = ob_id
            log_frames.append(ev_log)

        # Save the event log
        ev_log = pd.concat(log_frames)
        ev_log.to_csv(f"{self.path_dict['ev_log_path']}", index=False)
