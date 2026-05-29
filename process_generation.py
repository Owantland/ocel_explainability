# Given an OCEL 2.0 standard database and a viewpoint, obtain all relevant objects and events for each viewpoint object
# and create a simplified list of process executions.

import sqlite3
import yaml
import pandas as pd
import sup_funcs as sup

'''
    Creating a unified Event table
'''
class process_generation():
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sup.support_functions(database)
        self.get_paths()
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()


    def get_paths(self):
        with open('files/config.yml', 'r') as file:
            db_configs = yaml.safe_load(file)

        self.ocel_path = db_configs[self.database]['ocel_path']
        self.ev_output = db_configs[self.database]['ev_output_path']
        self.filtered_tbls = db_configs[self.database]['filtered_tables']
        self.viewpoint = db_configs[self.database]['viewpoint']
        self.depth = db_configs[self.database]['added_depth']
        self.attributes = db_configs[self.database]['attributes']
        self.time_attributes = db_configs[self.database]['time_attributes']
        self.kpi_event = db_configs[self.database]['kpi_event']
        self.unique_ids = db_configs[self.database]['unique_ids']

    def related_nodes(self):
        print("Obtaining all related nodes and arcs")
        # Generate a list of all objects of the chosen viewpoint
        qry = f'''
                    SELECT *
                    FROM OBJECT_{self.viewpoint}
                    ORDER BY 2
                    LIMIT {self.cant};
               '''
        self.cursor.execute(qry)
        vwpnt_objects = self.cursor.fetchall()

        rltd_nodes = {}
        event_log = []

        # For each viewpoint object obtain a list of related objects
        for vwpnt_object in vwpnt_objects:
            ob_list = set()

            # Add the viewpoint object to the list and initiate the relevant dictionaries
            ob_list.add(vwpnt_object[0])
            rltd_nodes[vwpnt_object[0]] = {'related_objects':[], 'related_events':[], 'events_by_objects':[]}

            # Query related objects
            cols = self.funcs.col_names('object_object')
            qry = f'''
                        SELECT O_O.{cols[0]}, O_O.{cols[1]}, OM.OCEL_TYPE_MAP, TM.OCEL_TYPE_MAP
                        FROM OBJECT_OBJECT O_O
                        JOIN OBJECT O ON O.OCEL_ID = O_O.ocel_source_id
                        JOIN OBJECT T ON T.OCEL_ID = O_O.ocel_target_id
                        JOIN OBJECT_MAP_TYPE OM ON O.OCEL_TYPE = OM.OCEL_TYPE
                        JOIN OBJECT_MAP_TYPE TM ON T.OCEL_TYPE = TM.OCEL_TYPE
                        WHERE {cols[0]} = '{vwpnt_object[0]}' 
                        ORDER BY 1;
                   '''
            self.cursor.execute(qry)
            objects = self.cursor.fetchall()
            private_types = set()
            for rltd_object in objects:
                ob_list.add(rltd_object[1])
                ob_list.add(rltd_object[0])
                private_types.add(rltd_object[2])
                private_types.add(rltd_object[3])

            for i in range(self.depth):
                qry_obs = [f"'{rltd_object}'" for rltd_object in ob_list]
                qry_obs = ','.join(qry_obs)
                qry = f'''
                            SELECT O_O.{cols[0]}, O_O.{cols[1]}, OM.OCEL_TYPE_MAP, TM.OCEL_TYPE_MAP
                            FROM OBJECT_OBJECT O_O
                            JOIN OBJECT O ON O.OCEL_ID = O_O.ocel_source_id
                            JOIN OBJECT T ON T.OCEL_ID = O_O.ocel_target_id
                            JOIN OBJECT_MAP_TYPE OM ON O.OCEL_TYPE = OM.OCEL_TYPE
                            JOIN OBJECT_MAP_TYPE TM ON T.OCEL_TYPE = TM.OCEL_TYPE
                            WHERE {cols[0]} IN ({qry_obs})
                                  OR {cols[1]} IN ({qry_obs})
                            ORDER BY 1;
                       '''
                self.cursor.execute(qry)
                objects = self.cursor.fetchall()
                for rltd_object in objects:
                    if rltd_object[2] in self.unique_ids:
                        ob_list.add(rltd_object[0])
                    if rltd_object[3] in self.unique_ids:
                        ob_list.add(rltd_object[1])

            # Generate a list of related events to the viewpoint object
            rltd_events = set()
            qry_obs = [f"'{ob}'" for ob in ob_list]
            qry_obs = ','.join(qry_obs)
            cols = self.funcs.col_names('event_object')
            ev_cols = self.funcs.col_names('event')
            mp_cols = self.funcs.col_names('event_map_type')

            # Obtain the event_id and its type as well as it's index in the timeline
            qry = f'''
                        SELECT DISTINCT EO.{cols[0]}, M.{mp_cols[1]}
                        FROM EVENT_OBJECT EO
                        JOIN EVENT E ON EO.{cols[0]} = E.{ev_cols[0]}
                        JOIN EVENT_MAP_TYPE M ON E.{ev_cols[1]} = M.{mp_cols[0]}
                        WHERE EO.{cols[1]} IN ({qry_obs})
                        ORDER BY 1;
                   '''
            self.cursor.execute(qry)
            events = self.cursor.fetchall()

            # Add a timestamp to each event
            for event in events:
                ev_id = event[0]
                ev_type = event[1]
                ev_table = f'event_{ev_type}'
                cols = self.funcs.col_names(ev_table)

                qry = f'''
                            SELECT {cols[-1]}
                            FROM {ev_table} E
                            WHERE E.{cols[0]} = '{ev_id}'
                       '''
                self.cursor.execute(qry)
                timestamp = self.cursor.fetchall()
                timestamp = timestamp[0][0]
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
            ev_dict = {}
            for event in rltd_events:
                ev_idx = event[0]
                ev_id = event[1]
                ev_type = event[2]
                ev_timestamp = event[3]
                try:
                    ev_dict[ev_type].append([ev_idx, ev_id, ev_timestamp])
                except KeyError:
                    ev_dict[ev_type] = [[ev_idx, ev_id, ev_timestamp]]

            # For each event type get the related objects
            for ev_type in ev_dict:
                qry_evs = [f"'{ev[1]}'" for ev in ev_dict[ev_type]]
                qry_evs = ','.join(qry_evs)
                cols = self.funcs.col_names('event_object')
                ob_cols = self.funcs.col_names('object')
                mp_cols = self.funcs.col_names('object_map_type')
                qry = f'''
                            SELECT DISTINCT EO.{cols[1]}, M.{mp_cols[1]}, EO.{cols[0]}
                            FROM EVENT_OBJECT EO
                            JOIN OBJECT O ON EO.{cols[1]} = O.{ob_cols[0]}
                            JOIN OBJECT_MAP_TYPE M ON O.{ob_cols[1]} = M.{mp_cols[0]}
                            WHERE {cols[0]} IN ({qry_evs})
                            ORDER BY 1;
                       '''
                self.cursor.execute(qry)
                objects = self.cursor.fetchall()
                for object in objects:
                    if object[0] in ob_list:
                        rltd_objects.add(object[:-1])
                        cont = True
                    elif object[0] not in ob_list and object[1] not in private_types:
                        rltd_objects.add(object[:-1])
                        cont = True
                    else:
                        cont = False

                    # Get a list of related events to each object
                    if cont:
                        ev_idx = [x[0] for x in ev_dict[ev_type] if x[1] == object[2]]
                        ev_idx = int(ev_idx[0])
                        obj = object[0]
                        obj_type = object[1]
                        if obj not in events_by_objects:
                            events_by_objects[obj] = {}
                            events_by_objects[obj]['Events'] = [ev_idx]
                            events_by_objects[obj]['Type'] = [obj_type]
                        else:
                            events_by_objects[obj]['Events'].append(ev_idx)

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
                object = (ob_index, rltd_object[0], rltd_object[1])
                rltd_objects.add(object)
            rltd_objects = sorted(rltd_objects, key=lambda x: (x[2], x[1]))

            # Update the dictionary
            rltd_nodes[vwpnt_object[0]]['related_events'].extend(rltd_events)
            rltd_nodes[vwpnt_object[0]]['related_objects'].extend(rltd_objects)
            rltd_nodes[vwpnt_object[0]]['events_by_objects'].append(events_by_objects)
        return rltd_nodes

    def get_ev_log(self, nodes):
        log_id = 0
        log_frames = []
        for vwpnt_object in nodes.keys():
            log_id += 1

            # Each related element is saved as a dictionary with the viewpoint object as key
            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])

            kpi_event_time = ev_df[ev_df['type'] == self.kpi_event]['timestamp'].values[-1]
            kpi_event_time = pd.to_datetime(kpi_event_time)
            ev_df['kpi_val'] = kpi_event_time - pd.to_datetime(ev_df['timestamp'])
            ev_df['kpi_val'] = ev_df['kpi_val'].apply(lambda x: x.total_seconds())
            ev_df = ev_df[ev_df['kpi_val'] >= 0]

            # Add the current viewpoint's elements to the event log
            id_col = [log_id for _ in range(len(ev_df.index))]
            ob_id = [vwpnt_object for _ in range(len(ev_df.index))]
            ev_log = ev_df[['ocel_id', 'type', 'timestamp', 'kpi_val']]
            ev_log['vwpnt_id'] = id_col
            ev_log['ob_id'] = ob_id
            log_frames.append(ev_log)

        # Save the event log
        ev_log = pd.concat(log_frames)
        ev_log.to_csv(self.ev_output, index=False)
