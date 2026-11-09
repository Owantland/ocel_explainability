# Given an OCEL 2.0 standard database and a viewpoint, obtain all relevant objects and events for each viewpoint object
# and create a simplified list of process executions.
import pandas as pd
import sup_funcs as sup
from sup_funcs import _validate_identifier

'''
    Creating a unified Event table
'''
class ProcessGeneration:
    def __init__(self, database, cant):
        self.database = database
        self.cant = cant
        self.funcs = sup.SupportFunctions(database, cant)
        self.path_dict = self.funcs.get_paths()
        # Reuse SupportFunctions' own connection/cursor to this same database file
        # rather than opening a second, redundant connection.
        self.cursor = self.funcs.cursor

    def get_data_dictionaries(self):
        # Generate a list of object to object relationships
        cols = self.funcs.col_names('object_object')
        # Not every database's object_object table has qualifier/timestamp columns beyond the
        # two ids (order_management/logistics don't; CargoPickup_IoT does) -- project them only
        # when present, schema-aware like every other table lookup in this method. 'qualifier'
        # and 'o2o_time' are consumed by the optional episode-scoped-traversal mechanism below;
        # absent for every database that doesn't configure episode_scoped_ids.
        extra_select, extra_cols = '', []
        if len(cols) > 2:
            extra_select += f', O_O.{cols[2]}'
            extra_cols.append('qualifier')
        if len(cols) > 3:
            extra_select += f', O_O.{cols[3]}'
            extra_cols.append('o2o_time')
        qry = f'''
                    SELECT O_O.{cols[0]}, O_O.{cols[1]}, OM.OCEL_TYPE_MAP, TM.OCEL_TYPE_MAP{extra_select}
                    FROM OBJECT_OBJECT O_O
                    JOIN OBJECT O ON O.OCEL_ID = O_O.ocel_source_id
                    JOIN OBJECT T ON T.OCEL_ID = O_O.ocel_target_id
                    JOIN OBJECT_MAP_TYPE OM ON O.OCEL_TYPE = OM.OCEL_TYPE
                    JOIN OBJECT_MAP_TYPE TM ON T.OCEL_TYPE = TM.OCEL_TYPE
                    ORDER BY 1;
               '''
        self.cursor.execute(qry)
        objects = self.cursor.fetchall()
        o2o_table = pd.DataFrame.from_records(
            objects, columns=['src_id', 'trgt_id', 'src_type', 'trgt_type'] + extra_cols)

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
                ev_table = f'event_{_validate_identifier(ev_type)}'
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
        # Flat ev_id → timestamp dict for O(1) lookup (replaces per-event DataFrame scan)
        ev_ts_dict = {}
        for tbl in ev_tables.values():
            ev_ts_dict.update(tbl.set_index('ev_id')['timestamp'].to_dict())

        return o2o_table, e20_table, ev_tables, ev_ts_dict

    def _resolve_episode_window(self, o2o_table, shared_id, anchor_id, start_q, end_q,
                                 start_ts, global_max_ts):
        """Resolve the [start, end] validity window for one shared-resource object's (shared_id)
        relationship to one specific anchor object (e.g. a Truck's assignment to one Pickup
        Plan), using object_object's own qualifier/timestamp columns -- this is how a
        many-to-many shared resource (unlike a strictly-owned hierarchy member) gets its
        traversal/event-inclusion scoped to one case instead of its entire cross-case history.

        If no matching end-qualifier row exists for this specific (shared_id, anchor_id) pair,
        the window is clamped to just before shared_id's own next start-qualifier timestamp (its
        next assignment to any OTHER case), or to the dataset's global max timestamp if there
        isn't one. This is a modeling policy choice, not a neutral default: it treats an
        unresolved ("still open") episode as lasting until the shared resource is reassigned
        elsewhere. Confirmed empirically for CargoPickup_IoT: ~68% of Truck/Pickup-Plan
        assignments have no 'Dropped' row at all, so this is the common case, not an edge case.

        The clamped end is nudged 1 second earlier than the next assignment's own timestamp,
        not equal to it: the window check elsewhere (_edge_time_ok) is inclusive on both ends,
        which is correct for a real 'Dropped' timestamp (that event genuinely belongs to this
        episode) but wrong for a clamped one -- an inclusive match would let the reassignment
        edge ITSELF (to a different case) pass as if still part of this episode, letting that
        other case leak in. Confirmed by direct reproduction against this dataset before this
        nudge was added."""
        start_ts = pd.to_datetime(start_ts)
        end_rows = o2o_table[(o2o_table['src_id'] == shared_id)
                              & (o2o_table['trgt_id'] == anchor_id)
                              & (o2o_table['qualifier'] == end_q)]
        if len(end_rows) > 1:
            print(f"  [_resolve_episode_window] {shared_id}->{anchor_id}: {len(end_rows)} "
                  f"'{end_q}' rows found for one pair (expected at most 1); using the earliest.")
        if not end_rows.empty:
            return start_ts, pd.to_datetime(end_rows['o2o_time']).min()

        later_starts = o2o_table[(o2o_table['src_id'] == shared_id)
                                  & (o2o_table['qualifier'] == start_q)
                                  & (pd.to_datetime(o2o_table['o2o_time']) > start_ts)]
        if not later_starts.empty:
            end_ts = pd.to_datetime(later_starts['o2o_time']).min() - pd.Timedelta(seconds=1)
        else:
            end_ts = global_max_ts
        return start_ts, end_ts

    @staticmethod
    def _edge_time_ok(row, episode_windows):
        """Gate for traversal hops that originate FROM an already episode-windowed object (e.g.
        reaching Silo via an already-windowed Truck): the edge's own timestamp must fall inside
        every endpoint's resolved window. A no-op (always True) when episode_windows is empty,
        so this never touches the optional 'o2o_time'/'qualifier' columns for databases that
        don't have them."""
        if not episode_windows:
            return True
        ts = pd.to_datetime(row['o2o_time'])
        for anchor_id in (row['src_id'], row['trgt_id']):
            if anchor_id in episode_windows:
                start, end = episode_windows[anchor_id]
                if not (start <= ts <= end):
                    return False
        return True

    def related_nodes(self):
        print("Obtaining all related nodes and arcs")

        # Generate a list of all objects of the chosen viewpoint
        viewpoint = _validate_identifier(self.path_dict['viewpoint'])
        qry = f'''
                    SELECT *
                    FROM object_{viewpoint}
                    ORDER BY 2
                    LIMIT ?;
               '''
        self.cursor.execute(qry, (self.cant,))
        vwpnt_objects = self.cursor.fetchall()
        o2o_table, e20_table, ev_tables, ev_ts_dict = self.get_data_dictionaries()

        # Optional: shared-resource unique_ids types (e.g. CargoPickup_IoT's Truck, which serves
        # many Pickup Plans) whose event/traversal inclusion must be scoped to a bounded
        # object_object episode rather than that object's entire cross-case history. Empty for
        # every database that doesn't configure it -- every gate below is then a guaranteed no-op
        # and this whole mechanism never touches the (possibly absent) qualifier/o2o_time columns.
        episode_cfg = self.path_dict.get('episode_scoped_ids', {})
        global_max_ts = None
        if episode_cfg:
            if 'o2o_time' not in o2o_table.columns:
                raise ValueError(
                    f"config.yml['{self.database}'].episode_scoped_ids is set but object_object "
                    f"has no timestamp column for this database -- episode windows cannot be "
                    f"resolved."
                )
            o2o_times = pd.to_datetime(o2o_table['o2o_time'])
            ev_times = pd.to_datetime(pd.Series(list(ev_ts_dict.values())))
            global_max_ts = max(o2o_times.max(), ev_times.max())

        # For each viewpoint object obtain a list of related objects
        rltd_nodes = {}
        vwpnt_cnt = 0
        progress_step = max(1, len(vwpnt_objects) // 20)
        for vwpnt_object in vwpnt_objects:
            vwpnt_cnt += 1
            if vwpnt_cnt % progress_step == 0:
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

            # Resolved [start, end] episode windows for this trace's shared-resource objects,
            # keyed by object id. Stays empty (and every gate below stays a no-op) whenever
            # episode_cfg is empty.
            episode_windows = {}

            # Symmetric pass: the block above only catches object_object edges where the
            # viewpoint object is the SOURCE. Some databases' schemas model the viewpoint's
            # relevant neighbors as the TARGET instead (e.g. CargoPickup_IoT's Truck->Pickupplan,
            # the reverse of order_management/logistics' parent->child convention) -- without
            # this, such a neighbor type never enters private_types, so stray same-typed objects
            # swept in later via unrelated events wrongly pass the "generic, non-private touched
            # object" check below instead of being gated by actual ob_list membership. Gated by
            # unique_ids membership, exactly like the depth loop already is, so this is a no-op
            # wherever the matched type isn't traversed at all -- confirmed inert for
            # order_management (its one such edge, customers->orders, involves 'customers',
            # which isn't in unique_ids there) and logistics (no such edge exists at all).
            rltd_trgt = o2o_table[o2o_table['trgt_id'] == vwpnt_object[0]]
            for i, row in rltd_trgt.iterrows():
                if row['src_type'] in self.path_dict['unique_ids']:
                    private_types.add(row['src_type'])
                    private_types.add(row['trgt_type'])
                    if (row['src_type'] in episode_cfg and row['src_id'] not in episode_windows
                            and row['qualifier'] == episode_cfg[row['src_type']]['start_qualifier']):
                        episode_windows[row['src_id']] = self._resolve_episode_window(
                            o2o_table, row['src_id'], row['trgt_id'],
                            episode_cfg[row['src_type']]['start_qualifier'],
                            episode_cfg[row['src_type']]['end_qualifier'],
                            row['o2o_time'], global_max_ts)
                    ob_list.add(row['src_id'])
                    ob_list.add(row['trgt_id'])

            for i in range(self.path_dict['depth']):
                src_objects = o2o_table[(o2o_table['src_id'].isin(ob_list))]
                trgt_objects = o2o_table[(o2o_table['trgt_id'].isin(ob_list))]
                rltd_objects = pd.concat([src_objects, trgt_objects])
                for idx, row in rltd_objects.iterrows():
                    # Further hops originating from an already-windowed object (e.g. reaching
                    # Silo via an already-windowed Truck) must respect that object's window --
                    # a no-op when episode_windows is empty.
                    if not self._edge_time_ok(row, episode_windows):
                        continue
                    # Deliberately do NOT resolve new episode windows here. Once an
                    # episode-scoped object (e.g. a shared Truck) is already in ob_list, this
                    # query re-matches ALL of its object_object rows -- including its OTHER
                    # cases' assignments -- in one unordered batch, so resolving a window from
                    # whichever row happens to be seen first here would be ambiguous and can
                    # pick the wrong anchor. Windows are only ever resolved in the one-shot
                    # trgt-pass above, where the anchor is unambiguously this trace's own
                    # viewpoint object. An episode-scoped type not yet windowed by that point
                    # is skipped here rather than guessed at.
                    # General invariant, independent of episode_scoped_ids: a trace must never
                    # admit a DIFFERENT instance of the viewpoint's own type. Any object type
                    # shared across multiple cases (e.g. CargoPickup_IoT's Cargo, confirmed to
                    # serve exactly 2 Pickup Plans each -- NOT 1:1 as the aggregate row count
                    # alone suggested) can otherwise act as a pivot: once it's in ob_list, this
                    # query re-matches its edge to every OTHER case it touches, and without this
                    # guard the other case's own viewpoint object slips back in alongside it.
                    # Only episode-scoping the "obviously" shared type (Truck) was not sufficient
                    # in practice -- this blanket rule covers every such type without having to
                    # enumerate them all in config.
                    viewpoint_type = self.path_dict['viewpoint']
                    if row['src_type'] in self.path_dict['unique_ids']:
                        if not (row['src_type'] in episode_cfg and row['src_id'] not in episode_windows):
                            if not (row['src_type'] == viewpoint_type and row['src_id'] != vwpnt_object[0]):
                                ob_list.add(row['src_id'])
                    if row['trgt_type'] in self.path_dict['unique_ids']:
                        if not (row['trgt_type'] in episode_cfg and row['trgt_id'] not in episode_windows):
                            if not (row['trgt_type'] == viewpoint_type and row['trgt_id'] != vwpnt_object[0]):
                                ob_list.add(row['trgt_id'])

            # Generate a list of related events to the viewpoint object
            rltd_events = set()
            if episode_windows:
                # Shared-resource objects: only pull in events within their resolved window, not
                # their entire cross-case history. Strictly-owned objects (not in
                # episode_windows) are unaffected -- this reduces to the original single-line
                # query below whenever episode_windows is empty.
                plain_ids = ob_list - set(episode_windows.keys())
                events = e20_table[e20_table['ob_id'].isin(plain_ids)]
                windowed = e20_table[e20_table['ob_id'].isin(episode_windows.keys())].copy()
                if not windowed.empty:
                    windowed['_ts'] = pd.to_datetime(windowed['ev_id'].map(ev_ts_dict))
                    keep = windowed.apply(
                        lambda r: episode_windows[r['ob_id']][0] <= r['_ts'] <= episode_windows[r['ob_id']][1],
                        axis=1)
                    events = pd.concat([events, windowed[keep].drop(columns='_ts')], ignore_index=True)
            else:
                events = e20_table[(e20_table['ob_id'].isin(ob_list))]

            # Add a timestamp to each event (O(1) dict lookup instead of DataFrame scan per event)
            for i, row in events.iterrows():
                ev_id = row['ev_id']
                ev_type = row['ev_type']
                timestamp = ev_ts_dict[ev_id]
                event = (ev_id, ev_type, timestamp)
                rltd_events.add(event)

            # Sort the events chronologically and add an index. ev_id is a secondary sort key
            # so events sharing an identical timestamp get a deterministic, reproducible order
            # instead of one dependent on Python's hash-randomized set iteration order.
            rltd_events = sorted(rltd_events, key=lambda x: (x[2], x[0]))
            for idx, event in enumerate(rltd_events):
                ev_id = event[0]
                ev_type = event[1]
                ev_timestamp = event[2]
                rltd_events[idx] = (idx, ev_id, ev_type, ev_timestamp)

            # Obtain a list of all objects related to the events
            rltd_objects = set()
            events_by_objects = {}
            ev_ids = [ev[1] for ev in rltd_events]
            ev_id_to_idx = {ev[1]: ev[0] for ev in rltd_events}  # O(1) lookup replaces list scan
            ev_obs = e20_table[e20_table['ev_id'].isin(ev_ids)]

            for i, row in ev_obs.iterrows():
                ob_id = row['ob_id']
                ob_type = row['ob_type']
                ev_id = row['ev_id']
                ev_idx = ev_id_to_idx[ev_id]

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
        log_frames = pd.DataFrame()
        all_kpis = pd.DataFrame()
        for vwpnt_object in nodes.keys():
            log_id += 1

            # Each related element is saved as a dictionary with the viewpoint object as key
            rltd_events = nodes[vwpnt_object]['related_events']
            ev_df = pd.DataFrame(rltd_events, columns=['index', 'ocel_id', 'type', 'timestamp'])
            ev_by_ob = nodes[vwpnt_object]['events_by_objects'][0]

            """
                Calculate the chosen KPI
                
                Depending on the option given in the config file calculate the appropriate KPI value and add it
                to the kpi dictionary file
            """
            kpi_event = self.path_dict['kpi_event']
            kpi_ob = self.path_dict['kpi_viewpoint']
            kpi_events = ev_df[ev_df['type'] == kpi_event]['index']

            if self.path_dict['kpi_type'] == 0:
                for i, row in ev_by_ob.iterrows():
                    ob_id = row['ob_id']
                    evs_by_ob = row['events']
                    ob_type = row['ob_type']
                    ob_idx = row['index']

                    if ob_type == kpi_ob:
                        first_event = evs_by_ob[0]
                        # Identify the end event for the trace
                        try:
                            end_event = [ev for ev in evs_by_ob if ev in kpi_events][-1]
                        except IndexError:
                            # The object isn't tied to any instance of the KPI event -- fall
                            # back to the trace's last instance instead. Logged since a
                            # misconfigured kpi_event would otherwise silently affect every trace.
                            trace_kpi_idx = ev_df[ev_df['type'] == kpi_event]['index'].values
                            if len(trace_kpi_idx) == 0:
                                # Not even the trace-wide fallback has one: this object's own
                                # process never reached kpi_event at all (e.g. a Truck that failed
                                # a weigh/load check before the event that defines "success").
                                # Skip this object rather than crash the whole run -- a real
                                # per-object condition, not a misconfiguration, so it's logged but
                                # not treated as fatal.
                                print(f"  [get_ev_log] object {ob_id} (viewpoint {vwpnt_object}) "
                                      f"has no '{kpi_event}' event anywhere in its trace; "
                                      f"skipping this object's KPI row entirely.")
                                continue
                            print(f"  [get_ev_log] object {ob_id} (viewpoint {vwpnt_object}) has "
                                  f"no '{kpi_event}' event of its own; falling back to the "
                                  f"trace's last instance.")
                            end_event = trace_kpi_idx[-1]
                        start_time = ev_df[ev_df['index'] == first_event]['timestamp'].values[0]
                        end_time = ev_df[ev_df['index'] == end_event]['timestamp'].values[0]

                        # Begin calculating the timeToEvent for each step of the process
                        event_log = ev_df[ev_df['timestamp'] >= start_time]
                        event_log = event_log[event_log['timestamp'] <= end_time].copy()

                        # Add lines to event log file
                        ev_log = event_log[['ocel_id', 'type', 'timestamp']].copy()
                        ev_log['vwpnt_id'] = log_id
                        ev_log['ob_id'] = vwpnt_object
                        log_frames = pd.concat([log_frames, ev_log])

                        # Calculate the KPI value
                        event_log['kpi_val'] = pd.to_datetime(end_time) - pd.to_datetime(event_log['timestamp'])
                        event_log['kpi_val'] = event_log['kpi_val'].apply(lambda x: x.total_seconds())

                        kpi = event_log[['timestamp', 'kpi_val']].copy()
                        kpi['viewpoint_id'] = log_id
                        kpi['kpi_event'] = kpi_event
                        kpi['ob_id'] = ob_id
                        kpi['ob_idx'] = ob_idx
                        all_kpis = pd.concat([all_kpis, kpi])


            elif self.path_dict['kpi_type'] == 1:
                # Add the current viewpoint's elements to the event log
                ev_log = ev_df[['ocel_id', 'type', 'timestamp']].copy()
                ev_log['vwpnt_id'] = log_id
                ev_log['ob_id'] = vwpnt_object
                log_frames = pd.concat([log_frames, ev_log])

                # Checks if there's more than one instance of the KPI event
                evs = ev_df[ev_df['type'] == kpi_event]
                trace_evs = len(evs.values)
                kpi_val = 1 if trace_evs > 1 else 0

                kpi = ev_df[['timestamp']].copy()
                kpi['kpi_val'] = kpi_val
                kpi['viewpoint_id'] = log_id
                kpi['kpi_event'] = kpi_event
                kpi['ob_id'] = vwpnt_object
                kpi['ob_idx'] = 0
                all_kpis = pd.concat([all_kpis, kpi])

        # Save the kpis and the event log
        all_kpis.to_csv(f"{self.path_dict['graph_output_path']}all_kpis.csv", index=False)
        log_frames.to_csv(f"{self.path_dict['ev_log_path']}", index=False)
