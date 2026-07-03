import process_generation as pg
import ocel_generator as og
import train_test_builder as tb
import hetero_graphs as hg
import explainer as exp
import training as t

import ast
import json
import numpy as np
import sqlite3
import pandas as pd
import sup_funcs as sup


def verify_process_generation(database='order_management', cant=2000):
    """Cross-check process_generation outputs against the raw OCEL database."""
    funcs = sup.SupportFunctions(database, cant)
    path_dict = funcs.get_paths()
    conn = sqlite3.connect(path_dict['ocel_path'])
    cursor = conn.cursor()

    all_kpis_df = pd.read_csv(f"{path_dict['graph_output_path']}all_kpis.csv")
    ev_log_df   = pd.read_csv(path_dict['ev_log_path'])
    all_kpis_df['timestamp'] = pd.to_datetime(all_kpis_df['timestamp'])

    kpi_event   = path_dict['kpi_event']
    viewpoint   = path_dict['viewpoint']
    errors      = []

    print(f"\n{'='*60}")
    print(f"Verifying process_generation: {database}, cant={cant}")
    print(f"{'='*60}")

    # ------------------------------------------------------------------
    # Check 1 — Schema assumptions (R1, R2)
    # ------------------------------------------------------------------
    print("\n[Check 1] Schema assumptions...")

    # R1: ORDER BY 2 in the viewpoint query must be the timestamp column
    cursor.execute(f"PRAGMA table_info(object_{viewpoint})")
    vp_cols = [r[1] for r in cursor.fetchall()]
    if 'time' not in vp_cols[1].lower():
        errors.append(f"R1: column 2 of object_{viewpoint} is '{vp_cols[1]}', not a timestamp")
    else:
        print(f"  R1 OK: object_{viewpoint} column 2 = '{vp_cols[1]}'")

    # R2: timestamp must be the LAST column of every event_* table
    cursor.execute("SELECT DISTINCT ocel_type_map FROM event_map_type")
    ev_types = [r[0] for r in cursor.fetchall()]
    for et in ev_types:
        cursor.execute(f"PRAGMA table_info(event_{et})")
        et_cols = [r[1] for r in cursor.fetchall()]
        if 'time' not in et_cols[-1].lower():
            errors.append(f"R2: last column of event_{et} is '{et_cols[-1]}', not a timestamp")
        else:
            print(f"  R2 OK: event_{et} last col = '{et_cols[-1]}'")

    # ------------------------------------------------------------------
    # Check 2 — KPI value spot-check (5 orders)  (R3)
    # ------------------------------------------------------------------
    print(f"\n[Check 2] KPI value spot-check (5 {viewpoint})...")
    sample_orders = (all_kpis_df.groupby('ob_id')['kpi_val'].max()
                                .reset_index().head(5))
    for _, row in sample_orders.iterrows():
        order_id   = row['ob_id']
        actual_max = row['kpi_val']

        cursor.execute(f"""
            SELECT MAX(EP.ocel_time)
            FROM event_{kpi_event} EP
            JOIN event_object EO ON EP.ocel_id = EO.ocel_event_id
            WHERE EO.ocel_object_id = ?
        """, (order_id,))
        result = cursor.fetchone()[0]
        if result is None:
            print(f"  WARN: no {kpi_event} linked to {order_id} in DB (fallback KPI used)")
            continue

        first_ts     = all_kpis_df[all_kpis_df['ob_id'] == order_id]['timestamp'].min()
        expected_max = (pd.Timestamp(result) - first_ts).total_seconds()
        if abs(expected_max - actual_max) > 1:
            errors.append(
                f"R3: KPI mismatch for {order_id}: "
                f"expected={expected_max:.0f}s, actual={actual_max:.0f}s"
            )
        else:
            print(f"  OK: {order_id}: expected={expected_max:.0f}s, actual={actual_max:.0f}s")

    # ------------------------------------------------------------------
    # Check 3 — Completeness (R4)
    # ------------------------------------------------------------------
    print("\n[Check 3] Completeness...")
    n_vp = all_kpis_df['ob_id'].nunique()
    if n_vp != cant:
        errors.append(f"R4: expected {cant} unique viewpoint objects, got {n_vp}")
    else:
        print(f"  OK: {n_vp} unique viewpoint objects")

    null_count = all_kpis_df['kpi_val'].isna().sum()
    if null_count:
        errors.append(f"Null KPI values: {null_count} rows")
    else:
        print(f"  OK: no null KPI values")

    neg_count = (all_kpis_df['kpi_val'] < 0).sum()
    if neg_count:
        errors.append(f"Negative KPI values: {neg_count} rows")
    else:
        print(f"  OK: no negative KPI values")

    # ------------------------------------------------------------------
    # Check 4 — Monotonic decrease within traces (R6)
    # ------------------------------------------------------------------
    print("\n[Check 4] Monotonic KPI decrease within each trace...")
    violations = []
    for order_id, grp in all_kpis_df.sort_values('timestamp').groupby('ob_id'):
        vals = grp['kpi_val'].values
        if not (vals[:-1] >= vals[1:]).all():
            violations.append(order_id)
    if violations:
        errors.append(
            f"R6: non-monotonic KPI in {len(violations)} trace(s): {violations[:5]}"
        )
    else:
        print(f"  OK: all traces have monotonically decreasing KPI values")

    # ------------------------------------------------------------------
    # Check 5 — Fallback KPI orders (R3)
    # ------------------------------------------------------------------
    print(f"\n[Check 5] Fallback KPI orders (no direct {kpi_event} link)...")
    cursor.execute(f"""
        SELECT DISTINCT EO.ocel_object_id
        FROM event_object EO
        JOIN object O ON O.ocel_id = EO.ocel_object_id
        JOIN object_map_type OM ON O.ocel_type = OM.ocel_type
        WHERE EO.ocel_event_id IN (SELECT ocel_id FROM event_{kpi_event})
          AND OM.ocel_type_map = ?
    """, (viewpoint,))
    orders_with_direct_kpi = {r[0] for r in cursor.fetchall()}
    all_order_ids   = set(all_kpis_df['ob_id'].unique())
    fallback_orders = all_order_ids - orders_with_direct_kpi
    if fallback_orders:
        print(f"  WARN: {len(fallback_orders)} order(s) used fallback KPI "
              f"(no direct {kpi_event} link):")
        for o in list(fallback_orders)[:5]:
            print(f"    {o}")
    else:
        print(f"  OK: all orders have a direct {kpi_event} event link")

    # ------------------------------------------------------------------
    # Check 6 — ev_log and all_kpis alignment
    # ------------------------------------------------------------------
    print("\n[Check 6] ev_log / all_kpis alignment...")
    if len(ev_log_df) != len(all_kpis_df):
        errors.append(
            f"Row count mismatch: ev_log={len(ev_log_df)}, all_kpis={len(all_kpis_df)}"
        )
    else:
        print(f"  OK: both files have {len(ev_log_df)} rows")

    kpi_ids = set(all_kpis_df['ob_id'].unique())
    ev_ids  = set(ev_log_df['ob_id'].unique())
    if kpi_ids != ev_ids:
        errors.append(
            f"Viewpoint ID mismatch — only in all_kpis: {kpi_ids - ev_ids}, "
            f"only in ev_log: {ev_ids - kpi_ids}"
        )
    else:
        print(f"  OK: viewpoint IDs match across both files")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    if errors:
        print(f"FAILED — {len(errors)} error(s):")
        for e in errors:
            print(f"  ✗ {e}")
    else:
        print("PASSED — all checks OK")
    print(f"{'='*60}\n")

    conn.close()
    return errors


def verify_ocel_generator(database='order_management', cant=2000):
    """Spot-check ocel.csv attributes against the raw DB and audit Adams et al. coverage."""

    def _has_objects(val):
        if pd.isna(val):
            return False
        try:
            return len(ast.literal_eval(val)) > 0
        except Exception:
            return False

    funcs = sup.SupportFunctions(database, cant)
    path_dict = funcs.get_paths()
    conn = sqlite3.connect(path_dict['ocel_path'])
    cursor = conn.cursor()

    ocel_df   = pd.read_csv(f"{path_dict['graph_output_path']}ocel.csv")
    ev_log_df = pd.read_csv(path_dict['ev_log_path'])
    errors    = []

    print(f"\n{'='*60}")
    print(f"Verifying ocel_generator output: {database}, cant={cant}")
    print(f"{'='*60}")

    # ------------------------------------------------------------------
    # Check 1 — Event type encoding (C1)
    # ------------------------------------------------------------------
    print("\n[Check 1] Event type encoding (C1)...")
    cursor.execute("SELECT DISTINCT ocel_type_map FROM event_map_type ORDER BY 1")
    ev_types       = [r[0] for r in cursor.fetchall()]
    ev_type_to_idx = {t: i for i, t in enumerate(ev_types)}

    for _, row in ocel_df.sample(10, random_state=42).iterrows():
        enc = ast.literal_eval(row['ev_type'])
        if len(enc) != len(ev_types):
            errors.append(f"C1: dim {len(enc)} ≠ {len(ev_types)} for {row['ev_id']}")
            continue
        if sum(enc) != 1:
            errors.append(f"C1: not one-hot for {row['ev_id']}: {enc}")
            continue
        ev_log_match = ev_log_df[ev_log_df['ocel_id'] == row['ev_id']]['type']
        if ev_log_match.empty:
            continue
        expected_idx = ev_type_to_idx.get(ev_log_match.values[0])
        if expected_idx is None or enc[expected_idx] != 1:
            errors.append(
                f"C1: wrong position for {row['ev_id']}: "
                f"type={ev_log_match.values[0]}, enc={enc}"
            )
    if not any(e.startswith('C1') for e in errors):
        print(f"  OK: 10 sampled encodings correct ({len(ev_types)}D)")

    # ------------------------------------------------------------------
    # Check 2 — Orders attribute: price (D3)
    # ------------------------------------------------------------------
    print(f"\n[Check 2] {path_dict['viewpoint']} attribute (price)...")
    viewpoint = path_dict['viewpoint']
    for _, row in ocel_df[ocel_df[f'{viewpoint}::ids'].apply(_has_objects)].sample(5, random_state=42).iterrows():
        ids   = ast.literal_eval(row[f'{viewpoint}::ids'])
        attrs = ast.literal_eval(row[f'{viewpoint}::attributes'])
        for ob_id, attr in zip(ids, attrs):
            cursor.execute(f"SELECT MAX(price) FROM object_{viewpoint} WHERE ocel_id=?", (ob_id,))
            db_price = cursor.fetchone()[0]
            if db_price is None:
                continue
            if abs(attr[0] - db_price) > 0.01:
                errors.append(f"Orders price mismatch {ob_id}: expected {db_price}, got {attr[0]}")
            else:
                print(f"  OK: {ob_id} price={attr[0]} ✓")

    # ------------------------------------------------------------------
    # Check 3 — Items attributes: weight, price (D3)
    # ------------------------------------------------------------------
    print("\n[Check 3] Items attributes (weight, price)...")
    for _, row in ocel_df[ocel_df['Items::ids'].apply(_has_objects)].sample(5, random_state=42).iterrows():
        ids   = ast.literal_eval(row['Items::ids'])
        attrs = ast.literal_eval(row['Items::attributes'])
        for ob_id, attr in zip(ids, attrs):
            cursor.execute("SELECT MAX(weight), MAX(price) FROM object_Items WHERE ocel_id=?", (ob_id,))
            db_w, db_p = cursor.fetchone()
            if db_w is None:
                continue
            if abs(attr[0] - db_w) > 0.001 or abs(attr[1] - db_p) > 0.01:
                errors.append(f"Items mismatch {ob_id}: expected ({db_w},{db_p}), got {attr}")
            else:
                print(f"  OK: {ob_id} weight={attr[0]}, price={attr[1]} ✓")

    # ------------------------------------------------------------------
    # Check 4 — Packages attribute: weight (D3)
    # ------------------------------------------------------------------
    print("\n[Check 4] Packages attribute (weight)...")
    pkg_col = 'Packages::ids'
    if pkg_col in ocel_df.columns and ocel_df[ocel_df[pkg_col].apply(_has_objects)].shape[0] > 0:
        for _, row in ocel_df[ocel_df[pkg_col].apply(_has_objects)].sample(5, random_state=42).iterrows():
            ids   = ast.literal_eval(row['Packages::ids'])
            attrs = ast.literal_eval(row['Packages::attributes'])
            for ob_id, attr in zip(ids, attrs):
                cursor.execute("SELECT MAX(weight) FROM object_Packages WHERE ocel_id=?", (ob_id,))
                db_w = cursor.fetchone()[0]
                if db_w is None:
                    continue
                if abs(attr[0] - db_w) > 0.001:
                    errors.append(f"Packages weight mismatch {ob_id}: expected {db_w}, got {attr[0]}")
                else:
                    print(f"  OK: {ob_id} weight={attr[0]} ✓")
    else:
        print("  SKIP: no Packages column or no rows with packages")

    # ------------------------------------------------------------------
    # Check 5 — Employee role encoding (R1)
    # ------------------------------------------------------------------
    print("\n[Check 5] Employee role encoding (R1)...")
    cursor.execute("SELECT DISTINCT role FROM object_Employees WHERE role IS NOT NULL ORDER BY 1")
    roles = [r[0] for r in cursor.fetchall()]
    print(f"  Roles in DB ({len(roles)}): {roles}")

    emp_rows = ocel_df[ocel_df['Employees::ids'].apply(_has_objects)].sample(5, random_state=42)
    for _, row in emp_rows.iterrows():
        emp_ids  = ast.literal_eval(row['Employees::ids'])
        emp_attrs = ast.literal_eval(row['Employees::attributes'])
        for emp_id, enc in zip(emp_ids, emp_attrs):
            if not enc:
                continue
            if len(enc) != len(roles):
                errors.append(f"R1: employee dim {len(enc)} ≠ {len(roles)} for {emp_id}")
                continue
            if sum(enc) != 1:
                errors.append(f"R1: employee encoding not one-hot for {emp_id}: {enc}")
                continue
            cursor.execute("SELECT MAX(role) FROM object_Employees WHERE ocel_id=?", (emp_id,))
            db_role = cursor.fetchone()[0]
            if db_role not in roles:
                errors.append(f"R1: unknown role '{db_role}' for {emp_id}")
                continue
            if enc[roles.index(db_role)] != 1:
                errors.append(f"R1: role mismatch {emp_id}: role={db_role}, enc={enc}")
            else:
                print(f"  OK: {emp_id} → {db_role} → {enc} ✓")

    # ------------------------------------------------------------------
    # Check 6 — Customer encoding dimensions (R1)
    # ------------------------------------------------------------------
    print("\n[Check 6] Customer encoding (R1)...")
    cursor.execute("SELECT COUNT(DISTINCT ocel_id) FROM object_Customers")
    n_cust = cursor.fetchone()[0]
    cursor.execute("SELECT DISTINCT ocel_id FROM object_Customers ORDER BY 1")
    cust_list    = [r[0] for r in cursor.fetchall()]
    expected_dim = n_cust if n_cust < 50 else 1
    print(f"  {n_cust} distinct customers → expected {expected_dim}D encoding")

    for _, row in ocel_df[ocel_df['Customers::ids'].apply(_has_objects)].sample(5, random_state=42).iterrows():
        ids   = ast.literal_eval(row['Customers::ids'])
        attrs = ast.literal_eval(row['Customers::attributes'])
        for cust_id, enc in zip(ids, attrs):
            if len(enc) != expected_dim:
                errors.append(f"R1: customer dim {len(enc)} ≠ {expected_dim} for {cust_id}")
                continue
            if expected_dim > 1:
                if sum(enc) != 1:
                    errors.append(f"R1: customer encoding not one-hot for {cust_id}")
                    continue
                if cust_id not in cust_list:
                    errors.append(f"R1: unknown customer '{cust_id}'")
                    continue
                if enc[cust_list.index(cust_id)] != 1:
                    errors.append(f"R1: customer position mismatch for {cust_id}")
                else:
                    print(f"  OK: {cust_id} → pos {cust_list.index(cust_id)} ✓")

    # ------------------------------------------------------------------
    # Check 7 — Products attributes: weight + closest-timestamp price (D3)
    # Products use the time_attributes lookup (closest timestamp), not MAX.
    # ------------------------------------------------------------------
    print("\n[Check 7] Products attributes (weight, closest-timestamp price)...")
    prod_rows = ocel_df[ocel_df['Products::ids'].apply(_has_objects)].sample(3, random_state=42)

    for _, row in prod_rows.iterrows():
        ids   = ast.literal_eval(row['Products::ids'])
        attrs = ast.literal_eval(row['Products::attributes'])
        ts    = pd.Timestamp(row['timestamp'])
        for prod_id, attr in zip(ids, attrs):
            cursor.execute(
                "SELECT ocel_time, weight, price FROM object_Products "
                "WHERE ocel_id=? ORDER BY ocel_time",
                (prod_id,)
            )
            db_rows = cursor.fetchall()
            if not db_rows:
                continue
            closest = min(db_rows, key=lambda r: abs((pd.Timestamp(r[0]) - ts).total_seconds()))
            db_w, db_p = closest[1], closest[2]
            if db_w is None or db_p is None:
                # change-tracking row with null attrs — skip (ocel_generator uses COALESCE fallback)
                continue
            if abs(attr[0] - db_w) > 0.001 or abs(attr[1] - db_p) > 0.01:
                errors.append(
                    f"Products mismatch {prod_id} at {ts.date()}: "
                    f"expected ({db_w},{db_p}), got {attr}"
                )
            else:
                print(f"  OK: {prod_id} at {ts.date()} → weight={attr[0]}, price={attr[1]} ✓")

    # ------------------------------------------------------------------
    # Check 8 — Object completeness (all columns present, no NaN)
    # ------------------------------------------------------------------
    print("\n[Check 8] Object completeness (no NaN columns)...")
    obj_types = ['Orders', 'Customers', 'Employees', 'Items', 'Products', 'Packages']
    missing_cols = [ot for ot in obj_types if f'{ot}::ids' not in ocel_df.columns]
    if missing_cols:
        errors.append(f"Missing object type columns: {missing_cols}")
    else:
        nan_counts = {
            ot: ocel_df[f'{ot}::ids'].isna().sum()
            for ot in obj_types
        }
        any_nan = {ot: c for ot, c in nan_counts.items() if c > 0}
        if any_nan:
            errors.append(f"NaN values in object columns: {any_nan}")
        else:
            print(f"  OK: all {len(obj_types)} object type columns fully populated")

    # ------------------------------------------------------------------
    # Adams et al. coverage audit (informational)
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Adams et al. Feature Coverage Audit")
    print(f"{'='*60}")
    coverage = [
        ("C1 current activity",     "✓ present",  "ocel_generator.py: 11D one-hot ev_type"),
        ("C2 preceding activities", "~ implicit", "hetero_graphs.py: directly-follows edges"),
        ("C3 activity frequency",   "✓ present",  "hetero_graphs.py: 11D C3 in Events[17:28]"),
        ("D3 object attributes",    "✓ present",  "ocel_generator.py: price, weight"),
        ("D1/D2 agg. history",      "~ implicit", "HGT message passing over graph"),
        ("R1 resource identity",    "✓ present",  "ocel_generator.py: role + ID encodings"),
        ("R2/R3 workload",          "✗ absent",   "deferred"),
        ("P1 elapsed time",         "✓ present",  "hetero_graphs.py: elapsed_h (Events[11])"),
        ("P2 waiting time",         "✓ present",  "hetero_graphs.py: waiting_h (Events[12])"),
        ("P3-P5 sync/pooling",      "✗ absent",   "deferred (need per-event obj cols)"),
        ("P6-P10 object lag",       "✗ absent",   "deferred (need per-event obj cols)"),
        ("O1 object count",         "✓ present",  "hetero_graphs.py: n_items/n_products/n_packages on Orders"),
        ("O1-ext per-type counts",  "✓ present",  "hetero_graphs.py: 6D O1-ext in Events[28:34]"),
        ("O2-O6 per-event counts",  "✗ absent",   "deferred (need per-event obj cols)"),
    ]
    for feature, status, note in coverage:
        print(f"  {status:12s} {feature:28s} — {note}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    if errors:
        print(f"FAILED — {len(errors)} error(s):")
        for e in errors:
            print(f"  ✗ {e}")
    else:
        print("PASSED — all checks OK")
    print(f"{'='*60}\n")

    conn.close()
    return errors


def verify_hetero_graphs(database='order_management', cant=2000):
    """Verify that get_learning_set() produces graphs with correct Adams et al. feature values."""
    import torch

    print(f"\n{'='*60}")
    print(f"Verifying hetero_graphs output: {database}, cant={cant}")
    print(f"{'='*60}")

    errors = []
    sample = list(range(1, 6))
    hgg    = hg.HeteroGraphsGenerator(database, cant, sample, sample, sample)
    graphs = hgg.get_learning_set(sample)

    td         = hgg.tensor_dict
    n_ev_types = hgg.n_ev_types
    obj_order  = hgg.obj_type_order
    n_obj      = len(obj_order)
    c3_start   = n_ev_types + 6           # first C3 index (17 for order_management)
    c3_end     = c3_start + n_ev_types    # one past last C3 index (28)
    o1_start   = c3_end                   # first O1-ext index (28)
    o1_end     = o1_start + n_obj         # one past last O1-ext index (34)

    # ------------------------------------------------------------------
    # Check 1 — Feature dimensions match tensor_dict
    # ------------------------------------------------------------------
    print("\n[Check 1] Feature dimensions...")
    dim_errors_before = len(errors)
    for g in graphs[:10]:
        for node_type in g.node_types:
            if not hasattr(g[node_type], 'x') or g[node_type].x is None:
                continue
            actual   = g[node_type].x.shape[1]
            expected = td.get(node_type)
            if expected is None:
                continue
            if actual != expected:
                errors.append(
                    f"Dim mismatch for {node_type}: expected {expected}, got {actual}"
                )
    if len(errors) == dim_errors_before:
        print(f"  OK: Events={td['Events']}D, Orders={td['Orders']}D, "
              f"Employees={td['Employees']}D (first 10 graphs)")

    # ------------------------------------------------------------------
    # Check 2 — C3 structural sanity: C3 sum at event position j = j+1
    # ------------------------------------------------------------------
    print("\n[Check 2] C3 sum = event position + 1...")
    c2_violations = 0
    for g in graphs[:20]:
        for j in range(g['Events'].x.shape[0]):
            c3_sum = g['Events'].x[j, c3_start:c3_end].sum().item()
            if abs(c3_sum - (j + 1)) > 0.5:
                c2_violations += 1
                errors.append(f"C3 sum wrong at position {j}: expected {j+1}, got {c3_sum:.1f}")
    if c2_violations == 0:
        print(f"  OK: C3 sums equal position+1 across first 20 graphs")

    # ------------------------------------------------------------------
    # Check 3 — C3 monotonicity and non-negativity
    # ------------------------------------------------------------------
    print("\n[Check 3] C3 monotonicity and non-negativity...")
    c3_neg = c3_mono = 0
    for g in graphs[:20]:
        c3_block = g['Events'].x[:, c3_start:c3_end]
        if (c3_block < 0).any().item():
            c3_neg += 1
            errors.append("C3: negative value found")
        if g['Events'].x.shape[0] > 1:
            c3_sums = c3_block.sum(dim=1)
            if not (c3_sums[1:] >= c3_sums[:-1]).all().item():
                c3_mono += 1
                errors.append("C3: non-monotone sums across event nodes")
    if c3_neg == 0 and c3_mono == 0:
        print(f"  OK: C3 is monotone and non-negative across first 20 graphs")

    # ------------------------------------------------------------------
    # Check 4 — C3 correctness: compare against ocel.csv for trace 1
    # ------------------------------------------------------------------
    print("\n[Check 4] C3 values match manual counts from ocel.csv (trace 1)...")
    ev_log  = hgg.ev_log
    ocel_df = hgg.ocel_df
    start_t = ev_log[ev_log['vwpnt_id'] == 1]['timestamp'].values[0]
    end_t   = ev_log[ev_log['vwpnt_id'] == 1]['timestamp'].values[-1]
    trace_df = ocel_df[ocel_df['vwpnt_id'] == 1]
    trace_df = trace_df[
        (trace_df['timestamp'] >= start_t) &
        (trace_df['timestamp'] <  end_t)
    ]
    n1 = len(trace_df)

    expected_c3 = [0] * n_ev_types
    c4_ok = True
    for j, (_, row) in enumerate(trace_df.iterrows()):
        ev_vec = ast.literal_eval(row['ev_type'])
        expected_c3[ev_vec.index(1)] += 1
        actual_c3 = graphs[j]['Events'].x[-1, c3_start:c3_end].tolist()
        mismatches = [
            k for k, (a, e) in enumerate(zip(actual_c3, expected_c3))
            if abs(a - e) > 0.5
        ]
        if mismatches:
            errors.append(f"C3 value mismatch at prefix {j}, type indices {mismatches}")
            c4_ok = False
            break
    if c4_ok:
        print(f"  OK: C3 values correct for all {n1} prefixes of trace 1")

    # ------------------------------------------------------------------
    # Check 5 — n_packages: Orders last feature = Packages node count
    # ------------------------------------------------------------------
    print("\n[Check 5] n_packages: Orders.x[0,-1] == Packages node count...")
    pkg_violations = 0
    for g in graphs:
        if 'Orders' not in g.node_types or 'Packages' not in g.node_types:
            continue
        if not hasattr(g['Orders'], 'x') or not hasattr(g['Packages'], 'x'):
            continue
        n_pkg        = float(g['Packages'].x.shape[0])
        orders_last  = g['Orders'].x[0, -1].item()
        if abs(orders_last - n_pkg) > 0.5:
            pkg_violations += 1
            errors.append(
                f"n_packages mismatch: Orders.x[0,-1]={orders_last:.0f}, "
                f"Packages count={n_pkg:.0f}"
            )
    if pkg_violations == 0:
        print(f"  OK: n_packages matches Packages node count across all {len(graphs)} graphs")

    # ------------------------------------------------------------------
    # Check 6 — O1-ext: each event node's counts match its ocel.csv row (trace 1)
    # O1-ext is per-event (captures per-event object participation, i.e. Adams O4),
    # not trace-level: different events involve different numbers of objects.
    # ------------------------------------------------------------------
    print("\n[Check 6] O1-ext matches per-event object counts from ocel.csv (trace 1)...")
    o1_violations = 0
    for j, (_, row) in enumerate(trace_df.iterrows()):
        for k, ot in enumerate(obj_order):
            attr_col = f'{ot}::attributes'
            try:
                expected = float(len(ast.literal_eval(row[attr_col])))
            except Exception:
                expected = 0.0
            actual = graphs[j]['Events'].x[-1, o1_start + k].item()
            if abs(actual - expected) > 0.5:
                o1_violations += 1
                errors.append(
                    f"O1-ext mismatch for {ot} at prefix {j}: "
                    f"expected {expected:.0f}, got {actual:.0f}"
                )
    if o1_violations == 0:
        print(f"  OK: O1-ext per-event counts correct for all {n1} prefixes of trace 1")

    # ------------------------------------------------------------------
    # Check 7 — Temporal features: elapsed_h and waiting_h for trace 1
    # ------------------------------------------------------------------
    print("\n[Check 7] elapsed_h and waiting_h correct for trace 1...")
    start_ts = pd.to_datetime(start_t)
    prev_ts  = None
    t7_ok    = True
    for j, (_, row) in enumerate(trace_df.iterrows()):
        ts          = pd.to_datetime(row['timestamp'])
        exp_elapsed = (ts - start_ts).total_seconds() / 3600.0
        exp_waiting = 0.0 if prev_ts is None else (ts - prev_ts).total_seconds() / 3600.0
        prev_ts     = ts
        act_elapsed = graphs[j]['Events'].x[-1, 11].item()
        act_waiting = graphs[j]['Events'].x[-1, 12].item()
        if abs(act_elapsed - exp_elapsed) > 1e-3 or abs(act_waiting - exp_waiting) > 1e-3:
            errors.append(
                f"Temporal mismatch at prefix {j}: "
                f"elapsed exp={exp_elapsed:.4f} got={act_elapsed:.4f}, "
                f"waiting exp={exp_waiting:.4f} got={act_waiting:.4f}"
            )
            t7_ok = False
            break
    if t7_ok:
        print(f"  OK: elapsed_h and waiting_h correct for all {n1} prefixes of trace 1")

    # ------------------------------------------------------------------
    # Check 8 — Updated Adams et al. coverage audit (informational)
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Adams et al. Feature Coverage Audit (updated after gap implementation)")
    print(f"{'='*60}")
    coverage = [
        ("C1 current activity",     "✓ present",  f"ocel_generator.py: {n_ev_types}D one-hot ev_type"),
        ("C2 preceding activities", "~ implicit", "hetero_graphs.py: directly-follows edges"),
        ("C3 activity frequency",   "✓ present",  f"hetero_graphs.py: {n_ev_types}D C3 in Events[{c3_start}:{c3_end}]"),
        ("D3 object attributes",    "✓ present",  "ocel_generator.py: price, weight"),
        ("D1/D2 agg. history",      "~ implicit", "HGT message passing over graph"),
        ("R1 resource identity",    "✓ present",  "ocel_generator.py: role + ID encodings"),
        ("R2/R3 workload",          "✗ absent",   "deferred"),
        ("P1 elapsed time",         "✓ present",  "hetero_graphs.py: elapsed_h (Events[11])"),
        ("P2 waiting time",         "✓ present",  "hetero_graphs.py: waiting_h (Events[12])"),
        ("P3-P5 sync/pooling",      "✗ absent",   "deferred (need per-event obj cols)"),
        ("P6-P10 object lag",       "✗ absent",   "deferred (need per-event obj cols)"),
        ("O1 object count",         "✓ present",  "hetero_graphs.py: n_items/n_products/n_packages on Orders"),
        ("O1-ext per-type counts",  "✓ present",  f"hetero_graphs.py: {n_obj}D O1-ext in Events[{o1_start}:{o1_end}]"),
        ("O2-O6 per-event counts",  "✗ absent",   "deferred (need per-event obj cols)"),
    ]
    for feature, status, note in coverage:
        print(f"  {status:12s} {feature:28s} — {note}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    if errors:
        print(f"FAILED — {len(errors)} error(s):")
        for e in errors:
            print(f"  ✗ {e}")
    else:
        print("PASSED — all checks OK")
    print(f"{'='*60}\n")

    return errors


# MAIN
cant = 2000
database = 'order_management'

verify_process_generation(database, cant)
verify_ocel_generator(database, cant)
verify_hetero_graphs(database, cant)

# # Obtains all related nodes and arcs in the dataset and then generates the list of process executions
# p = pg.ProcessGeneration(database, cant)
# nodes = p.related_nodes()
# p.get_ev_log(nodes)

# # Generate the OCEL file with relevant attributes (required when role_encoding config changes)
# g = og.Generator(database, cant)
# g.generate_ocel(nodes)

# # # Apply the train test split to the set of process executions to obtain the relevant sets for learning set generation
# ttb = tb.TrainTestBuilder(database, cant)
# train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ttb.timestamps_generator()
#
# # # Obtains the learning set for training, testing and validation and converts it into pytorch tensors
# hgg = hg.HeteroGraphsGenerator(database, cant, train_sampled_timestamps,
#                                val_sampled_timestamps, test_sampled_timestamps)
# hgg.trace_kpi()
#
# m = t.Modelling(database, cant)
# m.sweep()
# m.Modelling()
#
# # ── Validation ────────────────────────────────────────────────────────────────
# import torch
# import pandas as pd
# import os
# import csv
# from torch_geometric.loader import DataLoader
# from model_classes import REG_GNN
#
# out_dir    = f"files/explainer_outputs/{database}/validation_2000"
# os.makedirs(out_dir, exist_ok=True)
# criterion  = torch.nn.L1Loss()
# batch_size = m.path_dict.get('batch_size', 16)
# vp         = m.viewpoint_object
#
# # 1. HGT test metrics (denormalised hours)
# m.model.load_state_dict(torch.load(m.model_path, weights_only=False))
# m.model.eval()
# records = []
# with torch.no_grad():
#     for g in m.test_data:
#         pred_h = (m.model(g.x_dict, g.edge_index_dict)[0].item()
#                   * m.target_std.item() + m.target_mean.item()) / 3600.0
#         true_h = (g[vp].y[0].item()
#                   * m.target_std.item() + m.target_mean.item()) / 3600.0
#         records.append({'true_h': true_h, 'pred_h': pred_h,
#                         'abs_err_h': abs(true_h - pred_h),
#                         'n_events': g['Events'].num_nodes,
#                         'last_event': bool(g[vp].last_event[0].item())})
# df   = pd.DataFrame(records)
# last = df[df['last_event']]
#
# def _metrics(sub, label):
#     mae  = sub['abs_err_h'].mean()
#     rmse = np.sqrt((sub['abs_err_h']**2).mean())
#     ss_r = ((sub['true_h'] - sub['pred_h'])**2).sum()
#     ss_t = ((sub['true_h'] - sub['true_h'].mean())**2).sum()
#     r2   = 1 - ss_r / ss_t
#     print(f"\n{'='*55}\nTEST METRICS — {label}  (n={len(sub)})\n{'='*55}")
#     print(f"  MAE  : {mae:.1f} h\n  RMSE : {rmse:.1f} h\n  R²   : {r2:.3f}")
#     return mae, rmse, r2
#
# _metrics(df,   "ALL prefixes")
# _metrics(last, "LAST-EVENT only")
#
# # 2. HomoGNN training + comparison plots
# m.Homo_Reg_Modelling()
# m.compare_models()
# m.plot_training_curves()
#
# # 3. Compute normalised test MAEs for results log
# test_loader = DataLoader(m.test_data, batch_size=batch_size, shuffle=False)
# hgt_mae_n   = m.het_loss_test(test_loader, m.model, criterion, m.device)
#
# homo_graphs = m._hetero_to_homo(m.test_data)
# homo_loader = DataLoader(homo_graphs, batch_size=batch_size, shuffle=False)
# in_ch       = homo_graphs[0].x.size(-1)
# homo_model  = REG_GNN.REG_GNN(in_channels=in_ch,
#                                hidden_channels=m.params.get('hidden_channels', 48),
#                                num_layers=m.params.get('num_layers', 3)).to(m.device)
# homo_model.load_state_dict(
#     torch.load(m.model_path.replace('.pth', '_homo.pth'), weights_only=False))
# homo_mae_n = m.homo_eval(homo_loader, homo_model, criterion, m.device)
#
# # 4. Append to results.csv
# results_path = m.path_dict['results_path']
# write_header = not os.path.exists(results_path)
# rows = [
#     ['Heterogeneous', m.task_id, hgt_mae_n,
#      m.target_mean.item(), m.target_std.item(), os.path.basename(m.model_path)],
#     ['Homogeneous', m.task_id, homo_mae_n,
#      m.target_mean.item(), m.target_std.item(),
#      os.path.basename(m.model_path.replace('.pth', '_homo.pth'))],
# ]
# with open(results_path, 'a', newline='') as f:
#     w = csv.writer(f)
#     if write_header:
#         w.writerow(['Graph Type', 'KPI', 'Metric', 'Mean', 'STD', 'Model'])
#     w.writerows(rows)
# print(f"\nResults appended to {results_path}")
#
# # e = exp.Explainer(database, cant)
# # e.explain_feature_attribution()