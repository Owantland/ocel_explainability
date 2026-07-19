"""Interactive dashboard for the perturbation-analysis explainability layer.

Explores the held-out TEST SET of a trained HGT model -- this is NOT a live
process-monitoring feed; every order shown here is from an already-completed
trace, not an in-progress case (unlike Galanti et al. 2023b's IBM Process Mining
dashboard, which this loosely follows for presentation style: a case picker plus
Global/Local explanation views).

Run with: streamlit run dashboard.py
"""
import os

import pandas as pd
import streamlit as st

import dashboard_cache as dc
import dashboard_precompute as dpc
import explainer as exp

DATASETS = {
    'order_management': 2000,
    'logistics': 1000,
}
MODE_LABELS = {
    'loo': 'Exhaustive LOO',
    'gnn_primary': 'GNNExplainer-primary',
    'ig': 'Feature Attribution (IG)',
    'cf': 'Counterfactual',
}
IG_METHODS = ('InputXGradient', 'IntegratedGradients')

st.set_page_config(page_title="Perturbation Analysis Explorer", layout="wide")


@st.cache_resource
def get_explainer(database, cant):
    return exp.Explainer(database, cant)


def order_ids_for(explainer):
    """Smaller, curated subset -- the same DEMO_N orders dashboard_precompute.py
    already warms into dashboard_cache/, so the default (last-event) prefix view
    for these orders stays cache-aligned instead of drifting into a second,
    independent definition of "the demo set"."""
    return dpc.demo_order_ids(explainer, dpc.DEMO_N)


def prefix_options_for(explainer, order_id):
    """All valid n_events (event-count) prefixes recorded for this order in the
    test split, ascending -- mirrors _locate_test_graph()'s own available-prefix
    enumeration (explainer.py) so the picker only ever offers values guaranteed
    to resolve to a real graph."""
    vp = explainer.kpi_viewpoint
    return sorted(
        g['Events'].x.size(0)
        for g in explainer.test_data
        if g[vp]['id'][0].item() == order_id
    )


def compute_local(explainer, database, cant, mode, order_id, ig_method=None,
                   min_gap_hours=0.0, direction='lower', n_events=None, top_k=5):
    if mode == 'loo':
        compute_fn = lambda: explainer.explain_trace(order_id, top_k=top_k, n_events=n_events)
        return dc.get_or_compute(database, cant, mode, order_id, compute_fn,
                                 n_events=n_events, top_k=top_k)
    if mode == 'gnn_primary':
        compute_fn = lambda: explainer.explain_gnn_primary(order_id, top_k=top_k, n_events=n_events)
        return dc.get_or_compute(database, cant, mode, order_id, compute_fn,
                                 n_events=n_events, top_k=top_k)

    if mode == 'cf':
        # Deliberately NOT cached -- explain_counterfactual() returns a bare list of
        # candidate dicts each holding a live PyG HeteroData (not JSON-serializable,
        # and a different shape than the loo/gnn_primary dict return), so adapting it
        # into dashboard_cache's schema isn't a clean fit. There's also a correctness
        # reason, not just convenience: min_gap_hours/direction are themselves part of
        # the query, so a result cached only by order_id would silently go stale the
        # moment either control changes. find_counterfactuals() is a forward-pass sweep
        # over the candidate pool (no 200-epoch optimization like GNNExplainer), so
        # it's in the same "fast enough live" tier as ig, not gnn_primary.
        results = explainer.explain_counterfactual(order_id, min_gap_hours=min_gap_hours,
                                                    direction=direction, n_events=n_events)
        suffix = f"_ev{n_events}" if n_events is not None else ""
        save_dir = os.path.join(explainer.path_dict['explainer_path'], f"order_{order_id}{suffix}_cf")
        query_graph = explainer._locate_test_graph(order_id, n_events)
        query_predicted_hours = explainer._predict_value_for_graph(query_graph, 0) / 3600.0
        return {'results': results, 'save_dir': save_dir,
                'query_predicted_hours': query_predicted_hours,
                'n_events': query_graph['Events'].x.size(0)}, False

    # 'ig': deliberately NOT cached -- explain_trace_ig() is a single backward
    # pass per method, already fast enough live (verified directly, no visible
    # delay), so the cache's whole reason to exist (GNNExplainer's ~200-epoch
    # cost) doesn't apply here. Its return value also isn't the same shape as
    # the other two modes' (no save_dir/predicted_hours, raw numpy arrays
    # instead of plain tuples -- not JSON-serializable as-is either), so it's
    # reconstructed here rather than adapted into dashboard_cache's schema.
    attribution = explainer.explain_trace_ig(order_id, methods=(ig_method,), n_events=n_events)
    suffix = f"_ev{n_events}" if n_events is not None else ""
    save_dir = os.path.join(explainer.path_dict['explainer_path'], f"order_{order_id}{suffix}")
    graph = explainer._locate_test_graph(order_id, n_events)
    predicted_hours = explainer._predict_value_for_graph(graph, 0) / 3600.0
    result = {
        'save_dir': save_dir,
        'predicted_hours': predicted_hours,
        'method': ig_method,
        'attribution': attribution,
        'n_events': graph['Events'].x.size(0),
    }
    return result, False


def _characterization_color(score):
    """Linear red->green interpolation for a characterization score in [0, 1],
    reusing this project's own established increase/decrease palette (the same
    hex pair plot_feature_importances()/plot_aggregate_explanation_bars() use in
    explainer.py) rather than introducing a new one. Clamped defensively --
    characterization_score is documented as bounded [0, 1] but isn't asserted
    to be at the call site."""
    score = max(0.0, min(1.0, score))
    red = (0xd6, 0x27, 0x28)
    green = (0x2c, 0xa0, 0x2c)
    r = round(red[0] + (green[0] - red[0]) * score)
    g = round(red[1] + (green[1] - red[1]) * score)
    b = round(red[2] + (green[2] - red[2]) * score)
    return f"rgb({r}, {g}, {b})"


def render_local(result, cached, mode, explainer, order_id, top_k=5):
    st.caption("served from cache" if cached else "computed just now (now cached for next time)")

    quality = result['metrics'] if mode == 'loo' else result['quality']
    score = quality['characterization_score']
    metric_col, events_col, score_col = st.columns(3)
    with metric_col:
        st.metric("Predicted remaining time", f"{result['predicted_hours']:.1f} h")
    with events_col:
        st.metric("Events completed", str(result['n_events']))
    with score_col:
        st.markdown(
            f"<div style='font-size: 0.875rem; color: white;'>"
            f"Characterization score</div>"
            f"<div style='font-size: 2.25rem; font-weight: 600; "
            f"color: {_characterization_color(score)};'>{score:.2f}</div>",
            unsafe_allow_html=True,
        )

    node_rows = result.get('node_importances') or []
    if node_rows:
        st.subheader("Top nodes")
        df = pd.DataFrame(
            node_rows,
            columns=['node_type', 'node_idx', 'shift_seconds', 'large_shift', 'signed_shift_seconds'],
        )
        df['shift_hours'] = df['shift_seconds'] / 3600.0
        df['signed_shift_hours'] = df['signed_shift_seconds'] / 3600.0

        # Human-readable identifier (Events activity name, or a real identity for any
        # other encoding-listed type, real OCEL_ID from ocel.csv for everything
        # else -- Items/Products/Packages/... now resolve to their actual
        # database identifier, e.g. "i-880001", not a positional placeholder),
        # same decoder explain_trace()'s console/CSV output already uses --
        # re-derived here (cheap graph lookup, no model inference) since the raw
        # node_importances tuples don't carry it. The positional
        # "{node_type}[{node_idx}]" fallback is kept only as a defensive safety
        # net (e.g. a node type genuinely absent from ocel.csv) -- in practice
        # _decode_all_identifiers() now resolves every real object type.
        graph = explainer._locate_test_graph(order_id, result['n_events'])
        id_map = explainer._decode_all_identifiers(graph, order_id, result['n_events'])
        df['identifier'] = df.apply(
            lambda r: id_map.get((r['node_type'], r['node_idx']),
                                 f"{r['node_type']}[{r['node_idx']}]"),
            axis=1,
        )

        df = df[['node_type', 'node_idx', 'identifier', 'signed_shift_hours', 'large_shift']]
        st.dataframe(df.head(top_k), width='stretch')

    if mode == 'loo':
        edge_rows = result.get('edge_importances') or []
        if edge_rows:
            st.subheader("Top edges")
            edf = pd.DataFrame(
                edge_rows,
                columns=['edge_type', 'edge_idx', 'shift_seconds', 'large_shift', 'signed_shift_seconds'],
            )
            edf['signed_shift_hours'] = edf['signed_shift_seconds'] / 3600.0
            st.dataframe(edf[['edge_type', 'signed_shift_hours', 'large_shift']].head(top_k),
                        width='stretch')

    save_dir = result['save_dir']
    col1, col2 = st.columns(2)
    with col1:
        png = os.path.join(save_dir, "node_type_summary.png")
        if os.path.exists(png):
            st.image(png, caption="Node-type importance")
    with col2:
        png = os.path.join(save_dir, "explanation_subgraph.png")
        if os.path.exists(png):
            st.image(png, caption="Explanation subgraph")

    if mode == 'loo':
        st.subheader("Explanation quality (exhaustive sweep)")
        st.json(result['metrics'])
    else:
        st.subheader("Joint impact of masking the identified nodes together")
        st.json(result['quality'])
        st.caption("Edge importance isn't available in this mode -- GNNExplainer has no edge "
                  "signal on this architecture. Switch to Exhaustive LOO for edge importance.")


def render_local_ig(result):
    """Feature attribution's return/artifact shape is genuinely different from
    the other two modes' (no save_dir/predicted_hours in the return value, a
    bar chart PER node type PER method rather than one fixed-name summary chart)
    -- a separate render path rather than a branch inside render_local()."""
    st.caption(f"Method: {result['method']} -- not cached (fast enough to compute live)")
    metric_col, events_col = st.columns(2)
    with metric_col:
        st.metric("Predicted remaining time", f"{result['predicted_hours']:.1f} h")
    with events_col:
        st.metric("Events completed", str(result['n_events']))

    save_dir = result['save_dir']
    suffix = result['method'].lower()

    heatmap_png = os.path.join(save_dir, f"ig_heatmap_{suffix}.png")
    if os.path.exists(heatmap_png):
        st.image(heatmap_png, caption="Attribution heatmap (all node types x feature dims)")

    csv_path = os.path.join(save_dir, f"ig_attribution_{suffix}.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        top_types = (df.groupby('node_type')['abs'].mean()
                     .sort_values(ascending=False).head(2).index.tolist())

        if top_types:
            st.subheader(f"Top node types by mean |attribution|: {', '.join(top_types)}")
            cols = st.columns(len(top_types))
            for col, nt in zip(cols, top_types):
                with col:
                    png = os.path.join(save_dir, f"ig_attribution_{nt.lower()}_{suffix}.png")
                    if os.path.exists(png):
                        st.image(png)

        st.subheader("Full attribution table")
        st.dataframe(df.sort_values('abs', ascending=False).head(20), width='stretch')


def render_local_cf(result, explainer):
    """Counterfactual's result shape (a ranked list of candidate matches, not a
    single prediction + node-importance table) doesn't fit render_local()'s
    assumptions -- a separate render path, same reasoning as render_local_ig()."""
    st.caption("Not cached -- recomputed on every click (threshold/direction are part "
              "of the query, so a stale cache could silently answer the wrong question).")
    metric_col, events_col = st.columns(2)
    with metric_col:
        st.metric("Predicted remaining time (query)", f"{result['query_predicted_hours']:.1f} h")
    with events_col:
        st.metric("Events completed (query)", str(result['n_events']))

    results = result['results']
    if not results:
        st.warning("No counterfactuals found for this order under the current threshold/direction.")
        return

    st.subheader(f"Top {len(results)} counterfactual(s)")
    df = pd.DataFrame([
        {'rank': i, 'order_id': r['order_id'], 'predicted_hours': r['predicted_hours'],
         'dissimilarity': r['dissimilarity'], 'n_events': r['n_events'],
         'feat': r['components']['feat'], 'type': r['components']['type'],
         'edge': r['components']['edge'], 'struct': r['components']['struct']}
        for i, r in enumerate(results, 1)
    ])
    st.dataframe(df, width='stretch')

    save_dir = result['save_dir']
    png = os.path.join(save_dir, "cf_graph_structure_comparison.png")
    if os.path.exists(png):
        st.image(png, caption="Graph structure comparison (query vs. best CF)")

    col1, col2 = st.columns(2)
    with col1:
        png = os.path.join(save_dir, "cf_node_type_comparison.png")
        if os.path.exists(png):
            st.image(png, caption="Node-type counts (query vs. best CF)")
    with col2:
        png = os.path.join(save_dir, "cf_dissimilarity_breakdown.png")
        if os.path.exists(png):
            st.image(png, caption="Dissimilarity breakdown")

    col3, col4 = st.columns(2)
    with col3:
        png = os.path.join(save_dir, "cf_event_type_diff.png")
        if os.path.exists(png):
            st.image(png, caption="Event-type differences")
    with col4:
        vp = explainer.kpi_viewpoint
        png = os.path.join(save_dir, f"cf_{vp.lower()}_feature_diff.png")
        if os.path.exists(png):
            st.image(png, caption=f"{vp} feature differences")


def render_global(database, mode, ig_method=None):
    base = os.path.join("files", "explainer_outputs", database)
    if mode == 'ig':
        render_global_ig(base, ig_method)
        return

    if mode == 'cf':
        csv_path = os.path.join(base, "aggregate", "aggregate_cf_dissimilarity.csv")
        png_path = os.path.join(base, "aggregate", "aggregate_cf_components.png")
    elif mode == 'loo':
        csv_path = os.path.join(base, "aggregate", "aggregate_metrics.csv")
        png_path = os.path.join(base, "aggregate", "aggregate_node_type_importance.png")
    else:
        csv_path = os.path.join(base, "aggregate_gnnprimary", "aggregate_gnnprimary_metrics.csv")
        png_path = os.path.join(base, "aggregate_gnnprimary", "aggregate_gnnprimary_type_summary.png")

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        st.caption(f"From {csv_path} (n={len(df)} traces)")
        numeric_cols = df.select_dtypes(include='number').columns
        st.dataframe(df[numeric_cols].agg(['mean', 'std']), width='stretch')
    else:
        st.warning(f"No precomputed aggregate found at {csv_path}. Run it via Regenerate below, "
                  f"or from the command line first.")

    if os.path.exists(png_path):
        st.image(png_path)


def render_global_ig(base, ig_method):
    """Dataset-wide feature attribution -- reads files/explainer_outputs/{database}/
    attribution/, the fixed output folder explain_feature_attribution() already
    writes to (not per-order like the other two modes' aggregate folders).
    Different CSV columns (mean_signed/mean_abs, not signed/abs) and PNG naming
    (ig_{node_type}_importance_{suffix}.png, not ig_attribution_{node_type}_{suffix}.png)
    than the single-trace path, so this reuses the same top-2 curation idea as
    render_local_ig() but can't reuse its code directly."""
    out_dir = os.path.join(base, "attribution")
    suffix = ig_method.lower()
    csv_path = os.path.join(out_dir, f"ig_attribution_{suffix}.csv")
    heatmap_png = os.path.join(out_dir, f"ig_heatmap_{suffix}.png")

    if not os.path.exists(csv_path):
        st.warning(f"No precomputed attribution found at {csv_path}. Run it via Regenerate below, "
                  f"or from the command line first.")
        return

    df = pd.read_csv(csv_path)
    st.caption(f"From {csv_path}")
    if os.path.exists(heatmap_png):
        st.image(heatmap_png, caption="Attribution heatmap (all node types x feature dims)")

    top_types = (df.groupby('node_type')['mean_abs'].mean()
                 .sort_values(ascending=False).head(2).index.tolist())
    if top_types:
        st.subheader(f"Top node types by mean |attribution|: {', '.join(top_types)}")
        cols = st.columns(len(top_types))
        for col, nt in zip(cols, top_types):
            with col:
                png = os.path.join(out_dir, f"ig_{nt.lower()}_importance_{suffix}.png")
                if os.path.exists(png):
                    st.image(png)

    st.subheader("Full attribution table")
    st.dataframe(df.sort_values('mean_abs', ascending=False).head(20), width='stretch')


def render_regenerate(explainer, database, cant, mode, ig_method=None,
                      min_gap_hours=0.0, direction='lower'):
    cost_notes = {
        'loo': "Exhaustive LOO is slow (O(n_traces x nodes+edges) forward passes).",
        'gnn_primary': "GNNExplainer-primary is much slower (n_traces x 200-epoch mask optimizations).",
        'ig': "Feature attribution is the cheapest of the three -- a single backward pass per "
              "trace, comparable to or faster than Exhaustive LOO's aggregate.",
        'cf': "Counterfactual retrieval evaluates predictions over the full candidate pool per "
              "trace -- moderate cost, no gradient-based optimization involved.",
    }
    st.caption("Recomputes the aggregate from scratch and overwrites the CSV/PNG shown above -- "
              f"only needed after retraining the underlying model. {cost_notes[mode]}")
    if st.button(f"Regenerate {MODE_LABELS[mode]} aggregate (n_traces=50)", key=f"regen_{mode}"):
        with st.spinner("Running aggregate explanation -- this can take a while..."):
            if mode == 'loo':
                explainer.explain_aggregate(n_traces=50)
            elif mode == 'gnn_primary':
                explainer.explain_gnn_primary_aggregate(n_traces=50)
            elif mode == 'cf':
                explainer.explain_aggregate_counterfactuals(
                    n_traces=50, min_gap_hours=min_gap_hours, direction=direction
                )
            else:
                explainer.explain_feature_attribution(n_traces=50, methods=(ig_method,))
        st.success("Done. Reload the page to see the updated figures.")


def main():
    st.title("Perturbation Analysis Explorer")
    st.caption(
        "Exploring the held-out **test set** of a trained HGT model -- static, already-completed "
        "traces, not a live process feed."
    )

    with st.sidebar:
        database = st.selectbox("Dataset", list(DATASETS.keys()))
        cant = DATASETS[database]
        mode = st.radio(
            "Explanation mode", list(MODE_LABELS.keys()),
            format_func=lambda m: MODE_LABELS[m],
        )
        ig_method = None
        min_gap_hours, direction = 0.0, 'lower'
        top_k = 5
        if mode in ('loo', 'gnn_primary'):
            top_k = st.selectbox(
                "Top K Explanations", [3, 5, 10, 15, 20], index=1, key="top_k_picker",
                help="Number of nodes/edges shown in the importance tables (and, for "
                     "GNNExplainer-primary, the number GNNExplainer identifies and LOO measures).",
            )
        if mode == 'gnn_primary':
            st.caption("GNNExplainer identifies important nodes; LOO estimates their impact. "
                      "Node-only -- no edge importance.")
        elif mode == 'ig':
            ig_method = st.radio("IG method", IG_METHODS, key="ig_method_picker")
            st.caption("Gradient-based feature sensitivity, not perturbation -- complements "
                      "the other two modes rather than competing with them. Not cached; "
                      "cheap enough to compute live every time.")
        elif mode == 'cf':
            direction_label = st.radio(
                "Search direction",
                ["Below current prediction", "Above current prediction"],
                key="cf_direction_picker",
            )
            direction = 'lower' if direction_label.startswith("Below") else 'higher'
            min_gap_hours = st.number_input(
                "Minimum predicted-hours gap (threshold)", min_value=0.0, value=0.0, step=1.0,
                key="cf_min_gap_picker",
                help="Minimum |query - candidate| predicted-hours gap a counterfactual "
                     "must have to be eligible.",
            )
            st.caption("Retrieves the most similar test-set trace(s) with a contrasting "
                      "predicted outcome -- not a perturbation method, complements the "
                      "other three modes.")
        else:
            st.caption("Exhaustive sweep over every node, edge, and feature. Slower, but the "
                      "only source of edge importance.")

    explainer = get_explainer(database, cant)

    # Global (aggregate) tab dropped for now -- Local is the only view. render_global()/
    # render_global_ig()/render_regenerate() are left defined but unused, not deleted,
    # since this is a temporary scoping choice, not a permanent removal.
    ids = order_ids_for(explainer)
    order_id = st.selectbox("Order ID", ids, key="order_picker")

    prefix_opts = prefix_options_for(explainer, order_id)
    last_prefix = prefix_opts[-1]
    selected_prefix = st.select_slider(
        "Prefix (events completed)", options=prefix_opts, value=last_prefix,
        key="prefix_picker",
        format_func=lambda n: f"{n} (complete)" if n == last_prefix else str(n),
    )
    # The complete prefix maps back to n_events=None, not the literal max int, even
    # though both resolve to the same graph -- preserves exact parity with every other
    # explain_*() call's "last recorded prefix" convention, and keeps hitting
    # dashboard_precompute.py's already-warmed cache (keyed under n_events=None) for
    # the default view instead of silently missing it on a different cache key.
    n_events = None if selected_prefix == last_prefix else selected_prefix

    if st.button("Explain this order"):
        spinner_msg = ("Computing (GNNExplainer-primary can take ~1 minute on a cold cache)..."
                      if mode == 'gnn_primary' else "Computing...")
        try:
            with st.spinner(spinner_msg):
                result, cached = compute_local(explainer, database, cant, mode, order_id,
                                                ig_method=ig_method,
                                                min_gap_hours=min_gap_hours, direction=direction,
                                                n_events=n_events, top_k=top_k)
            if mode == 'ig':
                render_local_ig(result)
            elif mode == 'cf':
                render_local_cf(result, explainer)
            else:
                render_local(result, cached, mode, explainer, order_id, top_k=top_k)
        except ValueError as ex:
            # explain_gnn_primary()'s guard against a legitimately-empty edge
            # type (a real PyG/GNNExplainer limitation, not every order/prefix
            # supports this mode) -- show a clear message instead of a raw traceback.
            st.error(f"This order isn't explainable in {MODE_LABELS[mode]} mode: {ex}")


if __name__ == '__main__':
    main()
