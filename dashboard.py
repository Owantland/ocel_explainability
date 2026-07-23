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
    'cf': 'Counterfactual',
}

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


def compute_local(explainer, database, cant, mode, order_id,
                   min_gap_hours=0.0, direction='lower', n_events=None, top_k=5):
    # Ties a cached result to the exact model checkpoint that produced it -- found via
    # direct investigation that a retrain silently orphans old cached predictions (e.g.
    # predicted_hours) without this, since nothing about the cache key otherwise reflects
    # which checkpoint the cached values came from.
    checkpoint_fingerprint = int(os.path.getmtime(explainer.model_path))
    if mode == 'loo':
        # Shapley-based, not LOO, despite the mode name -- explain_trace_shapley()
        # returns the same dict shape as explain_trace() (see its own docstring),
        # so every downstream renderer/consumer here is unaffected. explain_trace()
        # itself is untouched -- compare_loo_vs_shapley() and other citable
        # pathways still call it directly.
        compute_fn = lambda: explainer.explain_trace_shapley(order_id, top_k=top_k, n_events=n_events)
        return dc.get_or_compute(database, cant, mode, order_id, compute_fn,
                                 n_events=n_events, top_k=top_k,
                                 checkpoint_fingerprint=checkpoint_fingerprint)

    if mode == 'cf':
        # Deliberately NOT cached -- explain_counterfactual() returns a bare list of
        # candidate dicts each holding a live PyG HeteroData (not JSON-serializable,
        # and a different shape than the loo/gnn_primary dict return), so adapting it
        # into dashboard_cache's schema isn't a clean fit. There's also a correctness
        # reason, not just convenience: min_gap_hours/direction are themselves part of
        # the query, so a result cached only by order_id would silently go stale the
        # moment either control changes. find_counterfactuals() is a forward-pass sweep
        # over the candidate pool (no 200-epoch optimization like GNNExplainer), so
        # it's in the same "fast enough live" tier as this section's own top-K
        # attribution/LOO comparison, not gnn_primary.
        results = explainer.explain_counterfactual(order_id, min_gap_hours=min_gap_hours,
                                                    direction=direction, n_events=n_events)
        suffix = f"_ev{n_events}" if n_events is not None else ""
        save_dir = os.path.join(explainer.path_dict['explainer_path'], f"order_{order_id}{suffix}_cf")
        query_graph = explainer._locate_test_graph(order_id, n_events)
        query_predicted_hours = explainer._predict_value_for_graph(query_graph, 0) / 3600.0
        return {'results': results, 'save_dir': save_dir,
                'query_predicted_hours': query_predicted_hours,
                'n_events': query_graph['Events'].x.size(0)}, False


_TEMPORAL_LABELS = {
    'elapsed_h': 'Elapsed time', 'waiting_h': 'Waiting time',
    'hour_sin': 'Hour of day (sin)', 'hour_cos': 'Hour of day (cos)',
    'dow_sin': 'Day of week (sin)', 'dow_cos': 'Day of week (cos)',
}
_VIEWPOINT_LABELS = {
    'n_items': 'Number of items', 'n_products': 'Number of products',
    'n_packages': 'Number of packages', 'total_weight': 'Total weight',
}


def humanize_feature_name(raw_name):
    """Raw internal column name -> readable label, for features calculated
    following Adams et al. 2022's OCEL feature-extraction definitions (temporal,
    C3 activity-frequency counts, O1 object counts). Literal pass-through data
    values (raw attribute columns, event/role/company/ID names) are left
    unchanged -- they're not "calculated" features and mostly already read fine
    as-is. Dashboard-display-only; explainer.py's own saved/thesis-citable PNGs
    are unaffected (this is never applied there by default)."""
    if raw_name in _TEMPORAL_LABELS:
        return _TEMPORAL_LABELS[raw_name]
    if raw_name in _VIEWPOINT_LABELS:
        return _VIEWPOINT_LABELS[raw_name]
    if raw_name.startswith('c3_'):
        return f"Count_{raw_name[len('c3_'):]}"
    if raw_name.startswith('o1_'):
        return f"Count_{raw_name[len('o1_'):]}"
    if raw_name.endswith('_present'):
        return f"{raw_name[:-len('_present')]} present"
    return raw_name


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


def render_local(result, cached, explainer, order_id, top_k=5):
    st.caption("served from cache" if cached else "computed just now (now cached for next time)")

    quality = result['metrics']
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

    col1, col2 = st.columns(2)
    with col1:
        png = os.path.join(result['save_dir'], "node_type_summary.png")
        if os.path.exists(png):
            st.image(png, caption="Node-type importance")
    with col2:
        png = os.path.join(result['save_dir'], "explanation_subgraph.png")
        if os.path.exists(png):
            st.image(png, caption="Explanation subgraph")

    # Human-readable identifier (Events activity name, or a real identity for any
    # other encoding-listed type, real OCEL_ID from ocel.csv for everything
    # else -- Items/Products/Packages/... now resolve to their actual
    # database identifier, e.g. "i-880001", not a positional placeholder),
    # same decoder explain_trace()'s console/CSV output already uses -- computed
    # here (cheap graph lookup, no model inference) rather than inside the "Top
    # nodes" block below, since the node-type/instance selectors further down
    # need graph/id_map too, regardless of whether node_rows is non-empty.
    graph = explainer._locate_test_graph(order_id, result['n_events'])
    id_map = explainer._decode_all_identifiers(graph, order_id, result['n_events'])

    node_rows = result.get('node_importances') or []
    if node_rows:
        st.subheader("Top nodes")
        df = pd.DataFrame(
            node_rows,
            columns=['node_type', 'node_idx', 'shift_seconds', 'large_shift', 'signed_shift_seconds'],
        )
        df['shift_hours'] = df['shift_seconds'] / 3600.0
        df['signed_shift_hours'] = df['signed_shift_seconds'] / 3600.0
        df['identifier'] = df.apply(
            lambda r: id_map.get((r['node_type'], r['node_idx']),
                                 f"{r['node_type']}[{r['node_idx']}]"),
            axis=1,
        )

        df = df[['node_type', 'node_idx', 'identifier', 'signed_shift_hours', 'large_shift']]
        st.dataframe(df.head(top_k), width='stretch')

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

    render_top_features_by_attribution(explainer, graph, id_map, result, order_id, top_k)

    st.subheader("Explanation quality (exhaustive sweep)")
    st.json(result['metrics'])


def render_top_features_by_attribution(explainer, graph, id_map, result, order_id, top_k):
    """Top-K (node, feature) pairs across the WHOLE trace, ranked by |attribution|
    -- not scoped to one selected node. Computed live (no caching), same cost
    tier as before: one Captum backward pass over the whole graph already
    returns every node instance's own attribution vector -- flattening and
    ranking it is cheap by comparison. Also shows the Shapley value for these
    SAME features (not a separate re-ranking), so the two charts are a direct,
    feature-for-feature comparison of what each method says about the identical
    set. Was LOO before this was swapped to Shapley -- LOO's own
    reg_feature_importance_for_node_in_graph() is untouched, still used
    elsewhere (e.g. compare_loo_vs_shapley())."""
    masks = explainer._compute_attribution_for_graph(graph, method='InputXGradient')
    rows = []
    for node_type, arr in masks.items():
        feat_names = explainer.feature_names.get(node_type, [])
        for node_idx in range(arr.shape[0]):
            for f in range(arr.shape[1]):
                v = float(arr[node_idx, f])
                fname = feat_names[f] if f < len(feat_names) else f"feat_{f}"
                rows.append({
                    'node_type': node_type, 'node_idx': node_idx, 'feature_idx': f,
                    'node': id_map.get((node_type, node_idx), f"{node_type}[{node_idx}]"),
                    'feature': humanize_feature_name(fname),
                    'signed': v, 'abs': abs(v),
                })
    df = pd.DataFrame(rows).sort_values('abs', ascending=False).head(top_k)

    # Shapley value for the SAME (node, feature) pairs attribution picked --
    # baseline computed fresh (not from a cached result, see earlier stale-baseline fix),
    # and grouped by node instance so each node's Shapley sweep runs once regardless of
    # how many of its features made the top-K. feature_indices=None (not the page's
    # top_k) so the sweep covers every one of that node's nonzero features -- otherwise
    # the specific feature index needed could be truncated out of a smaller top-k slice.
    baseline_seconds = explainer._predict_value_for_graph(graph, 0)
    shapley_lookup = {}
    for node_type, node_idx in df[['node_type', 'node_idx']].drop_duplicates().itertuples(index=False):
        feats = explainer.shapley_feature_importance_for_node(
            graph, node_type, node_idx, baseline_seconds, target_object_idx=0, n_samples=100
        )
        shapley_lookup[(node_type, node_idx)] = {f: sv / 3600.0 for f, sv in feats.items()}
    # None only for a feature whose input value is exactly 0 (skipped by the Shapley
    # method's own zero-value guard, same as LOO's) -- rare in practice, since
    # InputXGradient's own attribution for a zero-valued input is itself 0 and wouldn't
    # usually rank in the top-K to begin with.
    df['shapley_value_hours'] = df.apply(
        lambda r: shapley_lookup.get((r['node_type'], r['node_idx']), {}).get(r['feature_idx']),
        axis=1,
    )

    save_dir = result['save_dir']
    labels = [f"{r['node']}: {r['feature']}" for _, r in df.iterrows()]

    attr_png = os.path.join(save_dir, "top_features_attribution.png")
    explainer.plot_top_features_bar(labels, df['signed'].tolist(), attr_png,
                                    title=f"Top {top_k} features by attribution, order #{order_id}")

    shapley_png = os.path.join(save_dir, "top_features_shapley.png")
    shapley_values = df['shapley_value_hours'].fillna(0.0).tolist()
    explainer.plot_top_features_bar(labels, shapley_values, shapley_png,
                                    title=f"Shapley value, same features, order #{order_id}",
                                    xlabel="Shift in Hours")

    st.subheader(f"Top {top_k} features by attribution, compared against Shapley value")
    col1, col2 = st.columns(2)
    with col1:
        if os.path.exists(attr_png):
            st.image(attr_png)
        st.caption("Feature attribution (InputXGradient)")
    with col2:
        if os.path.exists(shapley_png):
            st.image(shapley_png)
        st.caption("Shapley value (hours)")
    st.dataframe(
        df[['node_type', 'node', 'feature', 'signed', 'abs', 'shapley_value_hours']],
        width='stretch',
    )



def render_local_cf(result, explainer):
    """Counterfactual's result shape (a ranked list of candidate matches, not a
    single prediction + node-importance table) doesn't fit render_local()'s
    assumptions -- a separate render path."""
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


def render_global(database, mode):
    base = os.path.join("files", "explainer_outputs", database)
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


def render_regenerate(explainer, database, cant, mode,
                      min_gap_hours=0.0, direction='lower'):
    cost_notes = {
        'loo': "The two flat 'top K' charts use Shapley values (permutation-sampled on top "
              "of an exhaustive LOO identification pass); the node-type chart and metrics "
              "below stay LOO-based. Exhaustive LOO alone is already slow "
              "(O(n_traces x nodes+edges) forward passes); Shapley requantification adds a "
              "bounded amount on top of that.",
        'cf': "Counterfactual retrieval evaluates predictions over the full candidate pool per "
              "trace -- moderate cost, no gradient-based optimization involved.",
    }
    st.caption("Recomputes the aggregate from scratch and overwrites the CSV/PNG shown above -- "
              f"only needed after retraining the underlying model. {cost_notes[mode]}")
    if st.button(f"Regenerate {MODE_LABELS[mode]} aggregate (n_traces=50)", key=f"regen_{mode}"):
        with st.spinner("Running aggregate explanation -- this can take a while..."):
            if mode == 'loo':
                # explain_aggregate() still runs (unchanged) -- it's what produces the
                # node-type chart and metrics CSV, which stay LOO-based;
                # explain_aggregate_shapley() additionally refreshes the two flat
                # "top K" bar charts with Shapley values, writing to the same file
                # paths so one button keeps every view on this tab current.
                explainer.explain_aggregate(n_traces=50)
                explainer.explain_aggregate_shapley(n_traces=50)
            elif mode == 'cf':
                explainer.explain_aggregate_counterfactuals(
                    n_traces=50, min_gap_hours=min_gap_hours, direction=direction
                )
        st.success("Done. Reload the page to see the updated figures.")


def render_local_flow(explainer, database, cant, mode, top_k, min_gap_hours=0.0, direction='lower'):
    """Order picker through persisted render -- extracted from main() so it can be
    called either inside a tab (loo, which also gets an Aggregate tab alongside this
    one) or directly (cf, which stays a flat single view, no tabs)."""
    # Global (aggregate) tab dropped for now for non-loo modes -- render_global()/
    # render_regenerate() are left defined but unused there, not deleted, since this
    # is a temporary scoping choice, not a permanent removal. loo gets its own
    # Aggregate tab via render_loo_aggregate(), wired up in main().
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
        try:
            with st.spinner("Computing..."):
                result, cached = compute_local(explainer, database, cant, mode, order_id,
                                                min_gap_hours=min_gap_hours, direction=direction,
                                                n_events=n_events, top_k=top_k)
            if mode == 'cf':
                render_local_cf(result, explainer)
            else:
                # Stored rather than rendered inline -- lets the render survive reruns
                # triggered by other sidebar interactions (e.g. Top-K) without forcing a
                # re-click of "Explain this order" each time. See the persisted-render
                # block below.
                st.session_state['local_ctx'] = (database, cant, mode, order_id, n_events, top_k)
                st.session_state['local_result'] = result
                st.session_state['local_cached'] = cached
        except ValueError as ex:
            # explain_trace()'s own guards (e.g. a legitimately-empty edge type on the
            # LOO side) -- show a clear message instead of a raw traceback.
            st.error(f"This order isn't explainable in {MODE_LABELS[mode]} mode: {ex}")

    # loo renders from persisted state, not inline in the button block above -- this fires
    # both on the same rerun right after a successful click (ctx matches immediately) and
    # on later reruns triggered by other controls. Any sidebar/toggle change changes ctx
    # and naturally stops the stale render, with no extra invalidation logic needed.
    ctx = (database, cant, mode, order_id, n_events, top_k)
    if mode == 'loo' and st.session_state.get('local_ctx') == ctx:
        render_local(st.session_state['local_result'], st.session_state['local_cached'],
                    explainer, order_id, top_k=top_k)


def render_loo_aggregate(explainer, database, cant, top_k):
    """Dataset-wide "top K nodes"/"top K features" bar charts -- Shapley-based, not
    LOO, despite the function name (kept for the mode='loo' call site; the tab
    itself is still labeled "Exhaustive LOO" since candidate identification still
    is). The underlying data is whatever was last computed via Regenerate
    (explain_aggregate_shapley(), n_traces=50: an exhaustive LOO sweep to identify
    the top ~20 candidates per chart, then a bounded Shapley requantification of
    just those) -- not recomputed live per Top-K change, since that would mean
    re-running the sweep on every dropdown interaction. The DISPLAY (chart +
    table), however, IS live: explain_aggregate_shapley() saves a generous
    top_n=20 rows precisely so this can cheaply slice/re-render down to the
    current top_k on every rerun, matplotlib-only, no model inference -- same
    cost tier as the single-trace tab's own live charts. Reuses
    plot_top_features_bar() (not plot_aggregate_explanation_bars(), which also
    feeds explain_gnn_primary_aggregate()'s thesis-citable output and is left on
    its own existing color convention) so this chart automatically matches the
    single-trace tab's color scheme.

    The node-type chart and the feature-attribution-by-depth heatmap below stay
    LOO-based (see explain_aggregate_shapley()'s own docstring for why: both are
    exhaustive-by-design, and Shapley-izing them fully would multiply their
    already-largest cost ~100x)."""
    base = os.path.join("files", "explainer_outputs", database, "aggregate")
    csv_path = os.path.join(base, "aggregate_explanation_bars.csv")

    if not os.path.exists(csv_path):
        st.warning(f"No precomputed aggregate found at {csv_path}. Use Regenerate below to "
                  "compute it (an exhaustive LOO sweep over 50 traces to identify candidates, "
                  "then Shapley requantification of the top ~20 -- can take a while).")
    else:
        df = pd.read_csv(csv_path)
        df_top = df.sort_values('mean_signed_shift', key=abs, ascending=False).head(top_k)

        chart_png = os.path.join(base, "aggregate_explanation_bars_topk.png")
        explainer.plot_top_features_bar(
            df_top['label'].tolist(), df_top['mean_signed_shift'].tolist(), chart_png,
            title=f"Top {top_k} nodes by aggregate value",
            xlabel="Shift in Hours",
        )

        st.caption(f"From {csv_path} (top {len(df_top)} of {len(df)} saved nodes by mean "
                  f"|signed shift| across traces)")
        st.subheader("Top nodes by aggregate value")
        if os.path.exists(chart_png):
            st.image(chart_png)
        st.dataframe(df_top, width='stretch')

    feat_csv_path = os.path.join(base, "aggregate_feature_bars.csv")
    if not os.path.exists(feat_csv_path):
        st.warning(f"No precomputed feature aggregate found at {feat_csv_path}. Use Regenerate "
                  "below to compute it.")
    else:
        fdf = pd.read_csv(feat_csv_path)
        fdf_top = fdf.sort_values('mean_signed_shift', key=abs, ascending=False).head(top_k)

        feat_chart_png = os.path.join(base, "aggregate_feature_bars_topk.png")
        explainer.plot_top_features_bar(
            fdf_top['label'].tolist(), fdf_top['mean_signed_shift'].tolist(), feat_chart_png,
            title=f"Top {top_k} features by aggregate value",
            xlabel="Shift in Hours",
        )

        st.caption(f"From {feat_csv_path} (top {len(fdf_top)} of {len(fdf)} saved features by "
                  f"mean |signed shift| across traces)")
        st.subheader("Top features by aggregate value")
        if os.path.exists(feat_chart_png):
            st.image(feat_chart_png)
        st.dataframe(fdf_top, width='stretch')

    with st.expander("Regenerate (advanced)"):
        render_regenerate(explainer, database, cant, 'loo')


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
        min_gap_hours, direction = 0.0, 'lower'
        top_k = 5
        if mode == 'loo':
            top_k = st.selectbox(
                "Top K Explanations", [3, 5, 10, 15, 20], index=1, key="top_k_picker",
                help="Number of nodes/edges shown in the importance tables.",
            )
        if mode == 'cf':
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
                      "other mode.")
        else:
            st.caption("Exhaustive sweep over every node, edge, and feature -- the only source "
                      "of edge importance.")

    explainer = get_explainer(database, cant)

    # Aggregate tab scoped to loo only, per what was asked -- cf keeps today's flat,
    # tab-free layout. render_local_flow() is shared between both branches so the
    # (fairly stateful) order-picker-through-persisted-render logic isn't duplicated.
    if mode == 'loo':
        tab_local, tab_aggregate = st.tabs(["Local (single case)", "Aggregate"])
        with tab_local:
            render_local_flow(explainer, database, cant, mode, top_k)
        with tab_aggregate:
            render_loo_aggregate(explainer, database, cant, top_k)
    else:
        render_local_flow(explainer, database, cant, mode, top_k,
                          min_gap_hours=min_gap_hours, direction=direction)


if __name__ == '__main__':
    main()
