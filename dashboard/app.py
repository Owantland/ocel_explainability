"""Interactive dashboard for the perturbation-analysis explainability layer.

Explores the held-out TEST SET of a trained HGT model, for any of the five analysed KPIs
(dashboard/kpis.py) -- not a live process-monitoring feed; every trace shown is already
completed (presentation loosely follows Galanti et al. 2023b: a case picker plus
local/aggregate explanation views).

Two modes:
  * Perturbation -- LOO identification + Shapley quantification:
      - Single trace: explain_trace_shapley() on one trace prefix (cached on disk).
      - Aggregate: the precomputed LAST-EVENT aggregate (explain_aggregate_shapley(),
        scope='last_event') read from each KPI's own output folder. The all-prefix /
        time-normalized global Shapley analyses are deliberately not shown here.
  * Counterfactual -- explain_counterfactual(): the most similar test-set trace with a
    contrasting prediction, shown as a single-panel union graph.

Run with (from repo root): PYTHONPATH=. streamlit run dashboard/app.py
"""
import os

import pandas as pd
import streamlit as st

from dashboard import cache as dc
from dashboard import charts
from dashboard import kpis

MODE_LABELS = {
    'perturbation': 'Perturbation (LOO + Shapley)',
    'cf': 'Counterfactual',
}
CF_EXAMPLES_CSV = "thesis_parts/figures_tables/cf_union_examples/selection.csv"

st.set_page_config(page_title="Perturbation Analysis Explorer", layout="wide")


@st.cache_resource(show_spinner="Loading model and test set…")
def get_explainer(slug):
    return kpis.make_explainer(slug)


def curated_cf_example(slug):
    """The KPI's curated 4-5-event counterfactual example (thesis_scripts/
    cf_union_graph_examples.py), or None if that selection hasn't been generated."""
    if not os.path.exists(CF_EXAMPLES_CSV):
        return None
    sel = pd.read_csv(CF_EXAMPLES_CSV)
    rows = sel[sel['kpi'] == slug]
    return None if rows.empty else rows.iloc[0].to_dict()


def order_ids_for(explainer, slug):
    """The first DEMO_N last-event test traces (the ones precompute.py warms), plus this
    KPI's curated counterfactual example so it is always reachable from the picker."""
    ids = kpis.demo_order_ids(explainer)
    ex = curated_cf_example(slug)
    if ex is not None and int(ex['query_id']) not in ids:
        ids.append(int(ex['query_id']))
    return ids


def prefix_options_for(explainer, order_id):
    """All n_events prefixes recorded for this trace in the test split, ascending --
    mirrors _locate_test_graph()'s own enumeration, so every option resolves."""
    vp = explainer.kpi_viewpoint
    return sorted(
        g['Events'].x.size(0)
        for g in explainer.test_data
        if g[vp]['id'][0].item() == order_id
    )


def _trace_dir(explainer, order_id, n_events, tag):
    suffix = f"_ev{n_events}" if n_events is not None else ""
    return os.path.join(kpis.dashboard_dir(explainer), f"order_{order_id}{suffix}_{tag}")


def compute_perturbation(explainer, order_id, n_events, top_k):
    checkpoint_fingerprint = int(os.path.getmtime(explainer.model_path))
    save_dir = _trace_dir(explainer, order_id, n_events, "shapley")
    return dc.get_or_compute(
        kpis.cache_scope(explainer), dc.PERTURBATION_MODE, order_id,
        lambda: explainer.explain_trace_shapley(order_id, top_k=top_k, n_events=n_events,
                                                save_dir=save_dir),
        n_events=n_events, top_k=top_k, checkpoint_fingerprint=checkpoint_fingerprint,
    )


def compute_counterfactual(explainer, order_id, n_events, min_gap_hours, direction):
    # Not cached: the result holds live HeteroData graphs (not JSON-serializable), and
    # min_gap_hours/direction are part of the query. A forward-pass sweep over the candidate
    # pool, so fast enough to run live.
    save_dir = _trace_dir(explainer, order_id, n_events, "cf")
    results = explainer.explain_counterfactual(order_id, min_gap_hours=min_gap_hours,
                                               direction=direction, n_events=n_events,
                                               save_dir=save_dir)
    query_graph = explainer._locate_test_graph(order_id, n_events)
    return {'results': results, 'save_dir': save_dir,
            'query_predicted_hours': explainer._predict_value_for_graph(query_graph, 0) / 3600.0,
            'n_events': query_graph['Events'].x.size(0)}


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
    """Raw internal column name -> readable label, for features calculated following
    Adams et al. 2022's OCEL feature-extraction definitions (temporal, C3 activity-frequency
    counts, O1 object counts). Literal data values pass through unchanged. Display-only."""
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
    """Linear red->green for a characterization score in [0, 1], using the project's
    increase/decrease palette (same hex pair as explainer.py's bar charts)."""
    score = max(0.0, min(1.0, score))
    red = (0xd6, 0x27, 0x28)
    green = (0x2c, 0xa0, 0x2c)
    r, g, b = (round(red[i] + (green[i] - red[i]) * score) for i in range(3))
    return f"rgb({r}, {g}, {b})"


def _image(path, caption=None):
    if os.path.exists(path):
        st.image(path, caption=caption)


# ---------------------------------------------------------------------------
# Trace picker (shared by single-trace perturbation and counterfactual)
# ---------------------------------------------------------------------------

def pick_trace(explainer, slug):
    """Order + prefix pickers. Widget keys are scoped per KPI (and per order for the prefix)
    so switching KPI never carries a stale value over, and so the curated-example button can
    set them by key. Returns (order_id, n_events) with n_events=None for the complete trace,
    matching every explain_*() call's "last recorded prefix" convention (and precompute.py's
    cache keys)."""
    ids = order_ids_for(explainer, slug)
    order_key = f"order_{slug}"
    if st.session_state.get(order_key) not in ids:
        st.session_state[order_key] = ids[0]
    order_id = st.selectbox(f"{explainer.kpi_viewpoint} ID", ids, key=order_key)

    prefix_opts = prefix_options_for(explainer, order_id)
    last_prefix = prefix_opts[-1]
    prefix_key = f"prefix_{slug}_{order_id}"
    if st.session_state.get(prefix_key) not in prefix_opts:
        st.session_state[prefix_key] = last_prefix
    selected = st.select_slider(
        "Prefix (events completed)", options=prefix_opts, key=prefix_key,
        format_func=lambda n: f"{n} (complete)" if n == last_prefix else str(n),
    )
    return order_id, (None if selected == last_prefix else selected)


def _load_curated_example(slug, ex):
    """on_click callback: runs before the rerun, so widget state can still be set."""
    order_id = int(ex['query_id'])
    st.session_state[f"order_{slug}"] = order_id
    st.session_state[f"prefix_{slug}_{order_id}"] = int(ex['query_events'])
    st.session_state["cf_min_gap"] = float(ex['min_gap_h'])
    st.session_state["cf_direction"] = "Below current prediction"
    st.session_state.pop('result_ctx', None)


# ---------------------------------------------------------------------------
# Single-trace perturbation
# ---------------------------------------------------------------------------

def render_perturbation_trace(result, cached, explainer, order_id, top_k):
    st.caption("served from cache" if cached else "computed just now (now cached for next time)")

    score = result['metrics']['characterization_score']
    metric_col, events_col, score_col = st.columns(3)
    with metric_col:
        st.metric("Predicted remaining time", f"{result['predicted_hours']:.1f} h")
    with events_col:
        st.metric("Events completed", str(result['n_events']))
    with score_col:
        # No hard-coded label colour, so it stays readable in light and dark themes.
        st.markdown(
            "<div style='font-size: 0.875rem; opacity: 0.8;'>Characterization score</div>"
            f"<div style='font-size: 2.25rem; font-weight: 600; "
            f"color: {_characterization_color(score)};'>{score:.2f}</div>",
            unsafe_allow_html=True,
        )

    # Human-readable identifiers (activity names / real OCEL ids), cheap graph lookup.
    graph = explainer._locate_test_graph(order_id, result['n_events'])
    id_map = explainer._decode_all_identifiers(graph, order_id, result['n_events'])

    node_rows = result.get('node_importances') or []
    if node_rows:
        df = pd.DataFrame(
            node_rows,
            columns=['node_type', 'node_idx', 'shift_seconds', 'large_shift', 'signed_shift_seconds'],
        ).head(top_k)
        df['signed_shift_hours'] = df['signed_shift_seconds'] / 3600.0
        std_lookup = {(nt, idx): sd / 3600.0 for nt, idx, sd in result.get('node_shapley_std', [])}
        df['std_hours'] = [std_lookup.get((nt, idx), 0.0)
                           for nt, idx in zip(df['node_type'], df['node_idx'])]
        df['identifier'] = df.apply(
            lambda r: id_map.get((r['node_type'], r['node_idx']), f"{r['node_type']}[{r['node_idx']}]"),
            axis=1,
        )
        render_trace_node_bars(df, result, order_id, top_k)

        st.subheader("Top nodes")
        st.dataframe(df[['node_type', 'node_idx', 'identifier', 'signed_shift_hours', 'std_hours',
                         'large_shift']], width='stretch', hide_index=True)

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

    with st.expander("Explanation quality metrics"):
        st.json(result['metrics'])


def render_trace_node_bars(df, result, order_id, top_k):
    """Showcase-style bar chart (as loo_shapley_trace_showcase.png): the trace's top-K
    Shapley-quantified nodes, labelled "Type: identifier", with ±1 std whiskers across the
    sampler's permutations (same n for every bar, so it goes in the title)."""
    n = result.get('n_samples', 100)
    png = os.path.join(result['save_dir'], f"top_nodes_shapley_k{top_k}.png")
    charts.plot_shapley_bars(
        [f"{nt}: {ident}" for nt, ident in zip(df['node_type'], df['identifier'])],
        df['signed_shift_hours'].tolist(), df['std_hours'].tolist(), [n] * len(df), png,
        title=f"Top {top_k} nodes by Shapley value, #{order_id} "
              f"(whiskers = ±1 std across {n} sampled permutations)",
        show_n=False)
    st.subheader(f"Top {top_k} nodes by Shapley value")
    _image(png)
    st.caption("Red pushes the predicted remaining time later, blue earlier.")


def render_top_features_by_attribution(explainer, graph, id_map, result, order_id, top_k):
    """Top-K (node, feature) pairs across the whole trace by |InputXGradient attribution|,
    next to the Shapley value of the SAME features -- a feature-for-feature comparison of
    the two methods. Computed live (one backward pass + one Shapley sweep per node); the
    sweeps run batched (many coalitions per forward pass), ~seconds instead of minutes."""
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

    # Fresh baseline (never a cached one), one Shapley sweep per distinct node; None only
    # for an exactly-zero input feature (skipped by the Shapley zero-value guard).
    baseline_seconds = explainer._predict_value_for_graph(graph, 0)
    shapley_lookup = {}
    for node_type, node_idx in df[['node_type', 'node_idx']].drop_duplicates().itertuples(index=False):
        feats = explainer.shapley_feature_importance_for_node(
            graph, node_type, node_idx, baseline_seconds, target_object_idx=0, n_samples=100,
            batched=True,
        )
        shapley_lookup[(node_type, node_idx)] = {f: sv / 3600.0 for f, sv in feats.items()}
    df['shapley_value_hours'] = df.apply(
        lambda r: shapley_lookup.get((r['node_type'], r['node_idx']), {}).get(r['feature_idx']),
        axis=1,
    )

    save_dir = result['save_dir']
    labels = [f"{r['node']}: {r['feature']}" for _, r in df.iterrows()]
    attr_png = os.path.join(save_dir, "top_features_attribution.png")
    explainer.plot_top_features_bar(labels, df['signed'].tolist(), attr_png,
                                    title=f"Top {top_k} features by attribution, #{order_id}")
    shapley_png = os.path.join(save_dir, "top_features_shapley.png")
    explainer.plot_top_features_bar(labels, df['shapley_value_hours'].fillna(0.0).tolist(),
                                    shapley_png,
                                    title=f"Shapley value, same features, #{order_id}",
                                    xlabel="Shift in Hours")

    st.subheader(f"Top {top_k} features by attribution, compared against Shapley value")
    col1, col2 = st.columns(2)
    with col1:
        _image(attr_png)
        st.caption("Feature attribution (InputXGradient)")
    with col2:
        _image(shapley_png)
        st.caption("Shapley value (hours)")
    st.dataframe(df[['node_type', 'node', 'feature', 'signed', 'abs', 'shapley_value_hours']],
                 width='stretch')


# ---------------------------------------------------------------------------
# Aggregate perturbation (precomputed, last-event)
# ---------------------------------------------------------------------------

def render_perturbation_aggregate(explainer, slug, top_k):
    """Dataset-wide top-K nodes / features from the KPI's precomputed last-event aggregate
    (explain_aggregate_shapley(): exhaustive LOO identification over the first 50 last-event
    test traces, then Shapley requantification of the top ~20 winners). Read-only: the CSVs
    keep ~20 rows so the chart can be re-sliced to the current top-K live, with no model
    inference; charts are written to the dashboard folder, never the thesis-cited one."""
    src = kpis.aggregate_dir(slug)
    out = os.path.join(kpis.dashboard_dir(explainer), "aggregate")
    os.makedirs(out, exist_ok=True)

    metrics_csv = os.path.join(src, "aggregate_metrics.csv")
    if os.path.exists(metrics_csv):
        m = pd.read_csv(metrics_csv)
        m = m[pd.to_numeric(m['trace'], errors='coerce').notna()]
        cols = st.columns(3)
        cols[0].metric("Traces aggregated", f"{len(m)}")
        cols[1].metric("Mean characterization score", f"{m['characterization_score'].mean():.2f}")
        cols[2].metric("Mean fidelity+ (h)", f"{m['fidelity_plus'].mean():.1f}")
    st.caption(f"Source: `{src}` — last-event prefixes only.")

    for csv_name, what in (("aggregate_explanation_bars.csv", "nodes"),
                           ("aggregate_feature_bars.csv", "features")):
        csv_path = os.path.join(src, csv_name)
        st.subheader(f"Top {top_k} {what} by mean Shapley shift")
        if not os.path.exists(csv_path):
            st.warning(f"No precomputed aggregate at `{csv_path}`.")
            continue
        df = pd.read_csv(csv_path)
        top = df.sort_values('mean_signed_shift', key=abs, ascending=False).head(top_k)
        png = os.path.join(out, f"aggregate_{what}_top{top_k}.png")
        charts.plot_shapley_bars(
            top['label'].tolist(), top['mean_signed_shift'].tolist(), top['std_shift'].tolist(),
            top['count'].astype(int).tolist(), png,
            title=f"Top {top_k} {what} by aggregate Shapley value "
                  f"(whiskers = ±1 std across revisits; none when n=1)")
        _image(png)
        st.caption("Red pushes the predicted remaining time later, blue earlier; "
                   "n = traces revisited for the Shapley value.")
        st.dataframe(top.rename(columns={'mean_signed_shift': 'mean_signed_shift_h',
                                         'std_shift': 'std_shift_h',
                                         'count': 'traces_containing'}),
                     width='stretch', hide_index=True)

    with st.expander("Recomputing the aggregate"):
        st.caption("Not available from the dashboard: regenerating overwrites the thesis-cited "
                   "CSVs above and takes a long time. Only needed after retraining; run from "
                   "the repo root, matching the parameters originally used for this KPI "
                   "(e.g. revisit_n):")
        st.code("PYTHONPATH=. python3 -c \"from dashboard import kpis; "
                f"kpis.make_explainer('{slug}').explain_aggregate_shapley("
                f"n_traces=50, save_dir='{src}')\"", language="bash")


# ---------------------------------------------------------------------------
# Counterfactual
# ---------------------------------------------------------------------------

def render_counterfactual(result, explainer):
    st.caption("Recomputed on every click (threshold and direction are part of the query).")
    metric_col, events_col = st.columns(2)
    metric_col.metric("Predicted remaining time (query)", f"{result['query_predicted_hours']:.1f} h")
    events_col.metric("Events completed (query)", str(result['n_events']))

    results = result['results']
    if not results:
        st.warning("No counterfactual found for this trace under the current threshold and "
                   "direction. Lower the minimum gap or flip the direction.")
        return

    best = results[0]
    st.subheader(f"Best counterfactual: #{best['order_id']} — "
                 f"{best['predicted_hours']:.1f} h predicted "
                 f"({best['predicted_hours'] - result['query_predicted_hours']:+.1f} h)")
    save_dir = result['save_dir']
    _image(os.path.join(save_dir, "cf_union_graph.png"),
           "Union of query and counterfactual: red = query only, green = counterfactual only, "
           "grey = shared")

    st.subheader(f"Top {len(results)} candidate(s)")
    st.dataframe(pd.DataFrame([
        {'rank': i, 'trace_id': r['order_id'], 'predicted_hours': r['predicted_hours'],
         'dissimilarity': r['dissimilarity'], 'n_events': r['n_events'],
         'feat': r['components']['feat'], 'type': r['components']['type'],
         'edge': r['components']['edge'], 'struct': r['components']['struct']}
        for i, r in enumerate(results, 1)
    ]), width='stretch', hide_index=True)

    with st.expander("More comparisons (query vs. best counterfactual)"):
        _image(os.path.join(save_dir, "cf_diff_graph.png"), "Side-by-side highlighted differences")
        _image(os.path.join(save_dir, "cf_graph_structure_comparison.png"), "Graph structures")
        col1, col2 = st.columns(2)
        with col1:
            _image(os.path.join(save_dir, "cf_node_type_comparison.png"), "Node-type counts")
            _image(os.path.join(save_dir, "cf_event_type_diff.png"), "Event-type differences")
        with col2:
            _image(os.path.join(save_dir, "cf_dissimilarity_breakdown.png"), "Dissimilarity breakdown")
            vp = explainer.kpi_viewpoint
            _image(os.path.join(save_dir, f"cf_{vp.lower()}_feature_diff.png"),
                   f"{vp} feature differences")


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

def render_trace_flow(explainer, slug, mode, top_k, min_gap_hours, direction):
    order_id, n_events = pick_trace(explainer, slug)
    ctx = (slug, mode, order_id, n_events, top_k, min_gap_hours, direction)

    if st.button("Explain this trace", type="primary"):
        try:
            with st.spinner("Computing…"):
                if mode == 'perturbation':
                    payload = compute_perturbation(explainer, order_id, n_events, top_k)
                else:
                    payload = (compute_counterfactual(explainer, order_id, n_events,
                                                      min_gap_hours, direction), False)
            # Persisted, not rendered inline: survives reruns from other widgets; any
            # control change alters ctx and naturally retires the stale render.
            st.session_state['result_ctx'] = ctx
            st.session_state['result'] = payload
        except ValueError as ex:
            st.error(f"This trace isn't explainable in {MODE_LABELS[mode]} mode: {ex}")

    if st.session_state.get('result_ctx') == ctx:
        result, cached = st.session_state['result']
        if mode == 'perturbation':
            render_perturbation_trace(result, cached, explainer, order_id, top_k)
        else:
            render_counterfactual(result, explainer)


def main():
    st.title("Perturbation Analysis Explorer")
    st.caption("Exploring the held-out **test set** of a trained HGT model: static, already "
               "completed traces, not a live process feed.")

    with st.sidebar:
        slug = st.selectbox("KPI", list(kpis.KPIS), format_func=lambda s: kpis.KPIS[s]['label'])
        mode = st.radio("Explanation mode", list(MODE_LABELS), format_func=MODE_LABELS.get)
        top_k, min_gap_hours, direction = 5, 0.0, 'lower'
        if mode == 'perturbation':
            top_k = st.selectbox("Top K", [3, 5, 10, 15, 20], index=1,
                                 help="Number of nodes, edges and features shown.")
            st.caption("Leave-one-out identifies candidate nodes, edges and features; Shapley "
                       "sampling quantifies their effect on the predicted remaining time.")
        else:
            ex = curated_cf_example(slug)
            if ex is not None:
                st.button("Load curated example", on_click=_load_curated_example,
                          args=(slug, ex),
                          help=f"#{int(ex['query_id'])} at {int(ex['query_events'])} events, "
                               f"minimum gap {ex['min_gap_h']:g} h (the thesis union-graph "
                               f"example for this KPI).")
            direction_label = st.radio("Search direction",
                                       ["Below current prediction", "Above current prediction"],
                                       key="cf_direction")
            direction = 'lower' if direction_label.startswith("Below") else 'higher'
            min_gap_hours = st.number_input(
                "Minimum predicted-hours gap", min_value=0.0, step=1.0, key="cf_min_gap",
                help="Minimum |query − candidate| predicted-hours gap a counterfactual must have.")
            st.caption("Retrieves the most similar test-set trace with a contrasting predicted "
                       "outcome. Not a perturbation method; it complements the other mode.")

    explainer = get_explainer(slug)

    if mode == 'perturbation':
        tab_trace, tab_aggregate = st.tabs(["Single trace", "Aggregate"])
        with tab_trace:
            render_trace_flow(explainer, slug, mode, top_k, min_gap_hours, direction)
        with tab_aggregate:
            render_perturbation_aggregate(explainer, slug, top_k)
    else:
        render_trace_flow(explainer, slug, mode, top_k, min_gap_hours, direction)


if __name__ == '__main__':
    main()
