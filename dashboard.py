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
import explainer as exp

DATASETS = {
    'order_management': 2000,
    'logistics': 1000,
}
MODE_LABELS = {
    'loo': 'Exhaustive LOO',
    'gnn_primary': 'GNNExplainer-primary',
}

st.set_page_config(page_title="Perturbation Analysis Explorer", layout="wide")


@st.cache_resource
def get_explainer(database, cant):
    return exp.Explainer(database, cant)


def order_ids_for(explainer):
    vp = explainer.kpi_viewpoint
    return sorted(
        int(g[vp]['id'][0].item())
        for g in explainer.test_data
        if g[vp]['last_event'][0].item()
    )


def compute_local(explainer, database, cant, mode, order_id):
    if mode == 'loo':
        compute_fn = lambda: explainer.explain_trace(order_id)
    else:
        compute_fn = lambda: explainer.explain_gnn_primary(order_id)
    return dc.get_or_compute(database, cant, mode, order_id, compute_fn)


def render_local(result, cached, mode):
    st.caption("served from cache" if cached else "computed just now (now cached for next time)")
    st.metric("Predicted remaining time", f"{result['predicted_hours']:.1f} h")

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

    node_rows = result.get('node_importances') or []
    if node_rows:
        st.subheader("Top nodes")
        df = pd.DataFrame(
            node_rows,
            columns=['node_type', 'node_idx', 'shift_seconds', 'large_shift', 'signed_shift_seconds'],
        )
        df['shift_hours'] = df['shift_seconds'] / 3600.0
        df['signed_shift_hours'] = df['signed_shift_seconds'] / 3600.0
        df = df[['node_type', 'node_idx', 'signed_shift_hours', 'large_shift']]
        st.dataframe(df.head(10), width='stretch')

    if mode == 'loo':
        st.subheader("Explanation quality (exhaustive sweep)")
        st.json(result['metrics'])
        edge_rows = result.get('edge_importances') or []
        if edge_rows:
            st.subheader("Top edges")
            edf = pd.DataFrame(
                edge_rows,
                columns=['edge_type', 'edge_idx', 'shift_seconds', 'large_shift', 'signed_shift_seconds'],
            )
            edf['signed_shift_hours'] = edf['signed_shift_seconds'] / 3600.0
            st.dataframe(edf[['edge_type', 'signed_shift_hours', 'large_shift']].head(10),
                        width='stretch')
    else:
        st.subheader("Joint impact of masking the identified nodes together")
        st.json(result['quality'])
        st.caption("Edge importance isn't available in this mode -- GNNExplainer has no edge "
                  "signal on this architecture. Switch to Exhaustive LOO for edge importance.")


def render_global(database, mode):
    base = os.path.join("files", "explainer_outputs", database)
    if mode == 'loo':
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


def render_regenerate(explainer, database, cant, mode):
    st.caption("Recomputes the aggregate from scratch and overwrites the CSV/PNG shown above -- "
              "only needed after retraining the underlying model. Exhaustive LOO is slow "
              "(O(n_traces x nodes+edges) forward passes); GNNExplainer-primary is much slower "
              "(n_traces x 200-epoch mask optimizations).")
    if st.button(f"Regenerate {MODE_LABELS[mode]} aggregate (n_traces=50)", key=f"regen_{mode}"):
        with st.spinner("Running aggregate explanation -- this can take several minutes..."):
            if mode == 'loo':
                explainer.explain_aggregate(n_traces=50)
            else:
                explainer.explain_gnn_primary_aggregate(n_traces=50)
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
        if mode == 'gnn_primary':
            st.caption("GNNExplainer identifies important nodes; LOO estimates their impact. "
                      "Node-only -- no edge importance.")
        else:
            st.caption("Exhaustive sweep over every node, edge, and feature. Slower, but the "
                      "only source of edge importance.")

    explainer = get_explainer(database, cant)

    tab_local, tab_global = st.tabs(["Local (single case)", "Global (aggregate)"])

    with tab_local:
        ids = order_ids_for(explainer)
        order_id = st.selectbox("Order ID", ids, key="order_picker")
        if st.button("Explain this order"):
            spinner_msg = ("Computing (GNNExplainer-primary can take ~1 minute on a cold cache)..."
                          if mode == 'gnn_primary' else "Computing...")
            try:
                with st.spinner(spinner_msg):
                    result, cached = compute_local(explainer, database, cant, mode, order_id)
                render_local(result, cached, mode)
            except ValueError as ex:
                # explain_gnn_primary()'s guard against a legitimately-empty edge
                # type (a real PyG/GNNExplainer limitation, not every order supports
                # this mode) -- show a clear message instead of a raw traceback.
                st.error(f"This order isn't explainable in {MODE_LABELS[mode]} mode: {ex}")

    with tab_global:
        render_global(database, mode)
        with st.expander("Regenerate (advanced)"):
            render_regenerate(explainer, database, cant, mode)


if __name__ == '__main__':
    main()
