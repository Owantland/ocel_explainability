"""The five analysed KPIs the dashboard can explore, each pinned to its own graph cache.

Every KPI on the same database shares config.yml's single kpi_event and graph-cache folder,
so thesis scripts swap caches in and out by hand. The dashboard instead loads each KPI
through Explainer's kpi_event/graph_dir overrides, pointing at the per-KPI *_backup copies
where those are current. PayOrder is the exception (its backup is stale): it reads the live
folder, guarded by not_same_as so a swapped-in PackageDelivered cache fails loudly instead
of being explained under the wrong KPI.

Slugs match thesis_scripts/cf_union_graph_examples.py's KPIS.
"""
import filecmp
import os

import torch

import explainer as exp

HETERO = "files/hetero_structures"
OUTPUTS = "files/explainer_outputs"

KPIS = {
    'om_payorder': dict(
        label="Order management — Orders → PayOrder",
        database='order_management', cant=2000, kpi_event='PayOrder',
        # The live folder, not _payorder_graphs_backup: the backup (Jul 4) predates the
        # Oct 13 cache rebuild (generator fixes in 6f86dc6) that every Oct thesis output used,
        # and gives slightly different predictions (~0.03-0.07 h). Guarded against a
        # PackageDelivered cache having been swapped in.
        graph_dir=f"{HETERO}/order_management/2000",
        not_same_as=f"{HETERO}/order_management/_packagedelivered_graphs_backup",
        aggregate_dir="aggregate_shapley_TimeFrom_Orders_to_PayOrder"),
    'om_packagedelivered': dict(
        label="Order management — Orders → PackageDelivered",
        database='order_management', cant=2000, kpi_event='PackageDelivered',
        graph_dir=f"{HETERO}/order_management/_packagedelivered_graphs_backup",
        aggregate_dir="aggregate_shapley_TimeFrom_Orders_to_PackageDelivered"),
    'logistics_depart': dict(
        label="Logistics — CustomerOrder → Depart",
        database='logistics', cant=1000, kpi_event='Depart',
        graph_dir=f"{HETERO}/logistics/1000_customerorder_depart_backup",
        aggregate_dir="aggregate_shapley_TimeFrom_CustomerOrder_to_Depart"),
    'logistics_loadtovehicle': dict(
        label="Logistics — CustomerOrder → LoadToVehicle",
        database='logistics', cant=1000, kpi_event='LoadToVehicle',
        graph_dir=f"{HETERO}/logistics/1000_loadtovehicle_backup",
        aggregate_dir="aggregate_shapley_TimeFrom_CustomerOrder_to_LoadToVehicle"),
    'iot_evaluatetruckexit': dict(
        label="CargoPickup IoT — Pickup Plan → EvaluateTruckExit",
        database='cargo_pickup_iot', cant=2000, kpi_event='EvaluateTruckExit',
        # Only KPI on this database, so its live folder is never swapped out.
        graph_dir=f"{HETERO}/cargo_pickup_iot/2000",
        aggregate_dir="aggregate_last_event_TimeFrom_Pickupplan_to_EvaluateTruckExit_revisit8"),
}

DEMO_N = 5  # first N last-event test traces per KPI (same ordering explain_aggregate() uses)


def make_explainer(slug):
    """Explainer for this KPI with its trained checkpoint already loaded. Explainer() itself
    builds an untrained model (each explain_* method loads the weights on entry), so loading
    here keeps direct helpers like _predict_value_for_graph() meaningful from the start."""
    k = KPIS[slug]
    if k.get('not_same_as'):
        test_file = "test_graphs_sg.pt"
        if filecmp.cmp(os.path.join(k['graph_dir'], test_file),
                       os.path.join(k['not_same_as'], test_file), shallow=False):
            raise RuntimeError(f"{k['graph_dir']} currently holds the cache from "
                               f"{k['not_same_as']} (another KPI was swapped in); swap the "
                               f"{slug} cache back before exploring it.")
    e = exp.Explainer(k['database'], k['cant'], kpi_event=k['kpi_event'],
                      graph_dir=k['graph_dir'])
    e.model.load_state_dict(torch.load(e.model_path, weights_only=False))
    e.model.eval()
    return e


def aggregate_dir(slug):
    k = KPIS[slug]
    return os.path.join(OUTPUTS, k['database'], k['aggregate_dir'])


def dashboard_dir(explainer):
    """Dashboard-only output root, namespaced by task_id so two KPIs on the same database
    never overwrite each other's per-trace PNGs, and kept apart from thesis-cited outputs."""
    return os.path.join(explainer.path_dict['explainer_path'], "dashboard", explainer.task_id)


def cache_scope(explainer):
    return f"{explainer.database}_{explainer.cant}_{explainer.task_id}"


def demo_order_ids(explainer, n=DEMO_N):
    vp = explainer.kpi_viewpoint
    last_event_graphs = [g for g in explainer.test_data if g[vp]['last_event'][0].item()]
    return [int(g[vp]['id'][0].item()) for g in last_event_graphs[:n]]
