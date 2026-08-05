import torch
import torch.nn.functional as F
import matplotlib.pyplot as plt
import os
from training import Modelling
from torch_geometric.explain import Explainer as PyGExplainer, CaptumExplainer, GNNExplainer


class Explainer(Modelling):
    def __init__(self, database, cant):
        super().__init__(database, cant)
        # Found via direct investigation: identical inputs (bit-identical node features
        # and edges, confirmed directly) + identical, fully state_dict-matched weights
        # still produced different raw model outputs across separate Explainer()
        # instantiations, while repeated calls WITHIN one instantiation were stable --
        # no dropout or non-persistent buffers exist to explain this architecturally.
        # Consistent with known floating-point non-determinism in PyTorch/PyG's
        # parallel scatter/reduce ops (used heavily in HGTConv's attention
        # aggregation) when not explicitly disabled. Scoped to Explainer, not
        # Modelling.__init__, so training/sweep() isn't slowed by deterministic algos --
        # reproducibility matters far more at explanation time than during a
        # many-hour hyperparameter sweep.
        torch.manual_seed(42)
        os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')  # required for CUDA determinism, harmless on CPU
        torch.use_deterministic_algorithms(True, warn_only=True)

    # ------------------------------------------------------------------
    # Regression explanations
    # ------------------------------------------------------------------

    @torch.no_grad()
    def _predict_value_for_graph(self, graph, object_idx, perturbed_graph=None):
        """Run the model on one small graph and return the de-normalized prediction."""
        g = perturbed_graph if perturbed_graph is not None else graph
        out = self.model(g.x_dict, g.edge_index_dict)
        denorm = out * self.target_std + self.target_mean
        return denorm[object_idx].item()

    def reg_feature_importance_for_node_in_graph(self, graph, node_type, node_idx, baseline_value,
                                                 target_object_idx, top_k=10):
        """Leave-one-out at the FEATURE level for regression."""
        x = graph[node_type].x[node_idx]
        num_features = x.size(0)
        feature_importances = []
        for f in range(num_features):
            if x[f].item() == 0.0:
                continue
            perturbed = graph.clone()
            perturbed[node_type].x[node_idx, f] = 0.0
            pred = self._predict_value_for_graph(graph, target_object_idx, perturbed_graph=perturbed)
            signed_shift = baseline_value - pred
            shift = abs(signed_shift)
            large_shift = shift > self.target_std.item()
            feature_importances.append((f, shift, large_shift, signed_shift))
        feature_importances.sort(key=lambda t: t[1], reverse=True)
        return feature_importances[:top_k]

    def reg_explanation_subgraph(self, graph, object_idx, node_importances,
                                 edge_importances, node_top_k=10):
        """Build a NetworkX subgraph from LOO regression importance scores."""
        import networkx as nx

        seed_key = (self.kpi_viewpoint, object_idx)
        G = nx.MultiDiGraph()
        G.add_node(seed_key, node_type=self.kpi_viewpoint, importance=1.0,
                   signed_importance=0.0, is_seed=True, large_shift=False, is_connector=False)

        included = {seed_key}
        for nt, i, shift, large, signed_shift in node_importances[:node_top_k]:
            key = (nt, i)
            G.add_node(key, node_type=nt, importance=shift / 3600.0,
                       signed_importance=signed_shift / 3600.0, is_seed=False,
                       large_shift=large, is_connector=False)
            included.add(key)

        all_edges = []
        for edge_type in graph.edge_types:
            src_type, _, dst_type = edge_type
            edge_index = graph[edge_type].edge_index
            for e in range(edge_index.size(1)):
                src, dst = edge_index[:, e].tolist()
                all_edges.append((edge_type, (src_type, src), (dst_type, dst)))

        edge_importance_lookup = {}
        for edge_type, e, shift, large, signed_shift in edge_importances:
            edge_index = graph[edge_type].edge_index
            src, dst = edge_index[:, e].tolist()
            edge_importance_lookup[(edge_type, src, dst)] = (shift, large, signed_shift)

        def add_real_edge(edge_type, src_key, dst_key):
            shift, large, signed_shift = edge_importance_lookup.get(
                (edge_type, src_key[1], dst_key[1]), (0.0, False, 0.0)
            )
            G.add_edge(src_key, dst_key, edge_type=edge_type[1], importance=shift / 3600.0,
                       signed_importance=signed_shift / 3600.0, large_shift=large)

        # Pass 1: induced subgraph on the selected nodes
        for edge_type, src_key, dst_key in all_edges:
            if src_key in included and dst_key in included:
                add_real_edge(edge_type, src_key, dst_key)

        # Pass 2: repair isolated nodes by pulling in shortest real path to seed
        full_nx = nx.Graph()
        for edge_type, src_key, dst_key in all_edges:
            full_nx.add_edge(src_key, dst_key, edge_type=edge_type)

        isolated = [n for n in included if G.degree(n) == 0]
        for node_key in isolated:
            try:
                path = nx.shortest_path(full_nx, source=node_key, target=seed_key)
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                continue

            for n in path:
                if n not in G.nodes:
                    G.add_node(n, node_type=n[0], importance=0.0, signed_importance=0.0,
                               is_seed=False, large_shift=False, is_connector=True)

            for a, b in zip(path[:-1], path[1:]):
                if G.has_edge(a, b) or G.has_edge(b, a):
                    continue
                edge_type = full_nx[a][b]["edge_type"]
                shift, large, signed_shift = edge_importance_lookup.get(
                    (edge_type, a[1], b[1]),
                    edge_importance_lookup.get((edge_type, b[1], a[1]), (0.0, False, 0.0)),
                )
                G.add_edge(a, b, edge_type=edge_type[1], importance=shift / 3600.0,
                           signed_importance=signed_shift / 3600.0, large_shift=large)

        return G

    def reg_visualize_explanation_subgraph(self, G, save_path="explanation_subgraph_regression.png", id_map=None):
        """Draw regression explanation subgraph -- fixed-size/plain-edge style matching
        _draw_hetero_nx's counterfactual-comparison plots (no importance-based sizing
        or coloring); nodes colored by type, seed node distinguished by size/border."""
        import matplotlib.pyplot as plt
        import networkx as nx

        palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
                   "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"]
        node_types = sorted({attrs["node_type"] for _, attrs in G.nodes(data=True)})
        type_colors = {nt: palette[i % len(palette)] for i, nt in enumerate(node_types)}

        try:
            pos = nx.kamada_kawai_layout(G)
        except Exception as exc:
            print(f"  [layout] kamada_kawai_layout failed ({type(exc).__name__}: {exc}), "
                  f"falling back to spring_layout")
            pos = nx.spring_layout(G, seed=42, k=0.9)

        # Fixed, non-importance-based visual encoding -- matches _draw_hetero_nx's style
        # (the plot used for counterfactual graph comparisons). Node fill color is the
        # only channel that still varies (by node_type, via type_colors above); size and
        # border are seed-vs-everything-else only. Connector nodes (path-filler pulled in
        # only to keep the pruned subgraph connected to the seed, not because they were
        # ranked important) keep the alpha fade below -- that's a structural/topological
        # distinction, not an importance encoding, so it's kept.
        node_colors, node_sizes, edge_colors_outline, node_linewidths, alphas = [], [], [], [], []
        for node, attrs in G.nodes(data=True):
            node_colors.append(type_colors.get(attrs["node_type"], "gray"))
            node_sizes.append(420 if attrs.get("is_seed") else 180)
            edge_colors_outline.append("black" if attrs.get("is_seed") else "none")
            node_linewidths.append(1.8 if attrs.get("is_seed") else 0)
            alphas.append(0.4 if attrs.get("is_connector") else 0.9)

        plt.figure(figsize=(10, 8))
        nx.draw_networkx_nodes(
            G, pos, node_color=node_colors, node_size=node_sizes,
            edgecolors=edge_colors_outline, linewidths=node_linewidths, alpha=alphas,
        )
        nx.draw_networkx_edges(
            G, pos, edge_color="gray", width=1.0,
            arrows=True, connectionstyle="arc3,rad=0.1", alpha=0.5,
        )
        labels = {
            node: (str(id_map[node]) if id_map and node in id_map
                   else f"{node[0]}[{node[1]}]")
            for node in G.nodes if not G.nodes[node].get("is_connector")
        }
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=7)

        legend_handles = [
            plt.Line2D([0], [0], marker="o", color="w", label=nt,
                       markerfacecolor=color, markersize=10)
            for nt, color in type_colors.items()
        ]
        legend_handles.append(plt.Line2D([0], [0], marker="o", color="w", label="connector (faded)",
                                         markerfacecolor="gray", alpha=0.4, markersize=8))
        plt.legend(handles=legend_handles, loc="best", fontsize=8)

        plt.title("LOO Explanation Subgraph (Regression)")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"Saved explanation subgraph visualization to {save_path}")

    def evaluate_explanation_quality(self, graph, object_idx, node_importances, edge_importances,
                                     node_top_k=10, edge_top_k=15, verbose=True):
        """Compute fidelity+/-, characterization, and sparsity for regression explanations."""
        baseline_value = self._predict_value_for_graph(graph, object_idx)

        explanation_nodes_by_type = {}
        for nt, i, _shift, _large, _signed in node_importances[:node_top_k]:
            explanation_nodes_by_type.setdefault(nt, set()).add(i)
        explanation_nodes_by_type.setdefault(self.kpi_viewpoint, set()).add(object_idx)

        explanation_edges_by_type = {}
        for et, e, _shift, _large, _signed in edge_importances[:edge_top_k]:
            explanation_edges_by_type.setdefault(et, set()).add(e)

        # Fidelity+: remove the explanation, keep everything else
        complement = graph.clone()
        for nt, idx_set in explanation_nodes_by_type.items():
            for i in idx_set:
                complement[nt].x[i] = 0.0
        for et, pos_set in explanation_edges_by_type.items():
            edge_index = complement[et].edge_index
            num_edges = edge_index.size(1)
            keep = torch.tensor(
                [pos not in pos_set for pos in range(num_edges)],
                dtype=torch.bool, device=self.device,
            )
            complement[et].edge_index = edge_index[:, keep]
        pred_complement = self._predict_value_for_graph(graph, object_idx, perturbed_graph=complement)
        fidelity_plus = abs(baseline_value - pred_complement) / 3600.0  # hours, matching every other metric

        # Fidelity-: keep ONLY the explanation
        subgraph = graph.clone()
        for nt in subgraph.node_types:
            keep_idx = explanation_nodes_by_type.get(nt, set())
            n = subgraph[nt].x.size(0)
            for i in range(n):
                if i not in keep_idx:
                    subgraph[nt].x[i] = 0.0
        for et in subgraph.edge_types:
            edge_index = subgraph[et].edge_index
            num_edges = edge_index.size(1)
            keep_pos = explanation_edges_by_type.get(et, set())
            keep = torch.tensor(
                [pos in keep_pos for pos in range(num_edges)],
                dtype=torch.bool, device=self.device,
            )
            subgraph[et].edge_index = edge_index[:, keep]
        pred_subgraph = self._predict_value_for_graph(graph, object_idx, perturbed_graph=subgraph)
        fidelity_minus = abs(baseline_value - pred_subgraph) / 3600.0  # hours, matching every other metric

        denom = fidelity_plus + fidelity_minus
        characterization_score = fidelity_plus / denom if denom > 1e-8 else 0.0

        total_nodes = sum(graph[nt].x.size(0) for nt in graph.node_types)
        explanation_node_count = sum(len(s) for s in explanation_nodes_by_type.values())
        node_sparsity = 1 - (explanation_node_count / total_nodes)

        total_edges = sum(graph[et].edge_index.size(1) for et in graph.edge_types)
        explanation_edge_count = sum(len(s) for s in explanation_edges_by_type.values())
        edge_sparsity = 1 - (explanation_edge_count / total_edges) if total_edges > 0 else float("nan")

        metrics = {
            "fidelity_plus": fidelity_plus,
            "fidelity_minus": fidelity_minus,
            "characterization_score": characterization_score,
            "node_sparsity": node_sparsity,
            "edge_sparsity": edge_sparsity,
        }

        if verbose:
            print("\n--- Explanation subgraph quality metrics ---")
            print(f"  Fidelity+        : {fidelity_plus:.4f}h  "
                  f"(higher is better -- removing the explanation should shift the prediction)")
            print(f"  Fidelity-        : {fidelity_minus:.4f}h  "
                  f"(closer to 0 is better -- the explanation alone should reproduce the prediction)")
            print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
            print(f"  Node sparsity    : {node_sparsity:.2%}  (share of the graph excluded from the explanation)")
            print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of edges excluded from the explanation)")

        return metrics

    def top_nodes_per_type(self, node_importances, top_n=3):
        """Group LOO node_importances by node_type, keeping each type's top_n
        highest-shift instances. Assumes node_importances is already sorted
        descending by shift (as reg_explanation() returns it) -- the viewpoint
        object type will be absent here since its only instance is the seed
        node, which reg_explanation() excludes from node_importances entirely."""
        grouped = {}
        for nt, idx, shift, large, signed_shift in node_importances:
            grouped.setdefault(nt, [])
            if len(grouped[nt]) < top_n:
                grouped[nt].append((idx, shift, large, signed_shift))
        return grouped

    def _loo_shift_for_nodes(self, explain_subgraph, object_idx, baseline_value, node_keys):
        """Zero each (node_type, idx) in node_keys' feature vector in turn and record
        the resulting signed/absolute prediction shift, one at a time -- the same
        single-node perturbation reg_explanation() applies exhaustively to every node
        in the graph, factored out so it can also be called on an arbitrary caller-
        supplied subset (see explain_gnn_primary(), which calls this only on the
        node instances GNNExplainer identified, not the whole graph)."""
        node_importances = []
        for node_type, idx in node_keys:
            perturbed = explain_subgraph.clone()
            perturbed[node_type].x[idx] = 0.0
            pred = self._predict_value_for_graph(explain_subgraph, object_idx, perturbed_graph=perturbed)
            signed_shift = baseline_value - pred
            shift = abs(signed_shift)
            large_shift = shift > self.target_std.item()
            node_importances.append((node_type, idx, shift, large_shift, signed_shift))
        return node_importances

    def reg_explanation(self, explain_subgraph, object_idx, graph_id, top_k):
        baseline_value = self._predict_value_for_graph(explain_subgraph, object_idx)

        all_node_keys = [
            (node_type, idx)
            for node_type in explain_subgraph.node_types
            for idx in range(explain_subgraph[node_type].x.size(0))
            if not (node_type == self.kpi_viewpoint and idx == object_idx)
        ]
        node_importances = self._loo_shift_for_nodes(explain_subgraph, object_idx, baseline_value, all_node_keys)

        edge_importances = []
        for edge_type in explain_subgraph.edge_types:
            edge_index = explain_subgraph[edge_type].edge_index
            num_edges = edge_index.size(1)
            for e in range(num_edges):
                perturbed = explain_subgraph.clone()
                keep = torch.ones(num_edges, dtype=torch.bool, device=self.device)
                keep[e] = False
                perturbed[edge_type].edge_index = edge_index[:, keep]
                pred = self._predict_value_for_graph(explain_subgraph, object_idx, perturbed_graph=perturbed)
                signed_shift = baseline_value - pred
                shift = abs(signed_shift)
                large_shift = shift > self.target_std.item()
                edge_importances.append((edge_type, e, shift, large_shift, signed_shift))

        node_importances.sort(key=lambda t: t[2], reverse=True)
        edge_importances.sort(key=lambda t: t[2], reverse=True)

        seed_feature_importances = self.reg_feature_importance_for_node_in_graph(
            explain_subgraph, self.kpi_viewpoint, object_idx, baseline_value, object_idx, top_k=top_k
        )

        if node_importances:
            top_node_type, top_node_idx, _, _, _ = node_importances[0]
            top_node_feature_importances = self.reg_feature_importance_for_node_in_graph(
                explain_subgraph, top_node_type, top_node_idx, baseline_value, object_idx, top_k=top_k
            )
        else:
            top_node_type, top_node_idx = None, None
            top_node_feature_importances = []

        return (node_importances, edge_importances, seed_feature_importances,
                top_node_feature_importances, baseline_value)

    # ------------------------------------------------------------------
    # Shapley-based explanations (perturbation, permutation-sampling)
    # ------------------------------------------------------------------
    # LOO's own shift, delta(v_j) = y_hat - Phi(p, X^{-v_j}), is mathematically one
    # specific term of the Shapley sum -- the marginal contribution of v_j in the
    # coalition "every other candidate element present, v_j alone removed." The
    # methods below generalize this to the FULL Shapley value: the average marginal
    # contribution of v_j across every possible coalition ordering of the candidate
    # set, not just the single "everything else present" ordering LOO checks. This
    # is why coalitions here vary only over the caller-supplied candidate set (e.g.
    # LOO's own top-K), with the rest of the graph always left at its real value --
    # v(FullSet) is then exactly the real baseline_value, keeping the two methods
    # directly comparable on the same elements. See compare_loo_vs_shapley().

    def _permutation_shapley(self, elements, value_fn, n_samples=100):
        """General permutation-sampling Shapley estimator: average, over n_samples
        random orderings of `elements`, each element's marginal contribution
        value_fn(prefix + [e]) - value_fn(prefix) at its position in that ordering.
        v(empty set) is cached once (same regardless of ordering). Cost is exactly
        n_samples * len(elements) calls to value_fn -- one incremental evaluation
        per permutation step, not a re-evaluation from scratch per coalition."""
        import random

        v_empty = value_fn(frozenset())
        totals = {e: 0.0 for e in elements}
        for _ in range(n_samples):
            perm = list(elements)
            random.shuffle(perm)
            prefix, prev_value = [], v_empty
            for e in perm:
                prefix.append(e)
                new_value = value_fn(frozenset(prefix))
                totals[e] += new_value - prev_value
                prev_value = new_value
        return {e: totals[e] / n_samples for e in elements}

    def shapley_node_importance(self, graph, object_idx, baseline_value, node_keys, n_samples=100):
        """Node-level Shapley values for a caller-supplied list of (node_type, idx)
        keys -- same signature shape as _loo_shift_for_nodes() for direct
        comparability. Coalitions vary only over node_keys: nodes in node_keys but
        not in the current coalition are zeroed, nodes in the coalition and every
        node OUTSIDE node_keys entirely keep their real features. Returns
        {(node_type, idx): shapley_value}, same hours-denominated units as LOO."""
        def value_fn(coalition):
            # Returns the raw prediction, not baseline-minus-prediction: the Shapley
            # marginal contribution value_fn(S+e) - value_fn(S) already reduces to
            # LOO's own y_hat - Phi(removed) sign convention for the coalition
            # S=FullSet\{e} case (baseline_value cancels out of the difference) --
            # pre-subtracting baseline_value here would double-negate that.
            perturbed = graph.clone()
            for node_type, idx in node_keys:
                if (node_type, idx) not in coalition:
                    perturbed[node_type].x[idx] = 0.0
            return self._predict_value_for_graph(graph, object_idx, perturbed_graph=perturbed)

        return self._permutation_shapley(list(node_keys), value_fn, n_samples=n_samples)

    def shapley_feature_importance_for_node(self, graph, node_type, node_idx, baseline_value,
                                            target_object_idx, feature_indices=None, n_samples=100):
        """Feature-level Shapley values for one node's features -- the analogue of
        reg_feature_importance_for_node_in_graph(). Coalitions vary over that node's
        feature dimensions (feature_indices defaults to every nonzero feature,
        matching LOO's own skip-zero-features optimization); the rest of the graph,
        including this node's OTHER dimensions outside the coalition being zeroed,
        is otherwise real. Returns {feature_idx: shapley_value}."""
        x = graph[node_type].x[node_idx]
        if feature_indices is None:
            feature_indices = [f for f in range(x.size(0)) if x[f].item() != 0.0]

        def value_fn(coalition):
            # See shapley_node_importance()'s value_fn -- same reasoning: return the
            # raw prediction, not baseline-minus-prediction.
            perturbed = graph.clone()
            for f in feature_indices:
                if f not in coalition:
                    perturbed[node_type].x[node_idx, f] = 0.0
            return self._predict_value_for_graph(graph, target_object_idx, perturbed_graph=perturbed)

        return self._permutation_shapley(feature_indices, value_fn, n_samples=n_samples)

    def shapley_edge_importance(self, graph, object_idx, baseline_value, edge_keys, n_samples=100):
        """Edge-level Shapley values for a caller-supplied list of (edge_type, edge_idx)
        keys -- same reasoning as shapley_node_importance(), but masking edges via the
        same boolean-mask pattern reg_explanation()'s own edge sweep already uses
        (explainer.py:322-334) instead of zeroing node features. Returns
        {(edge_type, edge_idx): shapley_value}."""
        from collections import defaultdict

        # Group candidate edge indices by edge_type so each edge_type's edge_index is
        # masked once per coalition evaluation, not once per candidate edge.
        by_type = defaultdict(list)
        for edge_type, e in edge_keys:
            by_type[edge_type].append(e)

        def value_fn(coalition):
            perturbed = graph.clone()
            for edge_type, candidate_es in by_type.items():
                edge_index = graph[edge_type].edge_index
                num_edges = edge_index.size(1)
                keep = torch.ones(num_edges, dtype=torch.bool, device=self.device)
                for e in candidate_es:
                    if (edge_type, e) not in coalition:
                        keep[e] = False
                perturbed[edge_type].edge_index = edge_index[:, keep]
            return self._predict_value_for_graph(graph, object_idx, perturbed_graph=perturbed)

        return self._permutation_shapley(list(edge_keys), value_fn, n_samples=n_samples)

    def compare_loo_vs_shapley(self, order_id, top_k=10, n_samples=100, n_events=None, save_dir=None):
        """Runs LOO and Shapley on the SAME candidate set from one trace and reports
        them side by side -- the concrete, empirical answer to how the two methods
        differ, not just the theoretical one. Candidate set is LOO's own top_k nodes
        (by |shift|), so both methods explain literally the same elements."""
        import pandas as pd

        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "shapley")
        os.makedirs(save_dir, exist_ok=True)

        graph = self._locate_test_graph(order_id, n_events)
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        node_imp, _, _, _, baseline_value = self.reg_explanation(graph, 0, None, top_k)
        candidates = [(nt, idx) for nt, idx, _, _, _ in node_imp[:top_k]]
        loo_signed = {(nt, idx): signed for nt, idx, _, _, signed in node_imp[:top_k]}

        shapley_values = self.shapley_node_importance(
            graph, 0, baseline_value, candidates, n_samples=n_samples
        )

        id_map = self._decode_all_identifiers(graph, order_id)
        rows = []
        for nt, idx in candidates:
            decoded = id_map.get((nt, idx))
            label = f"{nt}={decoded}" if decoded else f"{nt}[{idx}]"
            rows.append({
                'node_label': label,
                'loo_signed_shift': loo_signed[(nt, idx)] / 3600.0,
                'shapley_value': shapley_values[(nt, idx)] / 3600.0,
            })
        df = pd.DataFrame(rows)
        df['loo_rank'] = df['loo_signed_shift'].abs().rank(ascending=False, method='min').astype(int)
        df['shapley_rank'] = df['shapley_value'].abs().rank(ascending=False, method='min').astype(int)

        try:
            from scipy.stats import spearmanr
            rho, _ = spearmanr(df['loo_rank'], df['shapley_rank'])
        except ImportError:
            rho = df['loo_rank'].corr(df['shapley_rank'], method='spearman')
        print(f"LOO vs. Shapley rank agreement (Spearman's rho): {rho:.3f}")

        csv_path = os.path.join(save_dir, f"loo_vs_shapley_{order_id}.csv")
        df.to_csv(csv_path, index=False)

        fig, ax = plt.subplots(figsize=(8, max(3, len(df) * 0.5)))
        y = range(len(df))
        width = 0.35
        ax.barh([i + width / 2 for i in y], df['loo_signed_shift'], height=width,
               color="#4C72B0", alpha=0.85, label="LOO")
        ax.barh([i - width / 2 for i in y], df['shapley_value'], height=width,
               color="#DD8452", alpha=0.85, label="Shapley")
        ax.set_yticks(list(y))
        ax.set_yticklabels(df['node_label'], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel("Signed value shift (hours)")
        ax.set_title(f"LOO vs. Shapley, order #{order_id} (Spearman's rho={rho:.3f})")
        ax.legend()
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        chart_path = os.path.join(save_dir, f"loo_vs_shapley_{order_id}.png")
        plt.savefig(chart_path, dpi=150)
        plt.close()

        return df

    # ------------------------------------------------------------------
    # Explainability — visualizations and entry points
    # ------------------------------------------------------------------

    def plot_feature_importances(self, node_type, feature_importances, save_path, order_id=None,
                                 label_map=None, xlabel=None):
        """Horizontal bar chart of per-feature value shifts for one node. Bar color
        encodes sign (green = this feature's real value pushed the prediction toward
        a longer remaining time than the population-mean substitute would; red =
        toward shorter -- see explain_trace()'s signed_shift docs for the full
        caveat), bar edge (black, thick) flags a >1 std ('large') shift.

        label_map: optional {raw_feature_name: readable_label} override, applied
        after the raw name lookup below. None (every existing caller) preserves
        raw/technical labels -- this is opt-in, for dashboard.py's humanized
        display only, and doesn't affect any thesis-citable saved PNG."""
        names = self.feature_names.get(node_type, [])
        feats, shifts, larges, signs = [], [], [], []
        for f, shift, large, signed_shift in feature_importances:
            label = names[f] if f < len(names) else f"feat_{f}"
            if label_map:
                label = label_map.get(label, label)
            feats.append(label)
            shifts.append(shift / 3600)
            larges.append(large)
            signs.append(signed_shift)

        if not feats:
            return

        fig, ax = plt.subplots(figsize=(7, max(3, len(feats) * 0.45)))
        colors = ["#2ca02c" if s > 0 else ("#d62728" if s < 0 else "#888888") for s in signs]
        edgecolors = ["black" if l else "none" for l in larges]
        linewidths = [2.0 if l else 0 for l in larges]
        bars = ax.barh(range(len(feats)), shifts, color=colors,
                       edgecolor=edgecolors, linewidth=linewidths)
        ax.set_yticks(range(len(feats)))
        ax.set_yticklabels(feats, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel(xlabel if xlabel is not None else "Value shift if removed (hours)")
        title = f"Feature importance — {node_type} node"
        if order_id is not None:
            title += f", order #{order_id}"
        ax.set_title(title)
        for bar, val in zip(bars, shifts):
            ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                    f"{val:.2f}h", va="center", fontsize=8)
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color="#2ca02c", label="increases predicted time"),
                            Patch(color="#d62728", label="decreases predicted time"),
                            Patch(facecolor="white", edgecolor="black", linewidth=2,
                                  label=">1 std shift (thick edge)")],
                  fontsize=8, loc="lower right")
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def plot_top_features_bar(self, labels, signed_values, save_path, title, method='InputXGradient',
                              xlabel=None):
        """Horizontal bar chart of pre-resolved (label, signed value) pairs,
        sorted by the caller. Generic, unlike plot_feature_importances() --
        takes fully-resolved label strings directly rather than deriving them from
        one node type's feature_names, since labels here may mix multiple node
        types/instances from one trace (e.g. dashboard.py's trace-wide top-K
        attribution view).

        xlabel: overrides the default f"{method} attribution" x-axis label --
        needed when signed_values aren't attribution at all (e.g. an
        hours-denominated LOO value shift plotted with the same label set for
        a side-by-side comparison).

        Color convention deliberately inverted from plot_feature_importances()'s
        (green=increases predicted time there): here, red = increases predicted
        time (a worse outcome for a remaining-time KPI), green = decreases it
        (a better outcome) -- an outcome-valence convention, not a raw-sign
        convention. Intentional divergence, scoped to this function only (its
        only callers are dashboard.py's feature-analysis charts, not any
        thesis-citable saved PNG) -- flagged since it means this project now
        has two different sign-color conventions across different charts."""
        if not labels:
            return

        fig, ax = plt.subplots(figsize=(7, max(3, len(labels) * 0.45)))
        colors = ["#d62728" if v > 0 else ("#2ca02c" if v < 0 else "#888888") for v in signed_values]
        ax.barh(range(len(labels)), signed_values, color=colors)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel(xlabel if xlabel is not None else f"{method} attribution")
        ax.set_title(title)
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def plot_aggregate_explanation_bars(self, explanation_signed_shifts, save_path, title,
                                        dataset_label=None, n_traces=None, top_n=15):
        """Single, global, ranked horizontal bar chart of the most influential
        'attr=value' explanations across many traces -- cf. Galanti et al. 2023b's
        Fig. 1 (a global explanation bar chart for remaining-time prediction).
        Layout matches Fig. 1 (ranked attr=value labels, magnitude on the x-axis,
        signed/colored bars); color semantics deliberately keep this project's own
        established convention (green = increases predicted time, red = decreases,
        thick edge = >1 std 'large' shift -- see plot_feature_importances()) rather
        than Galanti's own literal red-for-increase choice, for consistency with
        every other importance chart in this project.

        explanation_signed_shifts: {label: [signed_shift_hours, ...]} -- one entry
        per (decoded identity or bare node type) label, values already in hours."""
        import numpy as np
        summary = []
        for label, vals in explanation_signed_shifts.items():
            arr = np.array(vals)
            summary.append({
                'label': label, 'mean_signed_shift': float(arr.mean()),
                'std_shift': float(arr.std()), 'count': len(vals),
            })
        summary.sort(key=lambda r: abs(r['mean_signed_shift']), reverse=True)
        summary = summary[:top_n]

        if not summary:
            return summary

        labels = [r['label'] for r in summary]
        means = [r['mean_signed_shift'] for r in summary]
        larges = [abs(r['mean_signed_shift']) > self.target_std.item() / 3600.0 for r in summary]

        fig, ax = plt.subplots(figsize=(8, max(3, len(labels) * 0.4)))
        colors = ["#2ca02c" if m > 0 else ("#d62728" if m < 0 else "#888888") for m in means]
        edgecolors = ["black" if l else "none" for l in larges]
        linewidths = [2.0 if l else 0 for l in larges]
        bars = ax.barh(range(len(labels)), means, color=colors,
                       edgecolor=edgecolors, linewidth=linewidths)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlabel("Average influence on predicted remaining time (hours)")
        full_title = title
        if dataset_label is not None:
            full_title += f" — {dataset_label}"
        if n_traces is not None:
            full_title += f" (n={n_traces} traces)"
        ax.set_title(full_title)
        # Offset scaled to the axis range (not a fixed constant) -- a fixed offset is
        # invisible next to a large-magnitude bar (e.g. -31h) and crowds the value
        # text right up against the y-axis/tick-label area; a fixed axis-fraction
        # margin (added to xlim below) keeps it legible regardless of scale.
        max_abs = max((abs(m) for m in means), default=1.0) or 1.0
        offset = 0.03 * max_abs
        for bar, val in zip(bars, means):
            ha = "left" if val >= 0 else "right"
            ax.text(bar.get_width() + (offset if val >= 0 else -offset),
                    bar.get_y() + bar.get_height() / 2,
                    f"{val:+.2f}h", va="center", ha=ha, fontsize=8)
        xmin = min((m for m in means if m < 0), default=0.0)
        xmax = max((m for m in means if m > 0), default=0.0)
        ax.set_xlim(xmin - offset * 4, xmax + offset * 4)
        from matplotlib.patches import Patch
        # Placed outside the axes (below the x-label) rather than inside a corner --
        # bars are sorted by magnitude, so the smallest (near-zero) bars always end
        # up at the bottom, where an in-plot legend corner would otherwise sit
        # directly on top of their value labels.
        ax.legend(handles=[Patch(color="#2ca02c", label="increases predicted time"),
                            Patch(color="#d62728", label="decreases predicted time"),
                            Patch(facecolor="white", edgecolor="black", linewidth=2,
                                  label=">1 std shift (thick edge)")],
                  fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.close()
        return summary

    def plot_node_type_bars(self, rows, save_path, title, ylabel,
                            show_error_bars=False, secondary_series=None, secondary_label=None):
        """Shared node-type bar chart. Generalizes what used to be three separate,
        visually-inconsistent implementations (this method's own single-trace body,
        plus inline ax.bar() blocks duplicated in explain_aggregate() and
        explain_gnn_primary_aggregate()) into one: bar height/color/optional error
        bars/optional secondary-axis series, reused by both single-trace and
        aggregate callers.

        rows: list of {'node_type', 'value', 'signed_value', 'std'} dicts, in the
              desired bar order. 'std' may be None when show_error_bars is False.
        show_error_bars: draw yerr from each row's 'std' (aggregate use).
        secondary_series: optional list of numbers (one per row) drawn as a dashed
              secondary-axis line -- node count for single-trace, selection count
              for GNNExplainer-primary aggregate.
        """
        types = [r['node_type'] for r in rows]
        values = [r['value'] for r in rows]
        bar_colors = ["#2ca02c" if r['signed_value'] > 0
                      else ("#d62728" if r['signed_value'] < 0 else "#888888")
                      for r in rows]
        yerr = [r['std'] or 0.0 for r in rows] if show_error_bars else None

        fig, ax1 = plt.subplots(figsize=(8, 4))
        x = range(len(types))
        ax1.bar(x, values, color=bar_colors, alpha=0.8, yerr=yerr,
               capsize=4 if show_error_bars else 0, label=ylabel)
        ax1.set_xticks(x)
        ax1.set_xticklabels(types, rotation=30, ha="right", fontsize=9)
        ax1.set_ylabel(ylabel)
        ax1.set_title(title)

        lines2, labels2 = [], []
        if secondary_series is not None:
            ax2 = ax1.twinx()
            ax2.plot(x, secondary_series, "o--", color="#e74c3c", label=secondary_label)
            ax2.set_ylabel(secondary_label, color="#e74c3c")
            ax2.tick_params(axis="y", labelcolor="#e74c3c")
            lines2, labels2 = ax2.get_legend_handles_labels()

        from matplotlib.patches import Patch
        lines1, labels1 = ax1.get_legend_handles_labels()
        sign_handles = [Patch(color="#2ca02c", label="net: increases predicted time"),
                        Patch(color="#d62728", label="net: decreases predicted time")]
        ax1.legend(lines1 + lines2 + sign_handles, labels1 + labels2 +
                   [h.get_label() for h in sign_handles], fontsize=8, loc="upper right")
        ax1.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def plot_node_type_summary(self, node_importances, save_path, xlabel=None):
        """Bar chart of total influence per node type across the whole trace. Bar
        height is the magnitude sum (total impact); bar color reflects the sign of
        that type's NET signed sum (its dominant direction across instances)."""
        from collections import defaultdict
        type_shift = defaultdict(float)
        type_signed_shift = defaultdict(float)
        type_count = defaultdict(int)
        for node_type, idx, shift, _, signed_shift in node_importances:
            type_shift[node_type] += shift / 3600
            type_signed_shift[node_type] += signed_shift / 3600
            type_count[node_type] += 1

        types = sorted(type_shift, key=lambda t: type_shift[t], reverse=True)
        rows = [{'node_type': t, 'value': type_shift[t],
                'signed_value': type_signed_shift[t], 'std': None} for t in types]
        counts = [type_count[t] for t in types]
        self.plot_node_type_bars(
            rows, save_path, "Node type importance summary",
            xlabel if xlabel is not None else "Cumulative value shift if type removed (hours)",
            secondary_series=counts, secondary_label="Number of nodes of this type",
        )

    def explain_trace(self, order_id, top_k=5, save_dir=None, n_events=None):
        """Full LOO explanation for a single order trace. n_events=None (default)
        explains the order's last recorded prefix; an int explains the prefix with
        exactly that many Events nodes, matching explain_counterfactual()'s convention."""
        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}{suffix}")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        explain_subgraph = self._locate_test_graph(order_id, n_events)

        (node_importances, edge_importances,
         seed_feats, top_neighbor_feats, baseline_value) = self.reg_explanation(
            explain_subgraph, 0, order_id, top_k
        )

        metrics = self.evaluate_explanation_quality(
            explain_subgraph, 0, node_importances, edge_importances,
            node_top_k=10, edge_top_k=15, verbose=False
        )

        self.plot_feature_importances(
            self.kpi_viewpoint, seed_feats,
            os.path.join(save_dir, f"feat_importance_{self.kpi_viewpoint}.png"),
            order_id=order_id
        )
        if node_importances:
            top_nt, top_ni, _, _, _ = node_importances[0]
            self.plot_feature_importances(
                top_nt, top_neighbor_feats,
                os.path.join(save_dir, f"feat_importance_{top_nt}.png"),
                order_id=order_id
            )

        self.plot_node_type_summary(
            node_importances,
            os.path.join(save_dir, "node_type_summary.png")
        )

        # Decode node indices to a real-world identity (activity name for Events, e.g.
        # "Events[1]" -> "Events[1](PlaceOrder)"; company/department/vehicle id for any
        # other encoding-listed type, e.g. "Customers[0]" -> "Customers[0](Acme Inc)")
        # wherever a node is printed below, instead of leaving raw indices unexplained
        # in LOO's console/CSV output. Computed here (before the subgraph plot) so it
        # can also label explanation_subgraph.png's nodes, not just the console/CSV.
        id_map = self._decode_all_identifiers(explain_subgraph, order_id, n_events)

        exp_graph = self.reg_explanation_subgraph(
            explain_subgraph, 0, node_importances, edge_importances, node_top_k=10
        )
        self.reg_visualize_explanation_subgraph(
            exp_graph, save_path=os.path.join(save_dir, "explanation_subgraph.png"),
            id_map=id_map
        )

        names = self.feature_names

        def _node_label(nt, idx):
            if (nt, idx) in id_map:
                return f"{nt}[{idx}]({id_map[(nt, idx)]})"
            return f"{nt}[{idx}]"

        print(f"\n{'='*60}")
        print(f"Explanation for {self.kpi_viewpoint} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours")
        print(f"  Graph size : " +
              ", ".join(f"{nt}={explain_subgraph[nt].num_nodes}" for nt in explain_subgraph.node_types))
        print(f"\nTop {top_k} nodes by influence (signed value shift if removed -- "
              f"positive = this element's real value pushed the prediction toward a "
              f"LONGER remaining time than the population-mean substitute would; "
              f"negative = toward SHORTER):")
        for rank, (nt, idx, shift, large, signed_shift) in enumerate(node_importances[:top_k], 1):
            feat_vals = ""
            if explain_subgraph[nt].x.size(0) > idx:
                node_names = names.get(nt, [])
                non_zero = [(node_names[f] if f < len(node_names) else f"feat_{f}",
                             explain_subgraph[nt].x[idx, f].item())
                            for f in range(explain_subgraph[nt].x.size(1))
                            if explain_subgraph[nt].x[idx, f].item() != 0][:2]
                if non_zero:
                    feat_vals = "  [" + ", ".join(f"{n}={v:.2f}" for n, v in non_zero) + "]"
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {_node_label(nt, idx)}{feat_vals}: shift={signed_shift/3600:+.2f}h{flag}")

        top_per_type = self.top_nodes_per_type(node_importances, top_n=3)
        print(f"\nTop 3 nodes per type:")
        for nt in sorted(top_per_type):
            entries = ", ".join(
                (f"({id_map[(nt, idx)]})" if (nt, idx) in id_map else "")
                + f"[{idx}]={signed_shift/3600:+.2f}h" + (" [LARGE]" if large else "")
                for idx, shift, large, signed_shift in top_per_type[nt]
            )
            print(f"  {nt:<12}: {entries}")

        import pandas as pd
        pd.DataFrame([
            {'node_type': nt, 'rank': rank, 'node_idx': idx,
             'identifier': id_map.get((nt, idx), ''),
             'shift_hours': shift / 3600, 'signed_shift_hours': signed_shift / 3600,
             'large_shift': large}
            for nt, entries in top_per_type.items()
            for rank, (idx, shift, large, signed_shift) in enumerate(entries, 1)
        ]).to_csv(os.path.join(save_dir, "top_nodes_per_type.csv"), index=False)

        def _idx_label(nt, idx):
            """Bare-index label for one endpoint of an edge, decorated with its decoded
            identity when available (no point repeating other types' names, since
            et[0]/et[2] already print the type once for the whole edge)."""
            return f"{idx}({id_map[(nt, idx)]})" if (nt, idx) in id_map else str(idx)

        print(f"\nTop {top_k} edges by influence:")
        for rank, (et, e, shift, large, signed_shift) in enumerate(edge_importances[:top_k], 1):
            src, dst = explain_subgraph[et].edge_index[:, e].tolist()
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {et[0]}→{et[2]} ({_idx_label(et[0], src)}→{_idx_label(et[2], dst)}): "
                  f"shift={signed_shift/3600:+.2f}h{flag}")

        print(f"\nTop {top_k} features on seed {self.kpi_viewpoint} node:")
        seed_names = names.get(self.kpi_viewpoint, [])
        for rank, (f, shift, large, signed_shift) in enumerate(seed_feats[:top_k], 1):
            fname = seed_names[f] if f < len(seed_names) else f"feat_{f}"
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {fname}: shift={signed_shift/3600:+.2f}h{flag}")

        print(f"\nExplanation quality metrics:")
        print(f"  Fidelity+       : {metrics['fidelity_plus']:.4f}h  (↑ better)")
        print(f"  Fidelity−       : {metrics['fidelity_minus']:.4f}h  (↓ better)")
        print(f"  Characterization: {metrics['characterization_score']:.4f}  (↑ better, max 1.0)")
        print(f"  Node sparsity   : {metrics['node_sparsity']:.1%}")
        print(f"  Edge sparsity   : {metrics['edge_sparsity']:.1%}")
        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            "order_id": order_id,
            "predicted_hours": baseline_value / 3600,
            "n_events": explain_subgraph['Events'].x.size(0) if 'Events' in explain_subgraph.node_types else 0,
            "node_importances": node_importances,
            "edge_importances": edge_importances,
            "seed_feature_importances": seed_feats,
            "top_neighbor_feature_importances": top_neighbor_feats,
            "top_nodes_per_type": top_per_type,
            "metrics": metrics,
            "save_dir": save_dir,
        }

    def explain_trace_shapley(self, order_id, top_k=5, n_samples=100, save_dir=None, n_events=None):
        """Single-trace Shapley explanation -- the cost-bounded replacement for
        explain_trace(). Returns the EXACT same dict shape (same keys, same
        (key, shift, large, signed_shift)-tuple lists) so every downstream consumer
        (reg_explanation_subgraph, reg_visualize_explanation_subgraph,
        evaluate_explanation_quality, dashboard.py's render_local()) works unchanged
        -- only how shift/signed_shift get computed differs.

        reg_explanation() still runs its full exhaustive sweep first: some exhaustive
        pass is unavoidable to know which elements are worth explaining at all. Only
        that identified candidate set (this trace's own top_k nodes/edges plus each
        node type's own top 3, matching top_nodes_per_type()'s scope; the features
        reg_explanation() already truncated to top_k) gets Shapley-requantified --
        bounding cost to roughly (shown elements) x n_samples rather than
        (everything in the graph) x n_samples. explain_trace() itself is untouched:
        compare_loo_vs_shapley() and any other citable pathway still needs it exactly
        as it was.
        """
        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}{suffix}_shapley")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        explain_subgraph = self._locate_test_graph(order_id, n_events)

        (node_importances, edge_importances,
         seed_feats, top_neighbor_feats, baseline_value) = self.reg_explanation(
            explain_subgraph, 0, order_id, top_k
        )

        def _rebuild_node_or_edge_tuples(shapley_dict):
            out = []
            for key, sv in shapley_dict.items():
                shift = abs(sv)
                large = shift > self.target_std.item()
                out.append((key[0], key[1], shift, large, sv))
            out.sort(key=lambda t: t[2], reverse=True)
            return out

        def _rebuild_feature_tuples(shapley_dict):
            out = []
            for f, sv in shapley_dict.items():
                shift = abs(sv)
                large = shift > self.target_std.item()
                out.append((f, shift, large, sv))
            out.sort(key=lambda t: t[1], reverse=True)
            return out

        # Candidate set: this trace's own flat top_k nodes, unioned with each node
        # type's own top 3 (so top_nodes_per_type() below stays meaningful for types
        # that didn't make the flat top_k) -- both derived from reg_explanation()'s
        # already-sorted node_importances, no extra forward passes yet.
        flat_node_candidates = [(nt, idx) for nt, idx, _, _, _ in node_importances[:top_k]]
        per_type = self.top_nodes_per_type(node_importances, top_n=3)
        type_node_candidates = [(nt, idx) for nt, entries in per_type.items() for idx, _, _, _ in entries]
        node_candidates = list(dict.fromkeys(flat_node_candidates + type_node_candidates))
        edge_candidates = [(et, e) for et, e, _, _, _ in edge_importances[:top_k]]

        node_shapley = (self.shapley_node_importance(explain_subgraph, 0, baseline_value,
                                                      node_candidates, n_samples=n_samples)
                        if node_candidates else {})
        edge_shapley = (self.shapley_edge_importance(explain_subgraph, 0, baseline_value,
                                                      edge_candidates, n_samples=n_samples)
                        if edge_candidates else {})

        seed_feat_indices = [f for f, _, _, _ in seed_feats]
        seed_shapley = (self.shapley_feature_importance_for_node(
            explain_subgraph, self.kpi_viewpoint, 0, baseline_value, 0,
            feature_indices=seed_feat_indices, n_samples=n_samples)
            if seed_feat_indices else {})

        if node_importances and top_neighbor_feats:
            top_nt, top_ni = node_importances[0][0], node_importances[0][1]
            top_feat_indices = [f for f, _, _, _ in top_neighbor_feats]
            top_shapley = self.shapley_feature_importance_for_node(
                explain_subgraph, top_nt, top_ni, baseline_value, 0,
                feature_indices=top_feat_indices, n_samples=n_samples
            )
        else:
            top_shapley = {}

        node_importances = _rebuild_node_or_edge_tuples(node_shapley)
        edge_importances = _rebuild_node_or_edge_tuples(edge_shapley)
        seed_feats = _rebuild_feature_tuples(seed_shapley)
        top_neighbor_feats = _rebuild_feature_tuples(top_shapley)

        metrics = self.evaluate_explanation_quality(
            explain_subgraph, 0, node_importances, edge_importances,
            node_top_k=10, edge_top_k=15, verbose=False
        )

        self.plot_feature_importances(
            self.kpi_viewpoint, seed_feats,
            os.path.join(save_dir, f"feat_importance_{self.kpi_viewpoint}.png"),
            order_id=order_id, xlabel="Shift in Hours"
        )
        if node_importances:
            top_nt, top_ni, _, _, _ = node_importances[0]
            self.plot_feature_importances(
                top_nt, top_neighbor_feats,
                os.path.join(save_dir, f"feat_importance_{top_nt}.png"),
                order_id=order_id, xlabel="Shift in Hours"
            )

        self.plot_node_type_summary(
            node_importances, os.path.join(save_dir, "node_type_summary.png"),
            xlabel="Shift in Hours"
        )

        id_map = self._decode_all_identifiers(explain_subgraph, order_id, n_events)

        exp_graph = self.reg_explanation_subgraph(
            explain_subgraph, 0, node_importances, edge_importances, node_top_k=10
        )
        self.reg_visualize_explanation_subgraph(
            exp_graph, save_path=os.path.join(save_dir, "explanation_subgraph.png"),
            id_map=id_map
        )

        top_per_type = self.top_nodes_per_type(node_importances, top_n=3)

        import pandas as pd
        pd.DataFrame([
            {'node_type': nt, 'rank': rank, 'node_idx': idx,
             'identifier': id_map.get((nt, idx), ''),
             'shift_hours': shift / 3600, 'signed_shift_hours': signed_shift / 3600,
             'large_shift': large}
            for nt, entries in top_per_type.items()
            for rank, (idx, shift, large, signed_shift) in enumerate(entries, 1)
        ]).to_csv(os.path.join(save_dir, "top_nodes_per_type.csv"), index=False)

        print(f"\n{'='*60}")
        print(f"Shapley explanation for {self.kpi_viewpoint} #{order_id} "
              f"(candidate set: LOO top_k={top_k} + per-type top 3, n_samples={n_samples})")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours")
        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            "order_id": order_id,
            "predicted_hours": baseline_value / 3600,
            "n_events": explain_subgraph['Events'].x.size(0) if 'Events' in explain_subgraph.node_types else 0,
            "node_importances": node_importances,
            "edge_importances": edge_importances,
            "seed_feature_importances": seed_feats,
            "top_neighbor_feature_importances": top_neighbor_feats,
            "top_nodes_per_type": top_per_type,
            "metrics": metrics,
            "save_dir": save_dir,
        }

    def explain_trace_ig(self, order_id, methods=('InputXGradient', 'IntegratedGradients'),
                         save_dir=None, n_events=None, top_k_display=10):
        """Single-trace gradient-based feature attribution for one order — the
        per-order analogue of explain_feature_attribution()'s aggregate computation,
        using the same _compute_attribution_for_graph() helper and the same bar-chart
        style, just for one graph instead of averaged across the test set. n_events=None
        (default) explains the order's last recorded prefix; an int explains the prefix
        with exactly that many Events nodes, matching explain_counterfactual()'s
        convention. top_k_display caps the number of features shown on each bar
        chart's Y axis (node types like Events/Customers have far more feature
        dimensions than are readable on one axis; this only affects the plot, not
        the full per-feature CSV, which still records every dimension)."""
        import numpy as np
        import pandas as pd
        from matplotlib.patches import Patch

        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}{suffix}")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        explain_subgraph = self._locate_test_graph(order_id, n_events)

        print(f"\n{'='*60}")
        print(f"Feature attribution for {self.kpi_viewpoint} #{order_id}")
        print(f"{'='*60}")

        results = {}
        for method in methods:
            masks = self._compute_attribution_for_graph(explain_subgraph, method=method)
            mean_signed = {nt: mask.mean(axis=0) for nt, mask in masks.items() if mask.shape[0] > 0}
            mean_abs    = {nt: np.abs(mask).mean(axis=0) for nt, mask in masks.items() if mask.shape[0] > 0}
            results[method] = {'signed': mean_signed, 'abs': mean_abs}

            print(f"\n[{method}]")
            for nt in sorted(mean_abs):
                scores = mean_abs[nt]
                fnames = self.feature_names.get(nt, [f"feat_{j}" for j in range(len(scores))])
                top_idx = np.argsort(scores)[::-1][:3]
                top_str = ", ".join(
                    f"{fnames[j] if j < len(fnames) else f'feat_{j}'}={scores[j]:.4f}"
                    for j in top_idx
                )
                print(f"  {nt:12s}  top-3: {top_str}")

            suffix = method.lower()
            rows = []
            for nt in sorted(mean_abs):
                scores_abs = mean_abs[nt]
                scores_sgn = mean_signed[nt]
                fnames = self.feature_names.get(nt, [f"feat_{j}" for j in range(len(scores_abs))])
                labels = [fnames[j] if j < len(fnames) else f"feat_{j}" for j in range(len(scores_abs))]
                order = np.argsort(scores_abs)[::-1][:top_k_display]
                colors = ['#4C72B0' if scores_sgn[j] >= 0 else '#DD8452' for j in order]

                fig, ax = plt.subplots(figsize=(max(6, len(order) * 0.45 + 2), 4))
                ax.barh([labels[j] for j in order], scores_abs[order], color=colors, alpha=0.85)
                ax.set_xlabel(f"|{method}| attribution")
                ax.set_title(f"Feature attribution — {nt} nodes ({method}), order #{order_id}")
                ax.grid(True, axis='x', alpha=0.3)
                ax.legend(handles=[Patch(color='#4C72B0', label='+ (raises prediction)'),
                                    Patch(color='#DD8452', label='− (lowers prediction)')],
                          fontsize=8)
                plt.tight_layout()
                plt.savefig(os.path.join(save_dir, f"ig_attribution_{nt.lower()}_{suffix}.png"), dpi=150)
                plt.close()

                for dim, (s, a) in enumerate(zip(scores_sgn, scores_abs)):
                    fname = fnames[dim] if dim < len(fnames) else f"feat_{dim}"
                    rows.append({'node_type': nt, 'feature_dim': dim, 'feature_name': fname,
                                 'signed': round(float(s), 6), 'abs': round(float(a), 6)})
            pd.DataFrame(rows).to_csv(os.path.join(save_dir, f"ig_attribution_{suffix}.csv"), index=False)

            # Node-type x feature-dim heatmap -- per-trace analogue of
            # explain_feature_attribution()'s aggregate ig_heatmap.
            all_types = sorted(mean_abs)
            max_dims  = max(len(v) for v in mean_abs.values())
            heat = np.zeros((len(all_types), max_dims))
            for i, nt in enumerate(all_types):
                arr = mean_abs[nt]
                heat[i, :len(arr)] = arr

            fig, ax = plt.subplots(figsize=(max(8, max_dims * 0.5 + 2), len(all_types) + 1))
            im = ax.imshow(heat, aspect='auto', cmap='YlOrRd')
            ax.set_yticks(range(len(all_types)))
            ax.set_yticklabels(all_types)
            ax.set_xlabel("Feature dimension index")
            ax.set_title(f"Feature attribution heatmap (|{method}|), order #{order_id}")
            plt.colorbar(im, ax=ax, shrink=0.8)
            plt.tight_layout()
            plt.savefig(os.path.join(save_dir, f"ig_heatmap_{suffix}.png"), dpi=150)
            plt.close()

            # Signed companion heatmap -- matches Zhai et al. 2025's own heatmap
            # convention (diverging colormap over signed values, not magnitude
            # only). NOT a replacement for the |...| heatmap above: a feature
            # whose sign flips across node instances/traces can average toward
            # zero here while still showing up as large in the abs heatmap --
            # the two are meant to be read together, not one in place of the
            # other. Zero-padded cells (node types with fewer feature dims than
            # max_dims) are masked as NaN/gray rather than left as literal 0.0,
            # since on a diverging colormap 0.0 is visually indistinguishable
            # from "no such feature dimension" -- unlike the sequential |...|
            # heatmap above, where 0 reads unambiguously as "no magnitude".
            heat_signed = np.full((len(all_types), max_dims), np.nan)
            for i, nt in enumerate(all_types):
                arr = mean_signed[nt]
                heat_signed[i, :len(arr)] = arr

            cmap_signed = plt.get_cmap('RdBu_r').copy()
            cmap_signed.set_bad('lightgray')

            fig, ax = plt.subplots(figsize=(max(8, max_dims * 0.5 + 2), len(all_types) + 1))
            im = ax.imshow(heat_signed, aspect='auto', cmap=cmap_signed)
            ax.set_yticks(range(len(all_types)))
            ax.set_yticklabels(all_types)
            ax.set_xlabel("Feature dimension index")
            ax.set_title(f"Feature attribution heatmap (signed {method}), order #{order_id}")
            plt.colorbar(im, ax=ax, shrink=0.8)
            if max_dims <= 20:  # keep cell-value annotations legible on narrow heatmaps only
                for i in range(len(all_types)):
                    for j in range(max_dims):
                        val = heat_signed[i, j]
                        if not np.isnan(val):
                            ax.text(j, i, f"{val:.2f}", ha='center', va='center', fontsize=6)
            plt.tight_layout()
            plt.savefig(os.path.join(save_dir, f"ig_heatmap_signed_{suffix}.png"), dpi=150)
            plt.close()

        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)
        return results

    def explain_aggregate(self, n_traces=50, top_k=5, save_dir=None):
        """Run LOO explanation on n_traces test graphs and aggregate results."""
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        last_event_graphs = [g for g in self.test_data
                             if g[self.kpi_viewpoint]['last_event'][0].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running aggregate explanation on {len(sample)} traces…")

        import numpy as np
        import pandas as pd
        from collections import defaultdict
        type_shifts = defaultdict(list)
        type_signed_shifts = defaultdict(list)
        feat_shifts = defaultdict(lambda: defaultdict(list))
        # Per-decoded-identity signed shifts (e.g. "Events=PlaceOrder", "Items=i-880001"),
        # for plot_aggregate_explanation_bars() -- same pattern as
        # explain_gnn_primary_aggregate()'s own explanation_signed_shifts, a finer
        # granularity than type_signed_shifts above (which only aggregates at the
        # node-TYPE level, losing individual identity).
        explanation_signed_shifts = defaultdict(list)
        # Per-"NodeType.feature_name" signed shifts, flat across node types -- feeds the
        # dashboard's "top K features" chart, the feature-level analogue of
        # explanation_signed_shifts above. Dot separator (not "=") deliberately distinct from
        # explanation_signed_shifts' node-identity labels so the two charts' labels are never
        # visually confused, and so two node types sharing a feature name (e.g. Orders.price
        # and Items.price) don't collide.
        feature_signed_shifts = defaultdict(list)
        all_metrics = []
        names = self.feature_names

        n_failed = 0
        for g in sample:
            oid = int(g[self.kpi_viewpoint]['id'][0].item()) if self.kpi_viewpoint in g.node_types else None
            try:
                (node_imp, edge_imp, seed_feats, top_node_feats, _) = self.reg_explanation(g, 0, None, top_k)
            except Exception as ex:
                n_failed += 1
                print(f"  [trace failed] order={oid}: {type(ex).__name__}: {ex}")
                continue

            id_map = self._decode_all_identifiers(g, oid) if oid is not None else {}
            for nt, idx, shift, _, signed_shift in node_imp:
                type_shifts[nt].append(shift / 3600)
                type_signed_shifts[nt].append(signed_shift / 3600)
                decoded = id_map.get((nt, idx))
                label = f"{nt}={decoded}" if decoded else nt
                explanation_signed_shifts[label].append(signed_shift / 3600.0)

            for f, shift, _, signed_shift in seed_feats:
                feat_shifts[self.kpi_viewpoint][f].append(shift / 3600)
                feat_names_vp = names.get(self.kpi_viewpoint, [])
                feat_label = feat_names_vp[f] if f < len(feat_names_vp) else f"feat_{f}"
                feature_signed_shifts[f"{self.kpi_viewpoint}.{feat_label}"].append(signed_shift / 3600.0)

            # top_node_feats is per-feature LOO for whichever node ranked #1 in node_imp that
            # trace (any node type, not just the viewpoint) -- already computed inside
            # reg_explanation() regardless of whether it's used, so capturing it here for the
            # features chart costs nothing extra.
            if node_imp and top_node_feats:
                top_nt = node_imp[0][0]
                feat_names_top = names.get(top_nt, [])
                for f, shift, _, signed_shift in top_node_feats:
                    feat_label = feat_names_top[f] if f < len(feat_names_top) else f"feat_{f}"
                    feature_signed_shifts[f"{top_nt}.{feat_label}"].append(signed_shift / 3600.0)

            m = self.evaluate_explanation_quality(g, 0, node_imp, edge_imp,
                                                   node_top_k=10, edge_top_k=15, verbose=False)
            all_metrics.append(m)

        import statistics

        types = sorted(type_shifts, key=lambda t: -sum(type_shifts[t]) / max(len(type_shifts[t]), 1))
        mean_shifts = [sum(type_shifts[t]) / len(type_shifts[t]) for t in types]
        mean_signed_shifts = [sum(type_signed_shifts[t]) / len(type_signed_shifts[t]) for t in types]
        std_shifts = [float(np.array(type_shifts[t]).std()) for t in types]

        rows = [{'node_type': t, 'value': v, 'signed_value': sv, 'std': sd}
                for t, v, sv, sd in zip(types, mean_shifts, mean_signed_shifts, std_shifts)]
        self.plot_node_type_bars(
            rows, os.path.join(save_dir, "aggregate_node_type_importance.png"),
            f"Aggregate node type importance (n={len(sample)} traces)",
            "Mean value shift if removed (hours)", show_error_bars=True,
        )

        # Global, ranked 'attr=value' explanation bars -- cf. Galanti et al. 2023b Fig. 1,
        # same pattern as explain_gnn_primary_aggregate()'s own equivalent output. A
        # finer-grained companion to the node-TYPE-level chart above: same underlying
        # per-trace data, but bucketed by decoded identity (e.g. "Events=PlaceOrder")
        # instead of just node type, in one combined cross-type ranking.
        explanation_summary = self.plot_aggregate_explanation_bars(
            explanation_signed_shifts, os.path.join(save_dir, "aggregate_explanation_bars.png"),
            "Global explanations for remaining time prediction",
            dataset_label=self.database, n_traces=len(sample), top_n=20,
        )
        pd.DataFrame(explanation_summary).to_csv(
            os.path.join(save_dir, "aggregate_explanation_bars.csv"), index=False
        )

        # Feature-level analogue of the block above -- flat, cross-node-type ranking of
        # "NodeType.feature_name" labels instead of node identities. Feeds the dashboard's
        # "top K features" chart, the same way aggregate_explanation_bars.csv feeds "top K nodes".
        feature_summary = self.plot_aggregate_explanation_bars(
            feature_signed_shifts, os.path.join(save_dir, "aggregate_feature_bars.png"),
            "Global feature attribution for remaining time prediction",
            dataset_label=self.database, n_traces=len(sample), top_n=20,
        )
        pd.DataFrame(feature_summary).to_csv(
            os.path.join(save_dir, "aggregate_feature_bars.csv"), index=False
        )
        for nt, fdict in feat_shifts.items():
            feat_names_nt = names.get(nt, [])
            items = sorted(fdict.items(), key=lambda kv: -sum(kv[1]) / max(len(kv[1]), 1))
            feats = [feat_names_nt[f] if f < len(feat_names_nt) else f"feat_{f}" for f, _ in items]
            means = [sum(vs) / len(vs) for _, vs in items]
            if not feats:
                continue
            fig, ax = plt.subplots(figsize=(7, max(3, len(feats) * 0.45)))
            ax.barh(range(len(feats)), means, color="#4C72B0", alpha=0.85)
            ax.set_yticks(range(len(feats)))
            ax.set_yticklabels(feats, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel("Mean value shift if removed (hours)")
            ax.set_title(f"Aggregate feature importance — {nt} (n={len(sample)})")
            ax.grid(True, axis="x", alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(save_dir, f"aggregate_feat_importance_{nt}.png"), dpi=150)
            plt.close()

        import csv
        metric_keys = ["fidelity_plus", "fidelity_minus", "characterization_score",
                       "node_sparsity", "edge_sparsity"]
        csv_path = os.path.join(save_dir, "aggregate_metrics.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["trace"] + metric_keys)
            writer.writeheader()
            for i, m in enumerate(all_metrics):
                writer.writerow({"trace": i, **{k: round(m[k], 6) for k in metric_keys}})
            summary = {"trace": "mean"}
            for k in metric_keys:
                vals = [m[k] for m in all_metrics]
                summary[k] = round(sum(vals) / len(vals), 6) if vals else float("nan")
            writer.writerow(summary)
            summary_std = {"trace": "std"}
            for k in metric_keys:
                vals = [m[k] for m in all_metrics]
                summary_std[k] = round(statistics.stdev(vals), 6) if len(vals) > 1 else 0.0
            writer.writerow(summary_std)

        print(f"\nAggregate explanation quality (n={len(all_metrics)} traces, "
              f"{n_failed} failed/skipped out of {len(sample)} sampled):")
        for k in metric_keys:
            vals = [m[k] for m in all_metrics]
            mean_v = sum(vals) / len(vals) if vals else float("nan")
            std_v = statistics.stdev(vals) if len(vals) > 1 else 0.0
            print(f"  {k:25s}: {mean_v:.4f} ± {std_v:.4f}")
        print(f"\nAggregate outputs saved to: {save_dir}")

        return all_metrics

    def explain_aggregate_shapley(self, n_traces=50, top_k=5, n_samples=30, revisit_n=3,
                                  max_revisit_candidates=6, save_dir=None):
        """Aggregate Shapley replacement for the dashboard's two flat "top K" bar
        charts (nodes, features) only -- NOT the per-node-type chart or the depth
        heatmap, which stay LOO-based (both are exhaustive-by-design; Shapley-izing
        them fully would multiply their already-largest cost ~100x -- see the
        planning notes for explain_trace_shapley()).

        Preserves explain_aggregate()'s own SELECTION semantics exactly -- the same
        exhaustive per-trace LOO pooling (unchanged cost) decides who makes the top
        ~20 for each chart. Only the DISPLAYED magnitude for those winners changes:
        each winning label gets Shapley-requantified by revisiting up to revisit_n
        of the traces where it actually occurred, using a bounded candidate set (the
        target instance/feature plus up to max_revisit_candidates-1 other same-type
        instances/features from that same trace) -- this is what actually captures
        redundancy (a singleton candidate set would trivially collapse back to
        LOO's own value). Writes to the SAME file paths explain_aggregate() uses for
        these two charts, so render_loo_aggregate()'s reading code needs no changes.
        explain_aggregate() itself is untouched -- still needed for the node-type
        chart and depth heatmap, which keep calling it as before.

        Defaults verified empirically on real data at n_traces=50: the original
        planning estimate (n_samples=100, revisit_n=5, max_revisit_candidates=10)
        actually cost ~334s, well over the ~100s planned -- these lower defaults
        cost ~81s instead, confirmed to still surface the same redundancy-driven
        corrections (see logistics' BringToLoadingBay case, corrected this
        session's earlier compare_loo_vs_shapley() runs) while staying close to
        the originally planned budget."""
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        last_event_graphs = [g for g in self.test_data
                             if g[self.kpi_viewpoint]['last_event'][0].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running aggregate Shapley explanation on {len(sample)} traces "
              f"(identification via exhaustive LOO, unchanged cost; only the top ~20 "
              f"winners per chart get Shapley-requantified)…")

        import numpy as np
        import pandas as pd
        from collections import defaultdict
        explanation_signed_shifts = defaultdict(list)
        explanation_sources = defaultdict(list)  # label -> [(trace_idx, node_type, idx), ...]
        feature_signed_shifts = defaultdict(list)
        feature_sources = defaultdict(list)  # label -> [(trace_idx, node_type, node_idx, feat_idx), ...]
        all_metrics = []
        names = self.feature_names
        baseline_values = {}

        n_failed = 0
        for trace_idx, g in enumerate(sample):
            oid = int(g[self.kpi_viewpoint]['id'][0].item()) if self.kpi_viewpoint in g.node_types else None
            try:
                (node_imp, edge_imp, seed_feats, top_node_feats, baseline_value) = self.reg_explanation(g, 0, None, top_k)
            except Exception as ex:
                n_failed += 1
                print(f"  [trace failed] order={oid}: {type(ex).__name__}: {ex}")
                continue
            baseline_values[trace_idx] = baseline_value

            id_map = self._decode_all_identifiers(g, oid) if oid is not None else {}
            for nt, idx, shift, _, signed_shift in node_imp:
                decoded = id_map.get((nt, idx))
                label = f"{nt}={decoded}" if decoded else nt
                explanation_signed_shifts[label].append(signed_shift / 3600.0)
                explanation_sources[label].append((trace_idx, nt, idx))

            for f, shift, _, signed_shift in seed_feats:
                feat_names_vp = names.get(self.kpi_viewpoint, [])
                feat_label = feat_names_vp[f] if f < len(feat_names_vp) else f"feat_{f}"
                label = f"{self.kpi_viewpoint}.{feat_label}"
                feature_signed_shifts[label].append(signed_shift / 3600.0)
                feature_sources[label].append((trace_idx, self.kpi_viewpoint, 0, f))

            if node_imp and top_node_feats:
                top_nt, top_ni = node_imp[0][0], node_imp[0][1]
                feat_names_top = names.get(top_nt, [])
                for f, shift, _, signed_shift in top_node_feats:
                    feat_label = feat_names_top[f] if f < len(feat_names_top) else f"feat_{f}"
                    label = f"{top_nt}.{feat_label}"
                    feature_signed_shifts[label].append(signed_shift / 3600.0)
                    feature_sources[label].append((trace_idx, top_nt, top_ni, f))

            m = self.evaluate_explanation_quality(g, 0, node_imp, edge_imp,
                                                   node_top_k=10, edge_top_k=15, verbose=False)
            all_metrics.append(m)

        def _top_labels(pooled, top_n=20):
            scored = [(label, abs(sum(vals) / len(vals))) for label, vals in pooled.items() if vals]
            scored.sort(key=lambda t: t[1], reverse=True)
            return [label for label, _ in scored[:top_n]]

        def _revisit_node_label(nt, idx, trace_idx):
            g = sample[trace_idx]
            baseline_value = baseline_values[trace_idx]
            all_same_type = [(nt, j) for j in range(g[nt].x.size(0))] if nt in g.node_types else [(nt, idx)]
            if len(all_same_type) > max_revisit_candidates:
                others = [k for k in all_same_type if k != (nt, idx)][:max_revisit_candidates - 1]
                candidates = others + [(nt, idx)]
            else:
                candidates = all_same_type
            sv = self.shapley_node_importance(g, 0, baseline_value, candidates, n_samples=n_samples)
            return sv[(nt, idx)] / 3600.0

        def _revisit_feature_label(nt, node_idx, feat_idx, trace_idx):
            g = sample[trace_idx]
            baseline_value = baseline_values[trace_idx]
            all_feats = [f for f in range(g[nt].x.size(1)) if g[nt].x[node_idx, f].item() != 0.0]
            if feat_idx not in all_feats:
                all_feats.append(feat_idx)
            if len(all_feats) > max_revisit_candidates:
                others = [f for f in all_feats if f != feat_idx][:max_revisit_candidates - 1]
                candidates = others + [feat_idx]
            else:
                candidates = all_feats
            sv = self.shapley_feature_importance_for_node(g, nt, node_idx, baseline_value, 0,
                                                           feature_indices=candidates, n_samples=n_samples)
            return sv[feat_idx] / 3600.0

        print(f"  Shapley-requantifying top nodes…")
        winners = _top_labels(explanation_signed_shifts, top_n=20)
        shapley_node_shifts = defaultdict(list)
        for label in winners:
            for trace_idx, nt, idx in explanation_sources[label][:revisit_n]:
                shapley_node_shifts[label].append(_revisit_node_label(nt, idx, trace_idx))

        explanation_summary = self.plot_aggregate_explanation_bars(
            shapley_node_shifts, os.path.join(save_dir, "aggregate_explanation_bars.png"),
            "Global Shapley explanations for remaining time prediction",
            dataset_label=self.database, n_traces=len(sample), top_n=20,
        )
        pd.DataFrame(explanation_summary).to_csv(
            os.path.join(save_dir, "aggregate_explanation_bars.csv"), index=False
        )

        print(f"  Shapley-requantifying top features…")
        feat_winners = _top_labels(feature_signed_shifts, top_n=20)
        shapley_feature_shifts = defaultdict(list)
        for label in feat_winners:
            for trace_idx, nt, node_idx, feat_idx in feature_sources[label][:revisit_n]:
                shapley_feature_shifts[label].append(_revisit_feature_label(nt, node_idx, feat_idx, trace_idx))

        feature_summary = self.plot_aggregate_explanation_bars(
            shapley_feature_shifts, os.path.join(save_dir, "aggregate_feature_bars.png"),
            "Global Shapley feature attribution for remaining time prediction",
            dataset_label=self.database, n_traces=len(sample), top_n=20,
        )
        pd.DataFrame(feature_summary).to_csv(
            os.path.join(save_dir, "aggregate_feature_bars.csv"), index=False
        )

        import csv, statistics
        metric_keys = ["fidelity_plus", "fidelity_minus", "characterization_score",
                       "node_sparsity", "edge_sparsity"]
        csv_path = os.path.join(save_dir, "aggregate_metrics.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["trace"] + metric_keys)
            writer.writeheader()
            for i, m in enumerate(all_metrics):
                writer.writerow({"trace": i, **{k: round(m[k], 6) for k in metric_keys}})
            summary = {"trace": "mean"}
            for k in metric_keys:
                vals = [m[k] for m in all_metrics]
                summary[k] = round(sum(vals) / len(vals), 6) if vals else float("nan")
            writer.writerow(summary)
            summary_std = {"trace": "std"}
            for k in metric_keys:
                vals = [m[k] for m in all_metrics]
                summary_std[k] = round(statistics.stdev(vals), 6) if len(vals) > 1 else 0.0
            writer.writerow(summary_std)

        print(f"\nAggregate Shapley outputs saved to: {save_dir} "
              f"({n_failed} failed/skipped out of {len(sample)} sampled)")

        return all_metrics

    def explain_loo_by_depth(self, n_traces=200, save_dir=None):
        """Depth-stratified LOO node-type importance across ALL test prefixes (not just
        last-event graphs, unlike explain_aggregate()) -- the LOO analogue of
        _explain_attribution_by_depth(), closing the gap EXPLAINABILITY.md flags:
        explain_aggregate() pools all prefix depths together, obscuring whether a node
        type's relative importance shifts as a trace matures (e.g. resource-assignment
        types mattering more early on, before the outcome-defining work has happened).
        Reuses the same _DEPTH_BINS as _explain_attribution_by_depth()/compare_models().

        Full LOO (this method) is far more expensive per graph than a gradient
        attribution backward pass -- O(nodes+edges) forward passes per graph, not one --
        so this defaults to a bounded sample (n_traces=200) rather than the whole test
        set the way _explain_attribution_by_depth() does by default.
        """
        import numpy as np
        import pandas as pd

        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        graphs = self.test_data if n_traces is None else self.test_data[:n_traces]
        n = len(graphs)
        print(f"\nDepth-stratified LOO: {n} prefixes across all depths (vs. "
              f"last-event-only scope in explain_aggregate() -- this is much slower, "
              f"full LOO ablation per prefix, not one backward pass)")

        bin_accum = {lbl: {} for _, _, lbl in self._DEPTH_BINS}  # lbl -> {node_type: [shifts_h]}
        n_used = 0
        for i, g in enumerate(graphs):
            if i % max(1, n // 10) == 0:
                print(f"  LOO depth stratification: {100 * i // n}%")
            n_events = g['Events'].x.size(0) if 'Events' in g.node_types else 0
            lbl = next((l for lo, hi, l in self._DEPTH_BINS if lo <= n_events <= hi), None)
            if lbl is None or g[self.kpi_viewpoint].y.shape[0] == 0:
                continue  # no depth bin matched, or kpi_viewpoint hasn't appeared yet
            try:
                node_imp, _, _, _, _ = self.reg_explanation(g, 0, None, top_k=5)
            except Exception as ex:
                print(f"  [skipped] prefix {i}: {type(ex).__name__}: {ex}")
                continue
            n_used += 1
            for nt, idx, shift, large, signed_shift in node_imp:
                bin_accum[lbl].setdefault(nt, []).append(shift / 3600.0)
        print(f"  LOO depth stratification: 100%  ({n_used}/{n} prefixes used)")

        labels = [lbl for _, _, lbl in self._DEPTH_BINS if bin_accum[lbl]]
        if not labels:
            print("  No prefixes with usable data for LOO depth stratification -- skipped")
            return None

        all_types = sorted({nt for lbl in labels for nt in bin_accum[lbl]})
        heat = np.array([
            [np.mean(bin_accum[lbl][nt]) if bin_accum[lbl].get(nt) else 0.0 for lbl in labels]
            for nt in all_types
        ])

        fig, ax = plt.subplots(figsize=(max(6, len(labels) * 0.9 + 2), len(all_types) * 0.4 + 2))
        im = ax.imshow(heat, aspect='auto', cmap='YlOrRd')
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels)
        ax.set_yticks(range(len(all_types)))
        ax.set_yticklabels(all_types, fontsize=9)
        ax.set_xlabel("Prefix depth (n events seen)")
        ax.set_title("LOO node-type importance by prefix depth (mean |shift|, hours)")
        plt.colorbar(im, ax=ax, shrink=0.8)
        plt.tight_layout()
        heatmap_path = os.path.join(save_dir, "loo_depth_heatmap.png")
        plt.savefig(heatmap_path, dpi=150)
        plt.close()

        rows = []
        for lbl in labels:
            for nt in all_types:
                vals = bin_accum[lbl].get(nt, [])
                rows.append({
                    'depth_bin': lbl, 'node_type': nt,
                    'mean_abs_shift_hours': round(float(np.mean(vals)), 6) if vals else 0.0,
                    'n_prefixes': len(vals),
                })
        csv_path = os.path.join(save_dir, "loo_depth_importance.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        print(f"  Saved LOO depth-stratified importance to: {save_dir}")

        return {'labels': labels, 'node_types': all_types, 'heat': heat}

    def plot_feature_depth_heatmap(self, row_labels, depth_labels, matrix, save_path, title):
        """Generic imshow heatmap of row_labels (e.g. features) x depth_labels (prefix-depth
        bins) -- same visual structure as explain_loo_by_depth()'s own inline heatmap, but
        standalone/reusable so that already-verified method isn't touched. Used both for
        explain_feature_attribution_by_depth()'s canonical (all-feature) save and for the
        dashboard's live Top-K-sliced re-render."""
        import numpy as np

        if not row_labels or not depth_labels:
            return
        matrix = np.asarray(matrix)
        fig, ax = plt.subplots(figsize=(max(6, len(depth_labels) * 0.9 + 2), len(row_labels) * 0.4 + 2))
        im = ax.imshow(matrix, aspect='auto', cmap='YlOrRd')
        ax.set_xticks(range(len(depth_labels)))
        ax.set_xticklabels(depth_labels)
        ax.set_yticks(range(len(row_labels)))
        ax.set_yticklabels(row_labels, fontsize=9)
        ax.set_xlabel("Prefix depth (n events seen)")
        ax.set_title(title)
        plt.colorbar(im, ax=ax, shrink=0.8)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def explain_feature_attribution_by_depth(self, node_type=None, n_traces=50, save_dir=None):
        """Depth-stratified, per-FEATURE LOO importance for one selected node type -- the
        feature-level analogue of explain_loo_by_depth() (which stops at node-type
        granularity) and the depth-resolved analogue of explain_aggregate()'s
        aggregate_feat_importance_{node_type}.png (which pools all depths together and, in
        practice, is only ever populated for self.kpi_viewpoint). Powers the dashboard's
        Aggregate-tab node-type selector.

        node_type defaults to self.kpi_viewpoint (the dataset's default viewpoint object).
        Iterates ALL test-set prefixes (not just last-event, like explain_loo_by_depth() and
        unlike explain_aggregate()) so the depth axis actually has spread -- last-event graphs
        cluster in the deepest bin alone.

        For non-viewpoint node types a single prefix can contain zero, one, or many instances
        of node_type (e.g. many Events nodes) -- every instance's per-feature LOO shift is
        pooled into that prefix's depth bin, same pooling convention explain_aggregate()/
        explain_loo_by_depth() already use (no per-instance averaging step).
        """
        import numpy as np
        import pandas as pd

        if node_type is None:
            node_type = self.kpi_viewpoint
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        feat_names = self.feature_names.get(node_type, [])
        n_feats = len(feat_names) if feat_names else 1

        graphs = self.test_data if n_traces is None else self.test_data[:n_traces]
        n = len(graphs)
        print(f"\nDepth-stratified feature LOO for node_type={node_type}: {n} prefixes")

        bin_accum = {lbl: {} for _, _, lbl in self._DEPTH_BINS}  # lbl -> {feature_idx: [shifts_h]}
        n_used = 0
        for i, g in enumerate(graphs):
            if i % max(1, n // 10) == 0:
                print(f"  Feature-by-depth LOO: {100 * i // n}%")
            n_events = g['Events'].x.size(0) if 'Events' in g.node_types else 0
            lbl = next((l for lo, hi, l in self._DEPTH_BINS if lo <= n_events <= hi), None)
            if lbl is None or g[self.kpi_viewpoint].y.shape[0] == 0:
                continue
            if node_type not in g.node_types or g[node_type].x.size(0) == 0:
                continue
            try:
                baseline_value = self._predict_value_for_graph(g, 0)
                for idx in range(g[node_type].x.size(0)):
                    feats = self.reg_feature_importance_for_node_in_graph(
                        g, node_type, idx, baseline_value, target_object_idx=0, top_k=n_feats
                    )
                    for f, shift, _, _ in feats:
                        bin_accum[lbl].setdefault(f, []).append(shift / 3600.0)
            except Exception as ex:
                print(f"  [skipped] prefix {i}: {type(ex).__name__}: {ex}")
                continue
            n_used += 1
        print(f"  Feature-by-depth LOO: 100%  ({n_used}/{n} prefixes used)")

        labels = [lbl for _, _, lbl in self._DEPTH_BINS if bin_accum[lbl]]
        if not labels:
            print("  No prefixes with usable data for feature-by-depth LOO -- skipped")
            return None

        all_feats = sorted({f for lbl in labels for f in bin_accum[lbl]})
        feat_labels = [feat_names[f] if f < len(feat_names) else f"feat_{f}" for f in all_feats]
        heat = np.array([
            [np.mean(bin_accum[lbl][f]) if bin_accum[lbl].get(f) else 0.0 for lbl in labels]
            for f in all_feats
        ])

        heatmap_path = os.path.join(save_dir, f"feat_attr_by_depth_{node_type}.png")
        self.plot_feature_depth_heatmap(
            feat_labels, labels, heat, heatmap_path,
            f"{node_type} feature LOO importance by prefix depth (mean |shift|, hours)",
        )

        rows = []
        for lbl in labels:
            for f, feat_label in zip(all_feats, feat_labels):
                vals = bin_accum[lbl].get(f, [])
                rows.append({
                    'depth_bin': lbl, 'feature': feat_label,
                    'mean_abs_shift_hours': round(float(np.mean(vals)), 6) if vals else 0.0,
                    'n_samples': len(vals),
                })
        csv_path = os.path.join(save_dir, f"feat_attr_by_depth_{node_type}.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        print(f"  Saved feature-by-depth LOO importance to: {csv_path}")

        return {'labels': labels, 'features': feat_labels, 'heat': heat}

    # ------------------------------------------------------------------
    # Counterfactual explanations
    # ------------------------------------------------------------------

    def _graph_dissimilarity(self, g1, g2):
        """Return (total_dissimilarity, components_dict) between two trace graphs.

        Four components, each in [0, 1]:
          feat   — seed-centric per-type feature distance (L2 + cosine average)
          type   — multiset Jaccard distance over node-type counts
          edge   — multiset Jaccard distance over edge-type counts
          struct — normalized absolute difference in total edge count
        Total = sum of the four components, in [0, 4].
        """
        # D_feat — seed-centric, matching Zhai et al.'s actual formula (each of g1's own
        # nodes of a type is treated as a "seed" and compared against every one of g2's
        # nodes of that type, then averaged over g1's nodes) rather than pre-pooling both
        # sides to per-type means. For the viewpoint type (exactly 1 node per graph) this
        # reduces to the original single-seed formula; for multi-instance types (Items,
        # Events, ...) it avoids diluting outlier nodes on the query side into one mean.
        feat_scores = []
        for nt in g1.node_types:
            if nt not in g2.node_types:
                continue
            x1, x2 = g1[nt].x, g2[nt].x
            if x1.size(0) == 0 or x2.size(0) == 0:
                continue
            x2_norm_mean = x2.norm(dim=1).mean()
            x1_norms = x1.norm(dim=1)
            diff_norms = (x2.unsqueeze(0) - x1.unsqueeze(1)).norm(dim=2)  # [N1, N2]
            l2_per_seed = diff_norms.mean(dim=1) / (x2_norm_mean + x1_norms + 1e-8)  # [N1]

            cos_sim = F.cosine_similarity(x1.unsqueeze(1), x2.unsqueeze(0), dim=2)  # [N1, N2]
            cos_dist_per_seed = 1.0 - (cos_sim.mean(dim=1) + 1.0) / 2.0  # [N1]

            per_seed_dist = (l2_per_seed + cos_dist_per_seed) / 2.0  # [N1]
            feat_scores.append(per_seed_dist.mean().item())
        d_feat = sum(feat_scores) / len(feat_scores) if feat_scores else 0.0

        # D_type: multiset Jaccard over raw node-type counts
        all_ntypes = set(g1.node_types) | set(g2.node_types)
        c1 = {t: (g1[t].x.size(0) if t in g1.node_types else 0) for t in all_ntypes}
        c2 = {t: (g2[t].x.size(0) if t in g2.node_types else 0) for t in all_ntypes}
        n_inter = sum(min(c1[t], c2[t]) for t in all_ntypes)
        n_union = sum(max(c1[t], c2[t]) for t in all_ntypes)
        d_type = 1.0 - n_inter / n_union if n_union > 0 else 0.0

        # D_edge: multiset Jaccard over raw edge-type counts
        all_etypes = set(g1.edge_types) | set(g2.edge_types)
        e1 = {et: (g1[et].edge_index.size(1) if et in g1.edge_types else 0) for et in all_etypes}
        e2 = {et: (g2[et].edge_index.size(1) if et in g2.edge_types else 0) for et in all_etypes}
        e_inter = sum(min(e1[et], e2[et]) for et in all_etypes)
        e_union = sum(max(e1[et], e2[et]) for et in all_etypes)
        d_edge = 1.0 - e_inter / e_union if e_union > 0 else 0.0

        # D_struct: normalized total edge count difference
        E1 = sum(g1[et].edge_index.size(1) for et in g1.edge_types)
        E2 = sum(g2[et].edge_index.size(1) for et in g2.edge_types)
        d_struct = abs(E1 - E2) / max(E1, E2, 1)

        total = d_feat + d_type + d_edge + d_struct
        return total, {'feat': d_feat, 'type': d_type, 'edge': d_edge, 'struct': d_struct}

    def _locate_test_graph(self, order_id, n_events=None):
        """Locate a single test graph for order_id.

        n_events=None: match the last recorded prefix (last_event==True) -- today's
            default query point.
        n_events=<int>: match the prefix with exactly that many Events nodes, so a
            counterfactual analysis can be run on an earlier, non-last-event stage.
        Raises ValueError (with the order's actually-available prefix lengths, for
        the n_events=<int> case) rather than returning None on no match.
        """
        vp = self.kpi_viewpoint

        if n_events is None:
            for g in self.test_data:
                if g[vp]['last_event'][0].item() and g[vp]['id'][0].item() == order_id:
                    return g
            raise ValueError(f"Order ID {order_id} with last_event=True not found in test data.")

        available = []
        for g in self.test_data:
            if g[vp]['id'][0].item() != order_id:
                continue
            n = g['Events'].x.size(0) if 'Events' in g.node_types else 0
            available.append(n)
            if n == n_events:
                return g
        if available:
            raise ValueError(
                f"Order ID {order_id} has no prefix with exactly {n_events} events. "
                f"Available prefix lengths: {sorted(available)}"
            )
        raise ValueError(f"Order ID {order_id} not found in test data.")

    def find_counterfactuals(self, order_id, target_band='opposite', n_results=3,
                              min_candidates=5, n_events=None, min_gap_hours=0.0,
                              direction='lower'):
        """Find the n_results most similar test traces with a contrasting predicted outcome.

        target_band: 'opposite' (default) — a quartile-based band computed from the
                     candidate pool and query_pred, whose side is controlled by
                     direction (below); or an explicit (low_s, high_s) tuple in
                     seconds, in which case direction is ignored (the tuple already
                     fully specifies the band).
        direction: 'lower' (default, preserves prior behavior exactly) or 'higher' --
                     only meaningful when target_band == 'opposite'. 'lower': traces
                     below Q1, or below the query's own value if the query itself is
                     already in the fastest quartile (today's original, only
                     behavior). 'higher': the mirror image -- traces above Q3, or
                     above the query's own value if the query itself is already in
                     the slowest quartile. Raises ValueError if not 'lower'/'higher'.
        min_candidates: minimum pool size before the length window is widened.
        n_events: None (default) queries the order's last recorded prefix, exactly as
                     before. An int queries the prefix with exactly that many Events
                     nodes -- the reference/candidate population is then depth-matched
                     by absolute event count instead of by last_event status, so an
                     early partial prefix is never compared against a population
                     dominated by fully-recorded (last-event) traces.
        min_gap_hours: minimum |query_predicted_hours - candidate_predicted_hours| a
                     candidate must have to be eligible at all (default 0.0 -- no
                     minimum, preserves prior behavior exactly). A HARD filter: never
                     relaxed by the depth-window-widening loop or the last-resort
                     skip-length-gate fallback below, so a too-strict threshold can
                     legitimately return fewer than n_results, or none, rather than
                     silently substituting a below-threshold candidate. Composes with
                     target_band/direction as an independent, additional constraint
                     (not tied to the 'opposite' branch specifically) -- orthogonal
                     to, and strictly stronger than, the direction-specific strict
                     inequality check.
        """
        if direction not in ('lower', 'higher'):
            raise ValueError(f"direction must be 'lower' or 'higher', got {direction!r}")
        # Note: previously rebuilt self.model from a legacy `_arch.json` sidecar here,
        # bypassing the model_params.json-driven architecture every other explain_*
        # method relies on (_load_params() already prioritizes model_params.json over
        # _arch.json -- self.model, built once in __init__, already reflects that
        # priority correctly). Removed 2026-07-13: rebuilding from a possibly-stale
        # _arch.json risked a shape-mismatch crash or a silently-substituted model
        # after a future retrain, and made self.model rebindable mid-session, which
        # is also what made cached PyG/GNNExplainer explainer objects a staleness risk
        # elsewhere in this class. Verified this fix doesn't change any currently-cited
        # counterfactual output (dissimilarity is model-independent; predictions were
        # already identical since _arch.json and model_params.json happened to agree).
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        vp = self.kpi_viewpoint
        query_graph = self._locate_test_graph(order_id, n_events)
        query_pred = self._predict_value_for_graph(query_graph, 0)
        query_oid = order_id

        def depth(g):
            return g['Events'].x.size(0) if 'Events' in g.node_types else 1

        n_q = depth(query_graph)

        min_gap_s = min_gap_hours * 3600.0

        def band_and_candidates(pool, preds):
            """Given a candidate pool + its predictions, compute the 'opposite' band
            over that pool (mirrored by direction) and return the filtered
            (graph, pred) candidates."""
            import numpy as np
            q1, _q2, q3 = np.percentile(preds, [25, 50, 75]).tolist()
            if target_band == 'opposite':
                if direction == 'lower':
                    low, high = float('-inf'), (q1 if query_pred > q1 else query_pred)
                else:
                    low, high = (q3 if query_pred < q3 else query_pred), float('inf')
            else:
                low, high = target_band
            return [
                (g, p) for g, p in zip(pool, preds)
                if g[vp]['id'][0].item() != query_oid and low <= p <= high
                and (target_band != 'opposite'
                     or (p < query_pred if direction == 'lower' else p > query_pred))
                and abs(query_pred - p) >= min_gap_s
            ]

        if n_events is None:
            # ---- Unchanged: identical to the original last-event-only algorithm ----
            last_event_graphs = [g for g in self.test_data if g[vp]['last_event'][0].item()]
            all_preds = [self._predict_value_for_graph(g, 0) for g in last_event_graphs]
            candidates_all = band_and_candidates(last_event_graphs, all_preds)

            window = max(2.0, 0.2 * n_q)
            filtered = []
            for _ in range(4):   # initial attempt + 3 doublings
                filtered = [(g, pred) for g, pred in candidates_all
                            if abs(depth(g) - n_q) <= window]
                if len(filtered) >= min_candidates:
                    break
                window *= 2

            if not filtered:
                filtered = candidates_all  # last-resort: skip length gate entirely
                window = float('inf')
        else:
            # ---- Depth-first pass: build the window-matched pool FIRST (from ALL
            # prefixes, any last_event status), then compute quartiles/band WITHIN
            # that same pool -- never against a depth-mismatched population. ----
            min_pool_for_quantiles = max(20, 4 * min_candidates)
            window = max(2.0, 0.2 * n_q)
            filtered = []
            for _ in range(4):
                pool = [g for g in self.test_data if abs(depth(g) - n_q) <= window]
                if len(pool) >= min_pool_for_quantiles:
                    preds = [self._predict_value_for_graph(g, 0) for g in pool]
                    filtered = band_and_candidates(pool, preds)
                    if len(filtered) >= min_candidates:
                        break
                window *= 2

            if not filtered:
                # Give up the length gate entirely -- but "everyone" here must mean
                # every prefix of every other order (any depth), never last-event-only,
                # or we reintroduce the depth-mismatch bug this feature exists to fix.
                all_preds = [self._predict_value_for_graph(g, 0) for g in self.test_data]
                filtered = band_and_candidates(self.test_data, all_preds)
                window = float('inf')

        # Rank by graph dissimilarity, then dedupe to each candidate order's single
        # best (lowest-dissimilarity) prefix -- a no-op when n_events is None (exactly
        # one graph per order there already), but necessary once candidates can be
        # drawn from multiple prefixes of the same other order.
        scored = []
        for g, pred in filtered:
            total, comps = self._graph_dissimilarity(query_graph, g)
            scored.append({
                'order_id': int(g[vp]['id'][0].item()),
                'predicted_hours': pred / 3600.0,
                'dissimilarity': total,
                'n_events': g['Events'].x.size(0) if 'Events' in g.node_types else 0,
                'length_window_used': window,
                'components': comps,
                'graph': g,
            })
        scored.sort(key=lambda r: r['dissimilarity'])

        best_per_order = {}
        for r in scored:
            best_per_order.setdefault(r['order_id'], r)
        results = sorted(best_per_order.values(), key=lambda r: r['dissimilarity'])

        return results[:n_results]

    def explain_counterfactual(self, order_id, target_band='opposite', n_results=3,
                                min_candidates=5, n_events=None, min_gap_hours=0.0,
                                direction='lower'):
        """Print counterfactual comparison for a given order and save a node-type bar chart.

        n_events: None (default) explains the order's last recorded prefix; an int
                     explains the prefix with exactly that many Events nodes.
        min_gap_hours: minimum |query - candidate| predicted-hours gap a counterfactual
                     must have to be eligible (default 0.0 -- no minimum). A hard filter:
                     see find_counterfactuals()'s docstring. Too strict a value can yield
                     fewer than n_results, or none.
        direction: 'lower' (default) or 'higher' -- which side of the query's own
                     prediction to search when target_band == 'opposite'; see
                     find_counterfactuals()'s docstring. Ignored for an explicit
                     target_band tuple.
        """
        results = self.find_counterfactuals(order_id, target_band, n_results,
                                             min_candidates, n_events, min_gap_hours,
                                             direction)

        query_graph = self._locate_test_graph(order_id, n_events)
        query_pred = self._predict_value_for_graph(query_graph, 0)
        n_q = query_graph['Events'].x.size(0) if 'Events' in query_graph.node_types else '?'

        print(f"\n{'=' * 60}")
        print(f"Counterfactual Explanation for {self.kpi_viewpoint} #{order_id}")
        print(f"  Query: {round(query_pred / 3600)}h predicted | prefix length: {n_q} events")
        print(f"  Graph: " +
              ", ".join(f"{nt}={query_graph[nt].num_nodes}" for nt in query_graph.node_types))

        if not results:
            if min_gap_hours > 0:
                print(f"  No counterfactuals found with a predicted-time gap "
                      f"≥ {min_gap_hours:g}h.")
            else:
                print("  No counterfactuals found.")
            print('=' * 60)
            return results

        print(f"\n  Top {len(results)} counterfactual(s) [target band: "
              f"{f'opposite quartile ({direction})' if target_band == 'opposite' else str(target_band)}]:")

        for i, r in enumerate(results, 1):
            win_str = (f"{r['length_window_used']:.0f}" if r['length_window_used'] != float('inf')
                       else "∞ (fallback)")
            print(f"\n  CF #{i} — Order #{r['order_id']}")
            print(f"    Predicted : {r['predicted_hours']:.1f}h | "
                  f"prefix: {r['n_events']} events | "
                  f"window used: ±{win_str}")
            print(f"    Total dissimilarity: {r['dissimilarity']:.4f}  "
                  f"(feat={r['components']['feat']:.3f}, "
                  f"type={r['components']['type']:.3f}, "
                  f"edge={r['components']['edge']:.3f}, "
                  f"struct={r['components']['struct']:.3f})")

            all_ntypes = sorted(set(list(query_graph.node_types) + list(r['graph'].node_types)))
            rows = []
            for nt in all_ntypes:
                n1 = query_graph[nt].x.size(0) if nt in query_graph.node_types else 0
                n2 = r['graph'][nt].x.size(0) if nt in r['graph'].node_types else 0
                delta = n2 - n1
                rows.append(f"      {nt:<12} {n1:>3} → {n2:>3}  ({delta:+d})")
            print("    Node counts (query → CF):")
            print('\n'.join(rows))

        # Save bar chart for top CF. Namespace partial-prefix runs by n_events so they
        # never clobber an existing last-event run's output directory for this order.
        suffix = f"_ev{n_events}" if n_events is not None else ""
        save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}{suffix}_cf")
        os.makedirs(save_dir, exist_ok=True)
        self._plot_cf_node_comparison(query_graph, results[0]['graph'], order_id,
                                      results[0]['order_id'], save_dir)
        self._plot_cf_dissimilarity_breakdown(results[0], order_id, save_dir)
        id_map_q = self._decode_all_identifiers(query_graph, order_id, n_events)
        id_map_cf = self._decode_all_identifiers(
            results[0]['graph'], results[0]['order_id'], results[0]['n_events']
        )
        self._plot_cf_graph_structures(query_graph, results[0]['graph'], order_id,
                                       results[0]['order_id'], query_pred / 3600.0,
                                       results[0]['predicted_hours'], save_dir,
                                       id_map_q=id_map_q, id_map_cf=id_map_cf)
        self._plot_cf_event_type_diff(query_graph, results[0]['graph'], order_id,
                                      results[0]['order_id'], save_dir)
        self._plot_cf_viewpoint_feature_diff(query_graph, results[0]['graph'], order_id,
                                             results[0]['order_id'], save_dir)

        import pandas as pd
        pd.DataFrame([
            {'rank': i, 'order_id': r['order_id'], 'predicted_hours': r['predicted_hours'],
             'feat': r['components']['feat'], 'type': r['components']['type'],
             'edge': r['components']['edge'], 'struct': r['components']['struct'],
             'total': r['dissimilarity']}
            for i, r in enumerate(results, 1)
        ]).to_csv(os.path.join(save_dir, "cf_dissimilarity.csv"), index=False)

        print(f"\n  Plot saved to: {save_dir}")
        print('=' * 60)
        return results

    def explain_aggregate_counterfactuals(self, n_traces=50, target_band='opposite',
                                           save_dir=None, min_gap_hours=0.0,
                                           direction='lower'):
        """Aggregate counterfactual retrieval across n_traces query traces -- the
        counterfactual analogue of explain_aggregate()/explain_feature_attribution(),
        closing the "no aggregate counterfactual mode" gap flagged in
        EXPLAINABILITY.md. For each of the first n_traces last-event test graphs (same
        deterministic sampling convention as explain_aggregate()), retrieves the
        single best counterfactual via find_counterfactuals() and aggregates the 4
        dissimilarity components plus the predicted-hours gap across all queries,
        reporting mean +/- std -- answering "what does a counterfactual typically
        look like across this dataset?" rather than one worked example at a time.

        Cost note: find_counterfactuals() recomputes predictions for the entire
        last-event candidate pool on every call (needed to determine the opposite-
        outcome quartile), so this is O(n_traces * pool_size) forward passes, not
        O(n_traces) -- the same default (50) as explain_aggregate() is used here for
        consistency, not because the per-call cost is comparable to LOO's.

        min_gap_hours: minimum |query - candidate| predicted-hours gap required, passed
                     through to find_counterfactuals() (default 0.0 -- no minimum). A
                     hard filter: a query with no candidate clearing the bar is counted
                     as a normal "no counterfactual found" failure, same as any other.
        direction: 'lower' (default) or 'higher', passed through to
                     find_counterfactuals() -- see its docstring. Only meaningful when
                     target_band == 'opposite'.
        """
        import pandas as pd

        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        vp = self.kpi_viewpoint
        last_event_graphs = [g for g in self.test_data if g[vp]['last_event'][0].item()]
        sample = last_event_graphs[:n_traces]
        print(f"\nRunning aggregate counterfactual retrieval on {len(sample)} traces "
              f"(target_band={target_band}, direction={direction})…")

        rows = []
        n_failed = 0
        for i, g in enumerate(sample):
            order_id = int(g[vp]['id'][0].item())
            try:
                results = self.find_counterfactuals(order_id, target_band=target_band, n_results=1,
                                                      min_gap_hours=min_gap_hours, direction=direction)
            except Exception as ex:
                n_failed += 1
                print(f"  [trace failed] order={order_id}: {type(ex).__name__}: {ex}")
                continue
            if not results:
                n_failed += 1
                print(f"  [no counterfactual found] order={order_id}")
                continue
            cf = results[0]
            query_pred_h = self._predict_value_for_graph(g, 0) / 3600.0
            rows.append({
                'query_order_id': order_id,
                'cf_order_id': cf['order_id'],
                'query_predicted_hours': query_pred_h,
                'cf_predicted_hours': cf['predicted_hours'],
                'predicted_hours_gap': query_pred_h - cf['predicted_hours'],
                'dissimilarity_total': cf['dissimilarity'],
                'd_feat': cf['components']['feat'],
                'd_type': cf['components']['type'],
                'd_edge': cf['components']['edge'],
                'd_struct': cf['components']['struct'],
            })

        if not rows:
            print("  No counterfactuals retrieved for any sampled trace -- skipped")
            return None

        df = pd.DataFrame(rows)
        csv_path = os.path.join(save_dir, "aggregate_cf_dissimilarity.csv")
        df.to_csv(csv_path, index=False)

        metric_cols = ['dissimilarity_total', 'd_feat', 'd_type', 'd_edge', 'd_struct',
                       'predicted_hours_gap']
        print(f"\nAggregate counterfactual retrieval (n={len(df)} traces, "
              f"{n_failed} failed/skipped out of {len(sample)} sampled):")
        for c in metric_cols:
            print(f"  {c:22s}: {df[c].mean():.4f} ± {df[c].std():.4f}")

        fig, ax = plt.subplots(figsize=(7, 4))
        comp_cols = ['d_feat', 'd_type', 'd_edge', 'd_struct']
        means = [df[c].mean() for c in comp_cols]
        stds = [df[c].std() for c in comp_cols]
        ax.bar(comp_cols, means, yerr=stds, color="#4C72B0", alpha=0.85, capsize=4)
        ax.set_ylabel("Mean dissimilarity component")
        ax.set_title(f"Aggregate counterfactual dissimilarity breakdown (n={len(df)} traces)")
        ax.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, "aggregate_cf_components.png"), dpi=150)
        plt.close()

        print(f"\nAggregate counterfactual outputs saved to: {save_dir}")

        return df

    def _plot_cf_node_comparison(self, query_graph, cf_graph, query_id, cf_id, save_dir):
        """Side-by-side bar chart of node type counts: query vs. top counterfactual."""
        all_ntypes = sorted(set(list(query_graph.node_types) + list(cf_graph.node_types)))
        counts_q = [query_graph[nt].x.size(0) if nt in query_graph.node_types else 0
                    for nt in all_ntypes]
        counts_cf = [cf_graph[nt].x.size(0) if nt in cf_graph.node_types else 0
                     for nt in all_ntypes]

        x = list(range(len(all_ntypes)))
        w = 0.35
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.bar([i - w / 2 for i in x], counts_q, w,
               label=f"Query (#{query_id})", color="#4C72B0", alpha=0.85)
        ax.bar([i + w / 2 for i in x], counts_cf, w,
               label=f"CF (#{cf_id})", color="#DD8452", alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels(all_ntypes, rotation=30, ha='right', fontsize=9)
        ax.set_ylabel("Node count")
        ax.set_title(f"Node type distribution: Query #{query_id} vs. CF #{cf_id}")
        ax.legend(fontsize=9)
        ax.grid(True, axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, "cf_node_type_comparison.png"), dpi=150)
        plt.close()
        print(f"Saved CF node-type comparison to {save_dir}")

    def _plot_cf_dissimilarity_breakdown(self, top_result, query_id, save_dir):
        """Bar chart of the top counterfactual's 4-component dissimilarity score
        (feat/type/edge/struct) -- shows where the structural vs. feature differences
        between query and CF actually lie, rather than just the total scalar score."""
        comps = top_result['components']
        labels = ['feat', 'type', 'edge', 'struct']
        values = [comps[k] for k in labels]

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(labels, values, color="#4C72B0", alpha=0.85)
        for i, v in enumerate(values):
            ax.text(i, v, f"{v:.3f}", ha='center', va='bottom', fontsize=9)
        ax.set_ylabel("Dissimilarity component score")
        ax.set_title(f"CF dissimilarity breakdown: Query #{query_id} vs. "
                     f"CF #{top_result['order_id']} (total={top_result['dissimilarity']:.3f})")
        ax.grid(True, axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, "cf_dissimilarity_breakdown.png"), dpi=150)
        plt.close()
        print(f"Saved CF dissimilarity breakdown to {save_dir}")

    def _hetero_to_nx(self, graph):
        """Convert a full (unpruned) HeteroData into a plain nx.MultiDiGraph for
        structural visualization only -- no importance weighting, no pruning."""
        import networkx as nx
        G = nx.MultiDiGraph()
        for node_type in graph.node_types:
            num_nodes = graph[node_type].x.size(0)
            for idx in range(num_nodes):
                G.add_node((node_type, idx), node_type=node_type)
        for edge_type in graph.edge_types:
            src_type, rel, dst_type = edge_type
            edge_index = graph[edge_type].edge_index
            for e in range(edge_index.size(1)):
                src, dst = edge_index[:, e].tolist()
                G.add_edge((src_type, src), (dst_type, dst), edge_type=rel)
        return G

    def _draw_hetero_nx(self, G, ax, type_colors, seed_key=None, title="", id_map=None):
        """Draw one full hetero graph onto a given axis -- plain structural view,
        no importance weighting. Independent axes/layouts per subplot (not meant
        to overlay one graph onto the other)."""
        import networkx as nx
        try:
            pos = nx.kamada_kawai_layout(G)
        except Exception as exc:
            print(f"  [layout] kamada_kawai_layout failed ({type(exc).__name__}: {exc}), "
                  f"falling back to spring_layout")
            pos = nx.spring_layout(G, seed=42, k=0.9)

        node_colors = [type_colors.get(attrs['node_type'], 'gray') for _, attrs in G.nodes(data=True)]
        node_sizes = [420 if node == seed_key else 180 for node in G.nodes]
        edgecolors = ['black' if node == seed_key else 'none' for node in G.nodes]
        linewidths = [1.8 if node == seed_key else 0 for node in G.nodes]

        nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colors, node_size=node_sizes,
                                edgecolors=edgecolors, linewidths=linewidths, alpha=0.9)
        nx.draw_networkx_edges(G, pos, ax=ax, edge_color="gray", width=1.0, alpha=0.5,
                                arrows=True, connectionstyle="arc3,rad=0.1")

        if G.number_of_nodes() <= 20:  # keep dense graphs (e.g. #1812) readable
            labels = {
                node: (str(id_map[node]) if id_map and node in id_map
                       else f"{node[0]}[{node[1]}]")
                for node in G.nodes
            }
            nx.draw_networkx_labels(G, pos, ax=ax, labels=labels, font_size=6)

        ax.set_title(title, fontsize=10)
        ax.axis("off")

    def _plot_cf_graph_structures(self, query_graph, cf_graph, query_id, cf_id,
                                   query_hours, cf_hours, save_dir,
                                   id_map_q=None, id_map_cf=None):
        """Side-by-side node-link diagrams of the FULL query and CF graphs --
        complementary to the aggregate bar charts, shows actual topology."""
        palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
                   "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"]
        G_q = self._hetero_to_nx(query_graph)
        G_cf = self._hetero_to_nx(cf_graph)
        all_types = sorted({a['node_type'] for _, a in G_q.nodes(data=True)} |
                            {a['node_type'] for _, a in G_cf.nodes(data=True)})
        type_colors = {nt: palette[i % len(palette)] for i, nt in enumerate(all_types)}

        n_q = query_graph['Events'].x.size(0) if 'Events' in query_graph.node_types else 0
        n_cf = cf_graph['Events'].x.size(0) if 'Events' in cf_graph.node_types else 0

        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        seed_key_q = (self.kpi_viewpoint, 0)
        seed_key_cf = (self.kpi_viewpoint, 0)
        self._draw_hetero_nx(G_q, axes[0], type_colors, seed_key=seed_key_q,
                              title=f"Query #{query_id}\n{n_q} events, {query_hours:.1f}h predicted",
                              id_map=id_map_q)
        self._draw_hetero_nx(G_cf, axes[1], type_colors, seed_key=seed_key_cf,
                              title=f"CF #{cf_id}\n{n_cf} events, {cf_hours:.1f}h predicted",
                              id_map=id_map_cf)

        legend_handles = [plt.Line2D([0], [0], marker="o", color="w", label=nt,
                                      markerfacecolor=c, markersize=10)
                          for nt, c in type_colors.items()]
        fig.legend(handles=legend_handles, loc="lower center",
                   ncol=min(len(all_types), 7), fontsize=9, bbox_to_anchor=(0.5, -0.02))
        fig.suptitle(f"Graph structure comparison: Query #{query_id} vs. CF #{cf_id}", fontsize=12)
        plt.tight_layout(rect=[0, 0.05, 1, 0.95])
        plt.savefig(os.path.join(save_dir, "cf_graph_structure_comparison.png"),
                    dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved CF graph structure comparison to {save_dir}")

    def _decode_event_types(self, graph):
        """Decode each Events node's activity type from the one-hot block of its
        feature vector (layout: [ev_type one-hot | temporal | C3 counts | O1-ext
        counts], see training.py's feature_names['Events'] construction). Features
        are z-score normalized, so this uses argmax over the one-hot block rather
        than an exact ==1 check -- exactly one column is the "hot" one pre-
        normalization, so its per-row argmax location survives normalization.
        Returns a collections.Counter of activity name -> count."""
        from collections import Counter
        counts = Counter()
        if 'Events' not in graph.node_types or graph['Events'].x.size(0) == 0:
            return counts
        fnames = self.feature_names.get('Events', [])
        n_types = fnames.index('elapsed_h') if 'elapsed_h' in fnames else 0
        if n_types == 0:
            return counts
        type_names = fnames[:n_types]
        idx = graph['Events'].x[:, :n_types].argmax(dim=1)
        for i in idx.tolist():
            counts[type_names[i]] += 1
        return counts

    def _decode_event_types_with_indices(self, graph):
        """Same decoding as _decode_event_types(), but returns the Events node
        INDICES belonging to each activity type instead of just counts -- needed to
        zero the right rows for a group-LOO ablation of a specific activity type."""
        from collections import defaultdict
        idx_by_type = defaultdict(list)
        if 'Events' not in graph.node_types or graph['Events'].x.size(0) == 0:
            return idx_by_type
        fnames = self.feature_names.get('Events', [])
        n_types = fnames.index('elapsed_h') if 'elapsed_h' in fnames else 0
        if n_types == 0:
            return idx_by_type
        type_names = fnames[:n_types]
        type_idx = graph['Events'].x[:, :n_types].argmax(dim=1)
        for node_idx, t in enumerate(type_idx.tolist()):
            idx_by_type[type_names[t]].append(node_idx)
        return idx_by_type

    def _decode_node_identifiers(self, graph, node_type):
        """Decode a single non-Events node type's real-world identity (company name,
        department, vehicle id, ...) via argmax over its full feature vector, mirroring
        _decode_event_types_with_indices()'s approach. Only viable for node types listed
        in config's 'encoding' -- those are the only ones training.py populates with real
        identity names in self.feature_names rather than raw numeric attribute names (see
        training.py's Modelling.__init__, ~line 148-180). Returns {} for anything not
        decodable: not encoding-listed, absent from this graph, or collapsed to the
        '{type}_present' fallback training.py uses when an entity has >50 distinct values
        (too many to one-hot -- decoding that fallback would misleadingly print a fake
        specific identity for what's really just a presence flag)."""
        result = {}
        if node_type == 'Events':
            return result
        if node_type not in (self.path_dict.get('encoding') or []):
            return result
        if node_type not in graph.node_types or graph[node_type].x.size(0) == 0:
            return result
        names = self.feature_names.get(node_type, [])
        if not names or names == [f'{node_type}_present']:
            return result
        x = graph[node_type].x
        if x.size(1) != len(names):
            return result
        idx = x.argmax(dim=1)
        for node_idx, i in enumerate(idx.tolist()):
            result[node_idx] = names[i]
        return result

    def _get_ocel_df(self):
        """Lazily-cached ocel.csv, reused across calls -- Modelling.__init__ already reads
        this file once for feature_names construction and caches it as self._ocel_df when
        it runs (training.py, ~line 126); this covers the case where that guard didn't fire
        (e.g. no Events nodes at init) by loading and caching it here on first use instead.
        Never re-read per call -- this file is 20-40MB depending on dataset, and
        _decode_ocel_ids() is called once per node-identifier lookup (every trace explained,
        every aggregate loop iteration)."""
        if not hasattr(self, '_ocel_df'):
            import pandas as pd
            self._ocel_df = pd.read_csv(f"{self.path_dict['graph_output_path']}ocel.csv")
        return self._ocel_df

    def _decode_ocel_ids(self, order_id, n_events=None):
        """Real OCEL_IDs (e.g. 'i-880001'), positionally aligned with each object type's
        node order in the graph -- confirmed empirically against ocel.csv directly: a type's
        '{type}::ids' column is stable/cumulative across an order's events and its list order
        matches '{type}::attributes' (what graph node features are built from) and
        '{type}::idx' exactly. hetero_graphs.py's get_learning_set() never reads '::ids' when
        building the .pt graphs, so this is the only place these real identifiers survive --
        read fresh from ocel.csv here rather than stored on the graph object itself.

        Picks the same prefix-boundary row _locate_test_graph() uses: the last row for this
        order when n_events is None, otherwise the row at that exact event count. Returns
        {(node_type, idx): ocel_id}, covering every object type with a '::ids' column in
        ocel.csv -- discovered dynamically, not a hardcoded type list."""
        import ast as _ast
        ocel_df = self._get_ocel_df()
        rows = ocel_df[ocel_df['vwpnt_id'] == order_id]
        if rows.empty:
            return {}
        row = rows.iloc[-1] if n_events is None else rows.iloc[n_events - 1]

        id_map = {}
        for col in ocel_df.columns:
            if not col.endswith('::ids'):
                continue
            node_type = col[:-len('::ids')]
            ids = _ast.literal_eval(row[col])
            for idx, ocel_id in enumerate(ids):
                id_map[(node_type, idx)] = ocel_id
        return id_map

    def _decode_all_identifiers(self, graph, order_id=None, n_events=None):
        """Combine Events activity-name decoding, every encoding-listed node type's identity
        decoding, and (when order_id is given) real OCEL_IDs for everything else, into one
        {(node_type, node_idx): name} lookup -- the single map every 'top nodes' presentation
        surface (explain_trace, explain_gnn_primary, explain_gnn_primary_aggregate,
        dashboard.py's render_local) needs. Precedence: feature-decoded identities (activity
        name, company name, ...) win where they exist -- they're more human-meaningful than a
        raw database id -- OCEL_IDs only fill in node types with no feature-based identity to
        decode (Items, Products, Packages, Container, TransportDocument, ...). order_id=None
        skips the OCEL_ID layer entirely (falls back to the old encoding/Events-only
        behavior), for any caller that doesn't have an order_id in scope."""
        id_map = {}
        if order_id is not None:
            id_map.update(self._decode_ocel_ids(order_id, n_events))
        for name, idxs in self._decode_event_types_with_indices(graph).items():
            for i in idxs:
                id_map[('Events', i)] = name
        for node_type in (self.path_dict.get('encoding') or []):
            for node_idx, name in self._decode_node_identifiers(graph, node_type).items():
                id_map[(node_type, node_idx)] = name
        return id_map

    def _plot_cf_event_type_diff(self, query_graph, cf_graph, query_id, cf_id, save_dir):
        """Table of Events activity types whose count DIFFERS between query and
        counterfactual -- types both graphs agree on are omitted entirely. Complements
        _plot_cf_node_comparison (which only compares the total Events COUNT) by
        showing which specific activities differ -- e.g. a PaymentReminder present
        in one graph but absent in the other, even when both graphs have the same
        total number of events.

        Also reports, per differing activity type, the LOO predicted-value shift
        from zeroing ONLY that type's one-hot activity-flag feature (not the whole
        node -- feature-level LOO, same mechanism as
        reg_feature_importance_for_node_in_graph(), narrowed to a single named
        feature) across every Events node of that type in the QUERY graph. This is
        deliberately narrower than a whole-node ablation: per the interaction-effect
        finding for order #1781 (whole-node shift +233.13h vs. the one-hot feature
        alone contributing +87.82h, only ~38%), the one-hot-only number isolates
        "how much does the model rely on knowing THIS SPECIFIC activity happened"
        from the rest of that node's features (elapsed_h, waiting_h, C3 counts,
        etc.), which the whole-node number conflates together. Only computable for
        types present in the query graph (nothing to zero out otherwise); rows for
        types absent from the query show a placeholder, not a fabricated 0.0.

        Loads model weights explicitly (like explain_trace()/explain_gnn_subgraph())
        rather than assuming the caller already did -- this method now runs its own
        predictions (the group-ablation shift), not just displaying precomputed
        values, so it can't silently run on an untrained/default-init model the way
        a pure display function safely could."""
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        counts_q = self._decode_event_types(query_graph)
        counts_cf = self._decode_event_types(cf_graph)
        all_types = sorted(set(counts_q) | set(counts_cf))
        differing = [t for t in all_types if counts_q.get(t, 0) != counts_cf.get(t, 0)]

        col_labels = ["Activity", f"Query (#{query_id})", f"CF (#{cf_id})", "Δ", "LOO one-hot Δpred (h)"]
        header_colors = ["#EAEAEA", "#4C72B0", "#DD8452", "#EAEAEA", "#EAEAEA"]

        if not differing:
            fig, ax = plt.subplots(figsize=(6, 1.6))
            ax.axis('off')
            ax.set_title(f"Event-type differences: Query #{query_id} vs. CF #{cf_id}",
                         fontsize=10)
            ax.text(0.5, 0.5, "No activity-type differences", ha='center', va='center',
                    fontsize=10, transform=ax.transAxes)
        else:
            query_idx_by_type = self._decode_event_types_with_indices(query_graph)
            fnames = self.feature_names.get('Events', [])
            n_types = fnames.index('elapsed_h') if 'elapsed_h' in fnames else 0
            type_names = fnames[:n_types]
            baseline_value = self._predict_value_for_graph(query_graph, 0)
            large_shift_threshold = self.target_std.item()

            rows = []
            shifts = []
            for t in differing:
                qv, cv = counts_q.get(t, 0), counts_cf.get(t, 0)
                node_idx = query_idx_by_type.get(t, [])
                if node_idx and t in type_names:
                    col = type_names.index(t)
                    perturbed = query_graph.clone()
                    for i in node_idx:
                        perturbed['Events'].x[i, col] = 0.0
                    pred_perturbed = self._predict_value_for_graph(
                        query_graph, 0, perturbed_graph=perturbed)
                    signed_shift = baseline_value - pred_perturbed
                    shift_str = f"{signed_shift/3600:+.2f}"
                else:
                    signed_shift = None
                    shift_str = "—"
                shifts.append(signed_shift)
                rows.append([t, str(qv), str(cv), f"{cv - qv:+d}", shift_str])

            fig, ax = plt.subplots(figsize=(8.5, 0.55 * len(rows) + 1.2))
            ax.axis('off')
            ax.set_title(f"Event-type differences: Query #{query_id} vs. CF #{cf_id}",
                         fontsize=10)
            table = ax.table(cellText=rows, colLabels=col_labels, loc='center',
                              cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.auto_set_column_width(col=list(range(len(col_labels))))
            table.scale(1, 1.6)

            for col in range(len(col_labels)):
                header_cell = table[0, col]
                header_cell.set_facecolor(header_colors[col])
                header_cell.set_text_props(weight='bold',
                                           color='white' if col in (1, 2) else 'black')
            for r, row in enumerate(rows, start=1):
                delta_cell = table[r, 3]
                delta_cell.set_text_props(color="#A5443A", weight='bold')
                shift = shifts[r - 1]
                shift_cell = table[r, 4]
                if shift is not None and abs(shift) > large_shift_threshold:
                    shift_cell.set_text_props(color="#A5443A", weight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, "cf_event_type_diff.png"), dpi=150,
                    bbox_inches='tight')
        plt.close()

        if differing:
            import pandas as pd
            diff_table = pd.DataFrame([
                {'activity': t, 'query_count': counts_q.get(t, 0), 'cf_count': counts_cf.get(t, 0),
                 'delta': counts_cf.get(t, 0) - counts_q.get(t, 0),
                 'query_loo_onehot_shift_hours': (shifts[i] / 3600.0) if shifts[i] is not None else None}
                for i, t in enumerate(differing)
            ])
            print(f"\nEvent-type differences: Query #{query_id} vs. CF #{cf_id}")
            print(diff_table.to_string(index=False))
            diff_table.to_csv(os.path.join(save_dir, "cf_event_type_diff.csv"), index=False)

        print(f"Saved CF event-type diff to {save_dir}")

    def _plot_cf_viewpoint_feature_diff(self, query_graph, cf_graph, query_id, cf_id, save_dir):
        """Table of the viewpoint (seed) node's own feature values, query vs.
        counterfactual, sorted by |delta| descending. Complements
        _plot_cf_event_type_diff (which only covers Events activity types) by
        showing differences in the query/CF order's OWN attributes (price,
        n_packages, etc.) -- values are the raw z-score-normalized features stored
        in the graph tensors, consistent with how every other feature value is
        displayed elsewhere in this file (e.g. explain_trace()'s printed feature
        values); not denormalized back to raw units."""
        vp = self.kpi_viewpoint
        names = self.feature_names.get(vp, [])
        q_feats = query_graph[vp].x[0]
        cf_feats = cf_graph[vp].x[0]

        rows = []
        for f in range(q_feats.size(0)):
            fname = names[f] if f < len(names) else f"feat_{f}"
            qv, cv = q_feats[f].item(), cf_feats[f].item()
            rows.append([fname, qv, cv, cv - qv])
        rows.sort(key=lambda r: abs(r[3]), reverse=True)

        col_labels = ["Feature", f"Query (#{query_id})", f"CF (#{cf_id})", "Δ"]
        header_colors = ["#EAEAEA", "#4C72B0", "#DD8452", "#EAEAEA"]

        fig, ax = plt.subplots(figsize=(7, 0.55 * len(rows) + 1.2))
        ax.axis('off')
        ax.set_title(f"{vp} feature differences: Query #{query_id} vs. CF #{cf_id}",
                     fontsize=10)
        cell_text = [[fname, f"{qv:.3f}", f"{cv:.3f}", f"{delta:+.3f}"]
                     for fname, qv, cv, delta in rows]
        table = ax.table(cellText=cell_text, colLabels=col_labels, loc='center',
                          cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.6)

        for col in range(len(col_labels)):
            header_cell = table[0, col]
            header_cell.set_facecolor(header_colors[col])
            header_cell.set_text_props(weight='bold',
                                       color='white' if col in (1, 2) else 'black')
        for r in range(1, len(cell_text) + 1):
            table[r, 3].set_text_props(color="#A5443A", weight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, f"cf_{vp.lower()}_feature_diff.png"), dpi=150,
                    bbox_inches='tight')
        plt.close()
        print(f"Saved CF {vp} feature diff to {save_dir}")

    # ── Feature attribution (PyG Explainer + CaptumExplainer) ───────────────────
    # Verified against the previous hand-rolled `captum.attr.InputXGradient(forward_func)`
    # call: identical output (max abs diff 0.0) on the same graphs/weights, so this wrapper
    # replaces it directly and additionally supports IntegratedGradients via the same API.

    _DEPTH_BINS = [(1, 3, '1-3'), (4, 6, '4-6'), (7, 9, '7-9'), (10, 9999, '10+')]

    def _get_pyg_explainer(self, method):
        """Build (and cache) a PyG Explainer wrapping the given Captum attribution method."""
        if not hasattr(self, '_pyg_explainers'):
            self._pyg_explainers = {}
        if method not in self._pyg_explainers:
            algo_kwargs = {'n_steps': 50} if method == 'IntegratedGradients' else {}
            self._pyg_explainers[method] = PyGExplainer(
                model=self.model,
                algorithm=CaptumExplainer(method, **algo_kwargs),
                explanation_type='model',
                node_mask_type='attributes',
                model_config=dict(mode='regression', task_level='node', return_type='raw'),
            )
        return self._pyg_explainers[method]

    def _compute_attribution_for_graph(self, graph, method='InputXGradient'):
        """Run the given Captum attribution method via PyG's Explainer; return signed
        per-type attribution arrays."""
        node_types = list(graph.node_types)
        x_dict = {nt: graph[nt].x for nt in node_types}
        try:
            explanation = self._get_pyg_explainer(method)(x_dict, graph.edge_index_dict, index=0)
        except Exception as exc:
            print(f"    {method} failed: {exc}")
            return {}
        return {nt: explanation.node_mask_dict[nt].detach().cpu().numpy() for nt in node_types}

    def explain_feature_attribution(self, n_traces=None,
                                     methods=('InputXGradient', 'IntegratedGradients'),
                                     depth_stratify=True):
        """Feature attribution aggregated across last-event test graphs, computed for each
        method in `methods` and cross-validated against each other via the top-K/bottom-K
        perturbation fidelity check. If `depth_stratify`, also runs a depth-stratified
        breakdown across ALL test prefixes (see `_explain_attribution_by_depth`)."""
        import numpy as np
        import pandas as pd
        from matplotlib.patches import Patch

        self.model.eval()
        last_event_graphs = [
            g for g in self.test_data if g[self.kpi_viewpoint]['last_event'][0].item()
        ]
        if n_traces is not None:
            last_event_graphs = last_event_graphs[:n_traces]
        n = len(last_event_graphs)

        out_dir = os.path.join(self.path_dict['explainer_path'], 'attribution')
        os.makedirs(out_dir, exist_ok=True)

        method_mean_abs = {}  # method -> {node_type: [F_type] mean-abs array}
        for method in methods:
            # Accumulate per-type signed importance arrays (mean-pooled over nodes per graph)
            accum = {}  # {node_type: list of [F_type] arrays}
            for i, graph in enumerate(last_event_graphs):
                if i % max(1, n // 10) == 0:
                    print(f"  {method} attribution: {100 * i // n}%")
                masks = self._compute_attribution_for_graph(graph, method=method)
                for nt, mask in masks.items():  # mask: [N_type, F_type] signed
                    if mask.shape[0] == 0:
                        continue
                    accum.setdefault(nt, []).append(mask.mean(axis=0))
            print(f"  {method} attribution: 100%")

            # Mean signed and mean absolute per (node_type, dim)
            mean_signed = {nt: np.stack(arrs).mean(axis=0) for nt, arrs in accum.items()}
            mean_abs    = {nt: np.abs(np.stack(arrs)).mean(axis=0) for nt, arrs in accum.items()}
            method_mean_abs[method] = mean_abs

            # Print ranked summary
            print("\n" + "=" * 60)
            print(f"Feature Attribution ({method}) — Dataset-Level")
            print(f"  Graphs analysed: {n}")
            print("=" * 60)
            for nt in sorted(mean_abs):
                scores = mean_abs[nt]
                fnames = self.feature_names.get(nt, [f"feat_{j}" for j in range(len(scores))])
                top_idx = np.argsort(scores)[::-1][:3]
                top_str = ", ".join(
                    f"{fnames[j] if j < len(fnames) else f'feat_{j}'}={scores[j]:.4f}"
                    for j in top_idx
                )
                print(f"  {nt:12s}  top-3: {top_str}")

            suffix = method.lower()

            # Per-type bar charts
            for nt in sorted(mean_abs):
                scores_abs = mean_abs[nt]
                scores_sgn = mean_signed[nt]
                fnames = self.feature_names.get(nt, [f"feat_{j}" for j in range(len(scores_abs))])
                labels = [fnames[j] if j < len(fnames) else f"feat_{j}" for j in range(len(scores_abs))]
                order  = np.argsort(scores_abs)[::-1]
                colors = ['#4C72B0' if scores_sgn[j] >= 0 else '#DD8452' for j in order]

                fig, ax = plt.subplots(figsize=(max(6, len(scores_abs) * 0.45 + 2), 4))
                ax.barh([labels[j] for j in order], scores_abs[order], color=colors, alpha=0.85)
                ax.set_xlabel(f"Mean |{method}| attribution")
                ax.set_title(f"Feature attribution — {nt} nodes ({method})")
                ax.grid(True, axis='x', alpha=0.3)
                ax.legend(handles=[Patch(color='#4C72B0', label='+ (raises prediction)'),
                                    Patch(color='#DD8452', label='− (lowers prediction)')],
                          fontsize=8)
                plt.tight_layout()
                plt.savefig(os.path.join(out_dir, f"ig_{nt.lower()}_importance_{suffix}.png"), dpi=150)
                plt.close()

            # Heatmap across all node types
            all_types = sorted(mean_abs)
            max_dims  = max(len(v) for v in mean_abs.values())
            heat = np.zeros((len(all_types), max_dims))
            for i, nt in enumerate(all_types):
                arr = mean_abs[nt]
                heat[i, :len(arr)] = arr

            fig, ax = plt.subplots(figsize=(max(8, max_dims * 0.5 + 2), len(all_types) + 1))
            im = ax.imshow(heat, aspect='auto', cmap='YlOrRd')
            ax.set_yticks(range(len(all_types)))
            ax.set_yticklabels(all_types)
            ax.set_xlabel("Feature dimension index")
            ax.set_title(f"Feature attribution heatmap (mean |{method}|)")
            plt.colorbar(im, ax=ax, shrink=0.8)
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, f"ig_heatmap_{suffix}.png"), dpi=150)
            plt.close()

            # Signed companion heatmap -- matches Zhai et al. 2025's own heatmap
            # convention (diverging colormap over signed values, not magnitude
            # only). NOT a replacement for the |...| heatmap above: mean_signed
            # is a mean across up to n_traces different orders' per-graph means,
            # so a context-dependent feature (positive effect in some orders,
            # negative in others) can cancel toward zero here while still
            # showing up as large in the abs heatmap -- read the two together,
            # a near-zero signed cell next to a large abs cell is itself a
            # meaningful finding, not a defect. Zero-padded cells (node types
            # with fewer feature dims than max_dims) are masked as NaN/gray
            # rather than left as literal 0.0, since on a diverging colormap
            # 0.0 is visually indistinguishable from "no such feature
            # dimension" -- unlike the sequential |...| heatmap above.
            heat_signed = np.full((len(all_types), max_dims), np.nan)
            for i, nt in enumerate(all_types):
                arr = mean_signed[nt]
                heat_signed[i, :len(arr)] = arr

            cmap_signed = plt.get_cmap('RdBu_r').copy()
            cmap_signed.set_bad('lightgray')

            fig, ax = plt.subplots(figsize=(max(8, max_dims * 0.5 + 2), len(all_types) + 1))
            im = ax.imshow(heat_signed, aspect='auto', cmap=cmap_signed)
            ax.set_yticks(range(len(all_types)))
            ax.set_yticklabels(all_types)
            ax.set_xlabel("Feature dimension index")
            ax.set_title(f"Feature attribution heatmap (mean signed {method})")
            plt.colorbar(im, ax=ax, shrink=0.8)
            if max_dims <= 20:  # keep cell-value annotations legible on narrow heatmaps only
                for i in range(len(all_types)):
                    for j in range(max_dims):
                        val = heat_signed[i, j]
                        if not np.isnan(val):
                            ax.text(j, i, f"{val:.2f}", ha='center', va='center', fontsize=6)
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, f"ig_heatmap_signed_{suffix}.png"), dpi=150)
            plt.close()

            # CSV
            rows = []
            for nt in sorted(mean_abs):
                fnames = self.feature_names.get(nt, [])
                for dim, (s, a) in enumerate(zip(mean_signed[nt], mean_abs[nt])):
                    fname = fnames[dim] if dim < len(fnames) else f"feat_{dim}"
                    rows.append({'node_type': nt, 'feature_dim': dim, 'feature_name': fname,
                                 'mean_signed': round(float(s), 6), 'mean_abs': round(float(a), 6)})
            pd.DataFrame(rows).to_csv(os.path.join(out_dir, f"ig_attribution_{suffix}.csv"), index=False)
            print(f"\nSaved {method} attribution outputs to: {out_dir}")

        # ── Validation: perturbation fidelity (top-K vs bottom-K), per method ───
        print("\n── Perturbation fidelity validation (K=2) ──")

        def _feat_label(nt, dim):
            fnames = self.feature_names.get(nt, [])
            return f"{nt}[{fnames[dim] if dim < len(fnames) else f'feat_{dim}'}]"

        fidelity_summary = []
        for method in methods:
            mean_abs = method_mean_abs[method]
            all_features = [
                (float(score), nt, int(dim))
                for nt in mean_abs
                for dim, score in enumerate(mean_abs[nt])
            ]
            all_features.sort(key=lambda x: x[0], reverse=True)
            K = 2
            top_k = [(nt, dim) for _, nt, dim in all_features[:K]]
            bot_k  = [(nt, dim) for _, nt, dim in all_features[-K:]]

            top_shifts, bot_shifts = [], []
            with torch.no_grad():
                for graph in last_event_graphs:
                    baseline = self._predict_value_for_graph(graph, 0)

                    x_top = {k: v.clone() for k, v in graph.x_dict.items()}
                    for nt, dim in top_k:
                        if nt in x_top and dim < x_top[nt].size(1):
                            x_top[nt][:, dim] = 0.0
                    out_top = self.model(x_top, graph.edge_index_dict)
                    pred_top = (out_top * self.target_std + self.target_mean)[0].item()
                    top_shifts.append(abs(baseline - pred_top))

                    x_bot = {k: v.clone() for k, v in graph.x_dict.items()}
                    for nt, dim in bot_k:
                        if nt in x_bot and dim < x_bot[nt].size(1):
                            x_bot[nt][:, dim] = 0.0
                    out_bot = self.model(x_bot, graph.edge_index_dict)
                    pred_bot = (out_bot * self.target_std + self.target_mean)[0].item()
                    bot_shifts.append(abs(baseline - pred_bot))

            top_mean_h = np.mean(top_shifts) / 3600
            bot_mean_h = np.mean(bot_shifts) / 3600
            gap = top_mean_h - bot_mean_h
            status = "PASS" if top_mean_h > bot_mean_h else "FAIL"
            fidelity_summary.append((method, top_mean_h, bot_mean_h, gap, status))
            print(f"  [{method}] Top-K ({', '.join(_feat_label(nt, d) for nt, d in top_k)}): "
                  f"mean |Δpred| = {top_mean_h:.3f}h")
            print(f"  [{method}] Bot-K ({', '.join(_feat_label(nt, d) for nt, d in bot_k)}): "
                  f"mean |Δpred| = {bot_mean_h:.3f}h")
            print(f"  [{method}] Fidelity check: {status}  "
                  f"(top-K shift {'>' if top_mean_h > bot_mean_h else '<='} bot-K shift, gap={gap:+.3f}h)")

        if len(methods) > 1:
            print("\n  Method comparison (higher gap = more faithful by this project's own check):")
            for method, top_h, bot_h, gap, status in sorted(fidelity_summary, key=lambda r: -r[3]):
                print(f"    {method:20s}  top={top_h:.3f}h  bot={bot_h:.3f}h  gap={gap:+.3f}h  {status}")

        print("=" * 60 + "\n")

        # ── Depth-stratified attribution (OCEL analogue of Zhai et al.'s time-of-day
        #    heatmap), computed across ALL test prefixes, not just last-event graphs.
        #    Covers both the viewpoint type and Events -- the only two node types that
        #    carry non-zero attribution (see EXPLAINABILITY_DEPTH.md) ───
        if depth_stratify:
            self._explain_attribution_by_depth(methods=methods, n_traces=n_traces,
                                                node_type=self.kpi_viewpoint)
            if 'Events' != self.kpi_viewpoint:
                self._explain_attribution_by_depth(methods=methods, n_traces=n_traces,
                                                    node_type='Events')

    def _explain_attribution_by_depth(self, methods=('InputXGradient',), n_traces=None,
                                       node_type=None):
        """Depth-stratified feature attribution across ALL test prefixes (not just
        last-event graphs) — the OCEL analogue of Zhai et al.'s time-of-day heatmap.
        Reuses the same depth bins as compare_models()/baselines.py's depth_mae()."""
        import numpy as np
        import pandas as pd

        node_type = node_type or self.kpi_viewpoint
        graphs = self.test_data if n_traces is None else self.test_data[:n_traces]
        n = len(graphs)
        print(f"\nDepth-stratified attribution: {n} prefixes across all depths "
              f"(vs. last-event-only scope used above — this is slower, one backward "
              f"pass per prefix per method)")

        out_dir = os.path.join(self.path_dict['explainer_path'], 'attribution')
        os.makedirs(out_dir, exist_ok=True)

        for method in methods:
            bin_accum = {lbl: [] for _, _, lbl in self._DEPTH_BINS}
            for i, graph in enumerate(graphs):
                if i % max(1, n // 10) == 0:
                    print(f"  {method} depth attribution: {100 * i // n}%")
                n_events = graph['Events'].x.size(0) if 'Events' in graph.node_types else 0
                lbl = next((l for lo, hi, l in self._DEPTH_BINS if lo <= n_events <= hi), None)
                if lbl is None:
                    continue
                masks = self._compute_attribution_for_graph(graph, method=method)
                mask = masks.get(node_type)
                if mask is None or mask.shape[0] == 0:
                    continue
                bin_accum[lbl].append(np.abs(mask).mean(axis=0))
            print(f"  {method} depth attribution: 100%")

            labels = [lbl for _, _, lbl in self._DEPTH_BINS if bin_accum[lbl]]
            if not labels:
                print(f"  No {node_type} prefixes with data for {method} depth attribution — skipped")
                continue
            fnames = self.feature_names.get(node_type, [])
            heat = np.stack([np.stack(bin_accum[lbl]).mean(axis=0) for lbl in labels])

            heat_t = heat.T  # rows=feature dims, cols=prefix-depth bins
            fig, ax = plt.subplots(figsize=(max(6, len(labels) * 0.9 + 2), heat_t.shape[0] * 0.4 + 2))
            im = ax.imshow(heat_t, aspect='auto', cmap='YlOrRd')
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels)
            ax.set_yticks(range(heat_t.shape[0]))
            feat_labels = [fnames[j] if j < len(fnames) else f"feat_{j}" for j in range(heat_t.shape[0])]
            ax.set_yticklabels(feat_labels, fontsize=8)
            ax.set_xlabel("Prefix depth (n events seen)")
            ax.set_title(f"{node_type} feature attribution by prefix depth (mean |{method}|)")
            plt.colorbar(im, ax=ax, shrink=0.8)
            plt.tight_layout()
            suffix = method.lower()
            plt.savefig(os.path.join(out_dir, f"ig_depth_heatmap_{node_type.lower()}_{suffix}.png"), dpi=150)
            plt.close()

            rows = []
            for lbl in labels:
                arr = np.stack(bin_accum[lbl]).mean(axis=0)
                for dim, val in enumerate(arr):
                    fname = fnames[dim] if dim < len(fnames) else f"feat_{dim}"
                    rows.append({'depth_bin': lbl, 'feature_dim': dim, 'feature_name': fname,
                                 'mean_abs': round(float(val), 6), 'n_prefixes': len(bin_accum[lbl])})
            pd.DataFrame(rows).to_csv(
                os.path.join(out_dir, f"ig_depth_attribution_{node_type.lower()}_{suffix}.csv"), index=False)
            print(f"  Saved {method} depth-stratified attribution to: {out_dir}")

    def compare_to_baselines(self, save_dir=None):
        """Table comparing the currently selected HGT model against every baseline
        this project has (HomoGNN, Mean predictor, GBT) -- CSV + a rendered table
        image, styled like _plot_cf_event_type_diff's table. Each model's metrics
        come from its own already-correct pipeline (hgt_predictions/homo_predictions/
        fresh Mean+GBT fit) rather than a forced per-example merge across models."""
        import baselines as bl
        import pandas as pd

        def _flatten(m_all, m_last, ci_all, ci_last):
            return {'MAE_all': m_all['mae'], 'RMSE_all': m_all['rmse'], 'R2_all': m_all['r2'],
                    'MAE_all_ci_low': ci_all[0], 'MAE_all_ci_high': ci_all[1],
                    'MAE_last': m_last['mae'], 'RMSE_last': m_last['rmse'], 'R2_last': m_last['r2'],
                    'MAE_last_ci_low': ci_last[0], 'MAE_last_ci_high': ci_last[1]}

        if save_dir is None:
            save_dir = f"files/explainer_outputs/{self.database}/validation_{self.cant}"
        os.makedirs(save_dir, exist_ok=True)

        rows = []

        # ── HGT (the currently selected model) ───────────────────────────────
        hgt_df, hgt_pred_time_s = bl.hgt_predictions(self)
        hgt_fit_time_s = bl.read_hgt_fit_time(self)
        last_mask = hgt_df['last_event'].values
        m_all = bl.metrics(hgt_df['true_h'].values, hgt_df['hgt_pred_h'].values)
        m_last = bl.metrics(hgt_df['true_h'].values[last_mask],
                             hgt_df['hgt_pred_h'].values[last_mask])
        ci_all = bl.mae_bootstrap_ci(hgt_df['true_h'].values, hgt_df['hgt_pred_h'].values)
        ci_last = bl.mae_bootstrap_ci(hgt_df['true_h'].values[last_mask],
                                       hgt_df['hgt_pred_h'].values[last_mask])
        rows.append({'Model': 'HGT (ours)', **_flatten(m_all, m_last, ci_all, ci_last),
                     'fit_time_s': hgt_fit_time_s, 'pred_time_s': hgt_pred_time_s})

        # ── HomoGNN, if a checkpoint exists for this database/cant/task ──────
        homo_model_path = self.model_path.replace(".pth", "_homo.pth")
        if os.path.exists(homo_model_path):
            homo_df, homo_pred_time_s = bl.homo_predictions(self)
            homo_fit_time_s = bl.read_homo_fit_time(self)
            last_mask_h = homo_df['last_event'].values
            m_all = bl.metrics(homo_df['true_h'].values, homo_df['homo_pred_h'].values)
            m_last = bl.metrics(homo_df['true_h'].values[last_mask_h],
                                 homo_df['homo_pred_h'].values[last_mask_h])
            ci_all = bl.mae_bootstrap_ci(homo_df['true_h'].values, homo_df['homo_pred_h'].values)
            ci_last = bl.mae_bootstrap_ci(homo_df['true_h'].values[last_mask_h],
                                           homo_df['homo_pred_h'].values[last_mask_h])
            rows.append({'Model': 'HomoGNN (GCN)', **_flatten(m_all, m_last, ci_all, ci_last),
                         'fit_time_s': homo_fit_time_s, 'pred_time_s': homo_pred_time_s})
        else:
            print(f"No HomoGNN checkpoint found at {homo_model_path} -- omitting from table")

        # ── k-dim GNN (HOEG's own architecture), if a checkpoint exists ───────
        kdim_model_path = self.model_path.replace(".pth", "_kdim.pth")
        if os.path.exists(kdim_model_path):
            kdim_df, kdim_pred_time_s = bl.kdim_predictions(self)
            kdim_fit_time_s = bl.read_kdim_fit_time(self)
            last_mask_k = kdim_df['last_event'].values
            m_all = bl.metrics(kdim_df['true_h'].values, kdim_df['kdim_pred_h'].values)
            m_last = bl.metrics(kdim_df['true_h'].values[last_mask_k],
                                 kdim_df['kdim_pred_h'].values[last_mask_k])
            ci_all = bl.mae_bootstrap_ci(kdim_df['true_h'].values, kdim_df['kdim_pred_h'].values)
            ci_last = bl.mae_bootstrap_ci(kdim_df['true_h'].values[last_mask_k],
                                           kdim_df['kdim_pred_h'].values[last_mask_k])
            rows.append({'Model': 'k-dim GNN (HOEG)', **_flatten(m_all, m_last, ci_all, ci_last),
                         'fit_time_s': kdim_fit_time_s, 'pred_time_s': kdim_pred_time_s})
        else:
            print(f"No k-dim GNN checkpoint found at {kdim_model_path} -- omitting from table")

        # ── Mean / GBT (refit fresh, as baselines.py's script already does) ──
        pt_path = self.path_dict['pytorch_path']
        train_df = bl.load_raw_split(f"{pt_path}/train_graphs_sg.pt", self.kpi_viewpoint)
        test_df = bl.load_raw_split(f"{pt_path}/test_graphs_sg.pt", self.kpi_viewpoint)
        # feature columns derived dynamically, not bl.FEAT_COLS -- the viewpoint's raw
        # feature count varies by database (e.g. Orders has 4, TransportDocument has 1)
        feat_cols = [c for c in train_df.columns if c not in ('y_h', 'order_id', 'last_event')]
        X_train, y_train = train_df[feat_cols].values, train_df['y_h'].values
        X_test, y_test = test_df[feat_cols].values, test_df['y_h'].values
        last_mask_t = test_df['last_event'].values

        import time as _time
        mean_pred = bl.MeanPredictor()
        _t0 = _time.time()
        mean_pred.fit(y_train)
        mean_fit_time_s = _time.time() - _t0
        _t0 = _time.time()
        mean_preds = mean_pred.predict(len(test_df))
        mean_pred_time_s = _time.time() - _t0
        m_all = bl.metrics(y_test, mean_preds)
        m_last = bl.metrics(y_test[last_mask_t], mean_preds[last_mask_t])
        ci_all = bl.mae_bootstrap_ci(y_test, mean_preds)
        ci_last = bl.mae_bootstrap_ci(y_test[last_mask_t], mean_preds[last_mask_t])
        rows.append({'Model': 'Mean predictor', **_flatten(m_all, m_last, ci_all, ci_last),
                     'fit_time_s': mean_fit_time_s, 'pred_time_s': mean_pred_time_s})

        gbt = bl.GBTPredictor()
        _t0 = _time.time()
        gbt.fit(X_train, y_train)
        gbt_fit_time_s = _time.time() - _t0
        _t0 = _time.time()
        gbt_preds = gbt.predict(X_test)
        gbt_pred_time_s = _time.time() - _t0
        m_all = bl.metrics(y_test, gbt_preds)
        m_last = bl.metrics(y_test[last_mask_t], gbt_preds[last_mask_t])
        ci_all = bl.mae_bootstrap_ci(y_test, gbt_preds)
        ci_last = bl.mae_bootstrap_ci(y_test[last_mask_t], gbt_preds[last_mask_t])
        rows.append({'Model': 'GBT', **_flatten(m_all, m_last, ci_all, ci_last),
                     'fit_time_s': gbt_fit_time_s, 'pred_time_s': gbt_pred_time_s})

        # ── Paired significance test: HGT vs. each baseline, last-event subset ──
        # compare_to_baselines() only ever reported independent bootstrap CIs per
        # model -- never tested whether HGT's accuracy edge over a given baseline
        # is statistically significant, the same gap validate_fidelity_comparison()
        # (added previously) closed for the LOO-vs-GNNExplainer-primary fidelity
        # comparison. Same methodology here: paired Wilcoxon signed-rank (primary,
        # no normality assumption) + paired t-test (secondary) on the per-example
        # absolute-error difference. Restricted to the last-event subset because
        # order_id is only a safe unique join key there -- the full test set has
        # multiple prefix-rows per order_id, which last_event filters down to at
        # most one -- and last-event is already this project's established primary
        # evaluation slice (MAE_last/R2_last, cited throughout TRAINING_VS_HOEG.md,
        # EXPLAINABILITY_DEPTH.md, and the combined baseline table). No new model
        # inference needed -- reuses the predictions already computed above.
        import numpy as np
        from scipy import stats

        def _bootstrap_ci(diff, n_boot=2000, seed=42):
            rng = np.random.default_rng(seed)
            n = len(diff)
            boot_means = np.array([rng.choice(diff, size=n, replace=True).mean()
                                   for _ in range(n_boot)])
            return float(np.percentile(boot_means, 2.5)), float(np.percentile(boot_means, 97.5))

        hgt_last_df = hgt_df[hgt_df['last_event']][['order_id', 'true_h', 'hgt_pred_h']].copy()
        hgt_last_df['hgt_ae'] = (hgt_last_df['true_h'] - hgt_last_df['hgt_pred_h']).abs()

        baseline_frames = {}
        if os.path.exists(homo_model_path):
            homo_last_df = homo_df[homo_df['last_event']][['order_id', 'homo_pred_h']].copy()
            baseline_frames['HomoGNN (GCN)'] = homo_last_df.rename(columns={'homo_pred_h': 'pred_h'})
        if os.path.exists(kdim_model_path):
            kdim_last_df = kdim_df[kdim_df['last_event']][['order_id', 'kdim_pred_h']].copy()
            baseline_frames['k-dim GNN (HOEG)'] = kdim_last_df.rename(columns={'kdim_pred_h': 'pred_h'})

        test_df_preds = test_df.copy()
        test_df_preds['mean_pred_h'] = mean_preds
        test_df_preds['gbt_pred_h'] = gbt_preds
        test_last_df = test_df_preds[test_df_preds['last_event']]
        baseline_frames['Mean predictor'] = (
            test_last_df[['order_id', 'mean_pred_h']].rename(columns={'mean_pred_h': 'pred_h'}))
        baseline_frames['GBT'] = (
            test_last_df[['order_id', 'gbt_pred_h']].rename(columns={'gbt_pred_h': 'pred_h'}))

        print(f"\nPaired significance test (HGT vs. each baseline, last-event subset, "
              f"negative diff = HGT more accurate):")
        sig_rows = []
        for name, base_df in baseline_frames.items():
            merged = hgt_last_df.merge(base_df, on='order_id', how='inner')
            merged['pred_ae'] = (merged['true_h'] - merged['pred_h']).abs()
            hgt_ae = merged['hgt_ae'].values
            base_ae = merged['pred_ae'].values
            diff = hgt_ae - base_ae
            if np.any(diff != 0):
                _, p_wilcoxon = stats.wilcoxon(hgt_ae, base_ae)
            else:
                p_wilcoxon = float('nan')
            _, p_ttest = stats.ttest_rel(hgt_ae, base_ae)
            ci_low, ci_high = _bootstrap_ci(diff)
            verdict = "significant" if p_wilcoxon < 0.05 else "NOT significant"
            print(f"  vs. {name} (n={len(merged)}): HGT MAE={hgt_ae.mean():.2f}h  "
                  f"{name} MAE={base_ae.mean():.2f}h  diff={diff.mean():+.2f}h "
                  f"[{ci_low:.2f}, {ci_high:.2f}]  Wilcoxon p={p_wilcoxon:.2e}  "
                  f"t-test p={p_ttest:.2e}  -> {verdict} at α=0.05")
            sig_rows.append({
                'baseline': name, 'n_paired': len(merged),
                'hgt_mae': float(hgt_ae.mean()), 'baseline_mae': float(base_ae.mean()),
                'mean_diff': float(diff.mean()), 'ci_low': ci_low, 'ci_high': ci_high,
                'wilcoxon_p': float(p_wilcoxon), 'ttest_p': float(p_ttest),
            })

        sig_df = pd.DataFrame(sig_rows)
        sig_csv_path = os.path.join(save_dir, "baseline_significance.csv")
        sig_df.to_csv(sig_csv_path, index=False)
        print(f"Saved paired significance results to {sig_csv_path}")

        # ── Assemble, save CSV ────────────────────────────────────────────────
        df = pd.DataFrame(rows)
        csv_path = os.path.join(save_dir, "model_comparison.csv")
        df.to_csv(csv_path, index=False)
        print(f"Saved model comparison table to {csv_path}")

        # ── Render table image ────────────────────────────────────────────────
        col_labels = ["Model", "MAE (all) [95% CI]", "RMSE (all)", "R² (all)",
                      "MAE (last) [95% CI]", "RMSE (last)", "R² (last)",
                      "Fit (s)", "Pred (s)"]
        cell_rows = []
        for _, r in df.iterrows():
            cell_rows.append([
                r['Model'],
                f"{r['MAE_all']:.1f} [{r['MAE_all_ci_low']:.1f}, {r['MAE_all_ci_high']:.1f}]",
                f"{r['RMSE_all']:.1f}", f"{r['R2_all']:.3f}",
                f"{r['MAE_last']:.1f} [{r['MAE_last_ci_low']:.1f}, {r['MAE_last_ci_high']:.1f}]",
                f"{r['RMSE_last']:.1f}", f"{r['R2_last']:.3f}",
                f"{r['fit_time_s']:.3f}" if pd.notna(r['fit_time_s']) else "n/a",
                f"{r['pred_time_s']:.3f}" if pd.notna(r['pred_time_s']) else "n/a",
            ])

        fig, ax = plt.subplots(figsize=(13, 0.6 * len(cell_rows) + 1.4))
        ax.axis('off')
        ax.set_title(f"Model comparison — {self.database} (cant={self.cant})", fontsize=10)
        table = ax.table(cellText=cell_rows, colLabels=col_labels, loc='center', cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.6)

        for col in range(len(col_labels)):
            table[0, col].set_facecolor("#EAEAEA")
            table[0, col].set_text_props(weight='bold')
        for r_idx, r in enumerate(df.itertuples(), start=1):
            if r.Model == 'HGT (ours)':
                for col in range(len(col_labels)):
                    table[r_idx, col].set_facecolor("#DCE6F1")
                    table[r_idx, col].set_text_props(weight='bold')

        plt.tight_layout()
        img_path = os.path.join(save_dir, "model_comparison_table.png")
        plt.savefig(img_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved model comparison table image to {img_path}")

        return df

    # ── GNNExplainer-based subgraph explanation ─────────────────────────────────
    # Distinct from the CaptumExplainer path above: GNNExplainer LEARNS a soft node-
    # feature mask and an edge mask via a small per-trace optimization loop (not a
    # one-shot gradient computation), and natively produces edge importance -- which
    # the Captum path never computes (its edge_mask_type is left None).

    def _get_gnn_explainer(self, epochs=200, lr=0.01):
        """Build (and cache) a PyG Explainer wrapping GNNExplainer, keyed by
        (epochs, lr). Mirrors _get_pyg_explainer()'s caching pattern.

        edge_mask_type is deliberately None, not 'object': PyG's set_hetero_masks
        (the mechanism GNNExplainer uses to inject edge masks) walks model.modules()
        looking for an nn.ModuleDict of per-relation MessagePassing submodules to
        patch -- the layout used by a HeteroConv-wrapped model. HGTConv (this
        project's conv layer, model_classes/HGT.py) isn't built that way: it's a
        single MessagePassing whose forward() fuses all relations into one
        propagate() call over a manually-constructed unified bipartite edge index
        (construct_bipartite_edge_index in hgt_conv.py), with per-relation params
        folded into edge_attr via self.p_rel (a ParameterDict), not separate
        submodules. So set_hetero_masks finds nothing to patch for any edge type,
        and GNNExplainer's edge_mask parameters end up disconnected from the
        forward computation -- confirmed empirically: with edge_mask_type='object'
        this raises "Could not compute gradients for edge masks" on the first
        edge type it tries, regardless of which relation that is. Node masking
        works fine because it intercepts x_dict directly rather than relying on
        this per-relation-submodule mechanism. Getting real edge importance out of
        GNNExplainer here would need a custom monkeypatch of HGTConv.message()
        (this project already did exactly that once, to inspect attention weights
        directly -- see EXPLAINABILITY_DEPTH.md) rather than PyG's built-in hook."""
        if not hasattr(self, '_gnn_explainer_cache'):
            self._gnn_explainer_cache = {}
        key = (epochs, lr)
        if key not in self._gnn_explainer_cache:
            self._gnn_explainer_cache[key] = PyGExplainer(
                model=self.model,
                algorithm=GNNExplainer(epochs=epochs, lr=lr),
                explanation_type='model',
                node_mask_type='attributes',
                edge_mask_type=None,
                model_config=dict(mode='regression', task_level='node', return_type='raw'),
            )
        return self._gnn_explainer_cache[key]

    def _run_gnn_explainer(self, gnn_explainer, x_dict, edge_index_dict, index):
        """Run one GNNExplainer explanation with a fixed seed set immediately beforehand,
        so its randomly-initialized mask parameters (torch.randn inside PyG's
        _initialize_node_mask(), drawn from the global RNG) are reproducible -- matching
        training.py's seed=42 convention, which nothing in this file previously extended
        to GNNExplainer. Without this, results are only mostly stable across repeated
        calls (verified empirically: a marginal top-k slot can flip between runs)."""
        torch.manual_seed(42)
        return gnn_explainer(x_dict, edge_index_dict, index=index)

    def _check_gnn_explainer_edges(self, graph, order_id):
        """Raise a clear error if any edge type has zero edges. PyG's
        GNNExplainer._initialize_masks() calls indices.max() on every relation's
        edge_index unconditionally, which crashes with an opaque RuntimeError on a
        legitimately-empty relation (not every trace touches every relation -- e.g.
        a Customer with no directly-linked Employees is common and legitimate).
        The aggregate GNNExplainer-based methods already pre-check and skip such
        traces (compare_loo_gnn_importance_aggregate, explain_gnn_primary_aggregate);
        this gives the single-trace callers (explain_gnn_subgraph, explain_gnn_primary)
        the same clear, catchable failure instead of PyG's internal crash -- caught
        directly by the empty-edge-type demo order dashboard_precompute.py hit."""
        empty_etypes = [et for et in graph.edge_types if graph[et].edge_index.size(1) == 0]
        if empty_etypes:
            raise ValueError(
                f"order={order_id}: {len(empty_etypes)} edge type(s) with zero edges "
                f"(e.g. {empty_etypes[0]}) -- GNNExplainer can't initialize masks for an "
                f"empty relation (PyG limitation, not a code bug). Use explain_trace() "
                f"instead, which has no such restriction."
            )

    def explain_gnn_subgraph(self, order_id, epochs=200, lr=0.01, top_k=5,
                             n_events=None, save_dir=None):
        """Explain a single trace via GNNExplainer's learned node-feature and edge
        masks. n_events=None explains the order's last recorded prefix; an int
        explains the prefix with exactly that many Events nodes (see
        _locate_test_graph), matching explain_counterfactual()'s convention."""
        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}{suffix}_gnn")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        graph = self._locate_test_graph(order_id, n_events)
        self._check_gnn_explainer_edges(graph, order_id)
        x_dict = {nt: graph[nt].x for nt in graph.node_types}
        baseline_value = self._predict_value_for_graph(graph, 0)

        explainer = self._get_gnn_explainer(epochs, lr)
        explanation = self._run_gnn_explainer(explainer, x_dict, graph.edge_index_dict, index=0)

        node_mask_dict = {nt: explanation.node_mask_dict[nt].detach().cpu().numpy()
                          for nt in graph.node_types if nt in explanation.node_mask_dict}
        try:
            raw_edge_mask_dict = explanation.edge_mask_dict
        except KeyError:
            raw_edge_mask_dict = {}
        edge_mask_dict = {et: raw_edge_mask_dict[et].detach().cpu().numpy()
                          for et in graph.edge_types if et in raw_edge_mask_dict}

        import numpy as np
        import pandas as pd

        node_type_importance = {nt: float(np.abs(mask).mean()) for nt, mask in node_mask_dict.items()}
        ranked_types = sorted(node_type_importance.items(), key=lambda kv: kv[1], reverse=True)

        edge_rows = []
        for et, mask in edge_mask_dict.items():
            for e_idx, val in enumerate(mask):
                edge_rows.append((et, e_idx, float(val)))
        edge_rows.sort(key=lambda r: abs(r[2]), reverse=True)

        n_q = graph['Events'].x.size(0) if 'Events' in graph.node_types else '?'
        print(f"\n{'='*60}")
        print(f"GNNExplainer subgraph explanation for {self.kpi_viewpoint} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours "
              f"| prefix length: {n_q} events")
        print(f"\nTop node types by mean |mask|:")
        for rank, (nt, val) in enumerate(ranked_types[:top_k], 1):
            print(f"  {rank}. {nt}: {val:.4f}")

        if edge_rows:
            print(f"\nTop {top_k} edges by |mask|:")
            for rank, (et, e_idx, val) in enumerate(edge_rows[:top_k], 1):
                src, dst = graph[et].edge_index[:, e_idx].tolist()
                print(f"  {rank}. {et[0]}→{et[2]} ({src}→{dst}): mask={val:+.4f}")
        else:
            print(f"\nEdge importance: not available -- HGTConv fuses all relations into a "
                  f"single MessagePassing call, so PyG's set_hetero_masks has no per-relation "
                  f"submodule to inject an edge mask into (see _get_gnn_explainer docstring).")

        node_rows = []
        for nt, mask in node_mask_dict.items():
            per_feat = np.abs(mask).mean(axis=0)
            names = self.feature_names.get(nt, [])
            for f_idx, val in enumerate(per_feat):
                fname = names[f_idx] if f_idx < len(names) else f"feat_{f_idx}"
                node_rows.append({'node_type': nt, 'feature_idx': f_idx,
                                  'feature_name': fname, 'mean_abs_mask': float(val)})
        node_df = pd.DataFrame(node_rows).sort_values('mean_abs_mask', ascending=False)
        node_csv = os.path.join(save_dir, "gnn_node_attribution.csv")
        node_df.to_csv(node_csv, index=False)

        edge_df = pd.DataFrame(
            [{'edge_type': f"{et[0]}->{et[2]}", 'edge_index': e_idx, 'mask_value': val}
             for et, e_idx, val in edge_rows],
            columns=['edge_type', 'edge_index', 'mask_value'],
        )
        edge_csv = os.path.join(save_dir, "gnn_edge_importance.csv")
        edge_df.to_csv(edge_csv, index=False)

        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            "order_id": order_id,
            "predicted_hours": baseline_value / 3600,
            "node_mask_dict": node_mask_dict,
            "edge_mask_dict": edge_mask_dict,
            "save_dir": save_dir,
        }

    def _gnn_node_instance_ranking(self, node_mask_dict):
        """Aggregate GNNExplainer's per-(node, feature) soft mask
        (node_mask_dict[nt]: [n_nodes, n_feats]) down to one score per node INSTANCE
        (mean |mask| across that node's own features), sorted descending. Returns
        [(node_type, idx, score), ...]. Shared by compare_loo_gnn_importance() (rank/
        overlap comparison against LOO) and explain_gnn_primary() (GNNExplainer as
        the primary structural identifier, with LOO reduced to a targeted impact
        estimate over exactly the node instances this ranking selects)."""
        import numpy as np
        gnn_scores = []
        for nt, mask in node_mask_dict.items():
            per_node = np.abs(mask).mean(axis=1)
            for idx, val in enumerate(per_node):
                gnn_scores.append((nt, idx, float(val)))
        gnn_scores.sort(key=lambda r: r[2], reverse=True)
        return gnn_scores

    def _induced_edges(self, graph, included_keys):
        """Real edges in graph where both endpoints are in included_keys (a set of
        (node_type, idx) tuples), in the (edge_type, e_idx, shift, large,
        signed_shift) schema evaluate_explanation_quality()/reg_explanation_subgraph()
        expect, with a placeholder shift=0.0 -- these edges carry no importance
        score of their own (GNNExplainer supplies none on this architecture), they
        exist purely to keep evaluate_explanation_quality()'s Fidelity- ('keep ONLY
        the explanation') subgraph topologically connected instead of edgeless.
        Without this, Fidelity- with edge_importances=[] strips every edge
        regardless of endpoints, so it measures 'can the model run with zero
        edges at all' rather than 'does this node selection reproduce the
        prediction' -- always bad, independent of which nodes were selected."""
        included = set(included_keys)
        induced = []
        for edge_type in graph.edge_types:
            src_type, _, dst_type = edge_type
            edge_index = graph[edge_type].edge_index
            for e in range(edge_index.size(1)):
                src, dst = edge_index[:, e].tolist()
                if (src_type, src) in included and (dst_type, dst) in included:
                    induced.append((edge_type, e, 0.0, False, 0.0))
        return induced

    def compare_loo_gnn_importance(self, order_id, top_k=5, n_events=None,
                                   epochs=200, lr=0.01, save_dir=None):
        """Compare which NODE instances LOO (reg_explanation, signed prediction-shift
        ablation) and GNNExplainer (learned soft attribution mask) each flag as most
        important for a single trace's prediction.

        Node importance only -- edge importance is intentionally excluded from this
        comparison. GNNExplainer's edge masks are disabled for this architecture
        (see _get_gnn_explainer's docstring: HGTConv fuses all relations into one
        MessagePassing call, so PyG's set_hetero_masks has nothing to patch), and the
        separate experimental edge-importance path below reweights post-softmax
        attention rather than performing a true ablation -- explicitly documented
        there as not comparable to LOO's edge ranking. Comparing node importance,
        where both methods are on solid footing, avoids that problem entirely.

        IMPORTANT SCALE CAVEAT: LOO's score is a signed shift in the model's actual
        predicted output (seconds/hours) -- removing this node changes the prediction
        by this much. GNNExplainer's score is an unsupervised soft attribution weight
        in [0, 1] with no such physical interpretation. The two are NOT comparable in
        magnitude -- only in RANK (which nodes each method considers most important).
        This method reports both raw scores for reference but the actual comparison
        is the top-k overlap/rank table, not a magnitude diff.

        Reuses explain_trace() and explain_gnn_subgraph() as-is (including their own
        printed output and saved plots) rather than re-deriving LOO/GNNExplainer from
        scratch -- this method's only new logic is aggregating GNNExplainer's
        per-(node, feature) mask down to one score per node INSTANCE (existing code
        only ever aggregates to per-node-TYPE or per-feature, never per-instance) and
        building the comparison table.
        """
        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}{suffix}_loo_gnn")
        os.makedirs(save_dir, exist_ok=True)

        loo_result = self.explain_trace(order_id, top_k=top_k, n_events=n_events)
        gnn_result = self.explain_gnn_subgraph(order_id, epochs=epochs, lr=lr,
                                               top_k=top_k, n_events=n_events)

        import numpy as np
        import pandas as pd

        # LOO: already a ranked list of (node_type, node_idx, shift, large, signed_shift)
        loo_ranked = loo_result['node_importances']
        loo_rank_map = {(nt, idx): (rank, signed_shift / 3600.0)
                        for rank, (nt, idx, shift, large, signed_shift) in enumerate(loo_ranked, 1)}
        loo_top_keys = [(nt, idx) for nt, idx, *_ in loo_ranked[:top_k]]

        gnn_scores = self._gnn_node_instance_ranking(gnn_result['node_mask_dict'])
        gnn_rank_map = {(nt, idx): (rank, val) for rank, (nt, idx, val) in enumerate(gnn_scores, 1)}
        gnn_top_keys = [(nt, idx) for nt, idx, val in gnn_scores[:top_k]]

        loo_top_set, gnn_top_set = set(loo_top_keys), set(gnn_top_keys)
        overlap = loo_top_set & gnn_top_set

        # Union order: LOO's top-k first (in LOO rank order), then any GNNExplainer
        # top-k nodes not already included (in GNNExplainer rank order).
        union_keys = list(loo_top_keys) + [k for k in gnn_top_keys if k not in loo_top_set]

        rows = []
        for nt, idx in union_keys:
            loo_rank, loo_shift = loo_rank_map.get((nt, idx), (None, None))
            gnn_rank, gnn_score = gnn_rank_map.get((nt, idx), (None, None))
            rows.append({
                'node': f"{nt}[{idx}]",
                'loo_rank': loo_rank if loo_rank is not None else None,
                'loo_signed_shift_hours': loo_shift,
                'gnn_rank': gnn_rank if gnn_rank is not None else None,
                'gnn_score': gnn_score,
                'in_both_top_k': (nt, idx) in overlap,
            })
        table = pd.DataFrame(rows)

        print(f"\n{'='*60}")
        print(f"LOO vs. GNNExplainer node-importance comparison for "
              f"{self.kpi_viewpoint} #{order_id}")
        print(f"  {len(overlap)} of top-{top_k} nodes agree between methods")
        print(f"  NOTE: LOO shift (hours) and GNNExplainer score ([0,1] soft mask) are "
              f"on different scales -- compare RANK/overlap, not magnitude.")
        print(table.to_string(index=False))

        csv_path = os.path.join(save_dir, "loo_gnn_node_comparison.csv")
        table.to_csv(csv_path, index=False)

        # Styled table image, matching the house style established by
        # _plot_cf_event_type_diff / _plot_cf_viewpoint_feature_diff.
        col_labels = ["Node", "LOO rank", "LOO shift (h)", "GNNExp. rank", "GNNExp. score", "Both top-k?"]
        header_colors = ["#EAEAEA", "#4C72B0", "#4C72B0", "#DD8452", "#DD8452", "#EAEAEA"]
        cell_text = []
        for r in rows:
            cell_text.append([
                r['node'],
                str(r['loo_rank']) if r['loo_rank'] is not None else "—",
                f"{r['loo_signed_shift_hours']:+.2f}" if r['loo_signed_shift_hours'] is not None else "—",
                str(r['gnn_rank']) if r['gnn_rank'] is not None else "—",
                f"{r['gnn_score']:.4f}" if r['gnn_score'] is not None else "—",
                "✓" if r['in_both_top_k'] else "",
            ])

        fig, ax = plt.subplots(figsize=(9, 0.55 * len(rows) + 1.4))
        ax.axis('off')
        ax.set_title(f"LOO vs. GNNExplainer node importance: {self.kpi_viewpoint} #{order_id}  "
                     f"({len(overlap)}/{top_k} agree)", fontsize=10)
        gtable = ax.table(cellText=cell_text, colLabels=col_labels, loc='center', cellLoc='center')
        gtable.auto_set_font_size(False)
        gtable.set_fontsize(9)
        gtable.scale(1, 1.6)
        for col in range(len(col_labels)):
            hc = gtable[0, col]
            hc.set_facecolor(header_colors[col])
            hc.set_text_props(weight='bold', color='white' if col in (1, 2, 3, 4) else 'black')
        for r_idx, r in enumerate(rows, start=1):
            if r['in_both_top_k']:
                gtable[r_idx, 5].set_text_props(color="#2E7D32", weight='bold')

        plt.tight_layout()
        png_path = os.path.join(save_dir, "loo_gnn_node_comparison.png")
        plt.savefig(png_path, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"\nSaved {csv_path}")
        print(f"Saved {png_path}")
        print('=' * 60)

        return {
            'order_id': order_id,
            'table': table,
            'overlap_count': len(overlap),
            'save_dir': save_dir,
        }

    def compare_loo_gnn_importance_aggregate(self, n_traces=235, top_k=5, epochs=200,
                                             lr=0.01, save_dir=None):
        """Aggregate version of compare_loo_gnn_importance(): for each of the first
        n_traces last-event test graphs (same deterministic sampling as
        explain_aggregate() -- both draw from the same ordered last-event pool, so
        they cover the identical trace set ONLY when called with the same n_traces;
        the two methods' defaults differ (235 here vs. 50 there), so by default they
        do NOT cover the same set), compute the top-k node-instance overlap between
        LOO and GNNExplainer, then report the mean/std overlap rate and its full
        distribution across traces.

        Uses the LOWER-LEVEL primitives directly (reg_explanation(), and a raw
        _get_gnn_explainer() call mirroring explain_gnn_subgraph()'s internals)
        rather than looping the full compare_loo_gnn_importance()/explain_trace()/
        explain_gnn_subgraph() wrappers, which each save ~15+ plot files per trace
        -- looping those 235 times would produce thousands of redundant files and
        be needlessly slow. This mirrors how explain_aggregate() itself avoids
        explain_trace()'s heavy wrapper for the same reason.

        Node importance only, same scope as the single-trace version (edge
        importance is out of scope -- see compare_loo_gnn_importance's docstring
        for why). Failures are logged, not silently swallowed."""
        import numpy as np
        import pandas as pd

        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate_loo_gnn")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        last_event_graphs = [g for g in self.test_data
                             if g[self.kpi_viewpoint]['last_event'][0].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running aggregate LOO-vs-GNNExplainer comparison on {len(sample)} traces…")

        gnn_explainer = self._get_gnn_explainer(epochs, lr)

        from collections import defaultdict
        type_loo_shifts = defaultdict(list)
        type_gnn_scores = defaultdict(list)

        rows = []
        n_failed = 0
        n_skipped_empty_edges = 0
        for g in sample:
            oid = g[self.kpi_viewpoint]['id'][0].item()
            n_nodes = sum(g[nt].x.size(0) for nt in g.node_types)

            # PyG's GNNExplainer._initialize_masks() calls indices.max() on each
            # relation's edge_index unconditionally, which raises on an EMPTY
            # edge_index (not every trace touches every relation -- e.g. a Customer
            # with no directly-linked Employees is common and legitimate). This is
            # a PyG library limitation, not fixable from here -- skip explicitly
            # with a clear reason rather than letting it surface as a generic
            # RuntimeError caught by the except block below.
            empty_etypes = [et for et in g.edge_types if g[et].edge_index.size(1) == 0]
            if empty_etypes:
                n_skipped_empty_edges += 1
                print(f"  [trace skipped] order={oid}: {len(empty_etypes)} edge type(s) with "
                      f"zero edges (e.g. {empty_etypes[0]}) -- GNNExplainer can't initialize "
                      f"masks for an empty relation (PyG limitation, not a code bug)")
                continue

            try:
                node_imp, _, _, _, _ = self.reg_explanation(g, 0, None, top_k)
                loo_top = {(nt, idx) for nt, idx, *_ in node_imp[:top_k]}
                # Per-type LOO magnitude, ALL nodes not just top-k -- matches
                # explain_aggregate()'s own convention (abs shift, not signed: signed
                # shifts from different traces would partly cancel out in a type-level
                # mean, which is why explain_aggregate() uses magnitude too).
                for nt, idx, shift, _, _ in node_imp:
                    type_loo_shifts[nt].append(shift / 3600.0)

                x_dict = {nt: g[nt].x for nt in g.node_types}
                explanation = self._run_gnn_explainer(gnn_explainer, x_dict, g.edge_index_dict, index=0)
                node_mask_dict = {nt: explanation.node_mask_dict[nt].detach().cpu().numpy()
                                  for nt in g.node_types if nt in explanation.node_mask_dict}
                gnn_scores = []
                for nt, mask in node_mask_dict.items():
                    per_node = np.abs(mask).mean(axis=1)
                    for idx, val in enumerate(per_node):
                        gnn_scores.append((nt, idx, float(val)))
                        type_gnn_scores[nt].append(float(val))
                gnn_scores.sort(key=lambda r: r[2], reverse=True)
                gnn_top = {(nt, idx) for nt, idx, val in gnn_scores[:top_k]}

                overlap = len(loo_top & gnn_top)
                rows.append({'order_id': int(oid), 'n_nodes': n_nodes, 'overlap': overlap})
            except Exception as ex:
                n_failed += 1
                print(f"  [trace failed] order={oid}: {type(ex).__name__}: {ex}")
                continue

        table = pd.DataFrame(rows)
        csv_path = os.path.join(save_dir, "aggregate_loo_gnn_overlap.csv")
        table.to_csv(csv_path, index=False)

        overlaps = table['overlap'].values
        mean_overlap = overlaps.mean()
        std_overlap = overlaps.std()

        print(f"\nAggregate LOO-vs-GNNExplainer overlap (n={len(table)} traces, "
              f"{n_skipped_empty_edges} skipped [empty edge type], "
              f"{n_failed} failed [other], top_k={top_k}):")
        print(f"  Mean overlap: {mean_overlap:.2f}/{top_k}  (σ={std_overlap:.2f})")
        counts = table['overlap'].value_counts().sort_index()
        for k in range(top_k + 1):
            print(f"  {k}/{top_k} agree: {counts.get(k, 0)} traces "
                  f"({100*counts.get(k, 0)/len(table):.1f}%)")

        # Distribution chart -- palette.md sequential blue, matching this session's
        # established chart conventions.
        import matplotlib.pyplot as plt
        dist = [int((table['overlap'] == k).sum()) for k in range(top_k + 1)]
        fig, ax = plt.subplots(figsize=(7, 4.5), facecolor='#fcfcfb')
        ax.set_facecolor('#fcfcfb')
        bars = ax.bar(range(top_k + 1), dist, color='#2a78d6', alpha=0.9, width=0.6)
        for bar, v in zip(bars, dist):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(dist)*0.01,
                        str(v), ha='center', va='bottom', fontsize=9, color='#0b0b0b')
        ax.set_xticks(range(top_k + 1))
        ax.set_xlabel(f"Nodes agreeing (out of top-{top_k})")
        ax.set_ylabel("Number of traces")
        ax.set_title(f"LOO vs. GNNExplainer top-{top_k} node overlap "
                     f"(n={len(table)} traces, mean={mean_overlap:.2f}/{top_k})",
                     fontsize=11, loc='left')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.grid(axis='y', color='#e1e0d9', linewidth=1, zorder=0)
        ax.set_axisbelow(True)
        plt.tight_layout()
        png_path = os.path.join(save_dir, "aggregate_loo_gnn_overlap_distribution.png")
        plt.savefig(png_path, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"\nSaved {csv_path}")
        print(f"Saved {png_path}")

        # ── Top-5 node TYPES by aggregate GNNExplainer importance, with LOO's
        # aggregate magnitude alongside -- node INSTANCES aren't comparable across
        # traces (Events[13] means something different in every graph), so this
        # aggregates at the type level, same granularity as
        # explain_aggregate()'s aggregate_node_type_importance.png, but bringing
        # GNNExplainer alongside LOO in one table/figure like the single-trace
        # compare_loo_gnn_importance() table does.
        type_gnn_mean = {nt: (sum(v) / len(v)) for nt, v in type_gnn_scores.items() if v}
        type_loo_mean = {nt: (sum(v) / len(v)) for nt, v in type_loo_shifts.items() if v}
        gnn_ranked_types = sorted(type_gnn_mean, key=lambda nt: type_gnn_mean[nt], reverse=True)[:top_k]
        loo_ranked_types = sorted(type_loo_mean, key=lambda nt: type_loo_mean[nt], reverse=True)
        loo_rank_of = {nt: r for r, nt in enumerate(loo_ranked_types, 1)}

        type_rows = []
        for rank, nt in enumerate(gnn_ranked_types, 1):
            type_rows.append({
                'node_type': nt,
                'gnn_rank': rank,
                'gnn_mean_score': type_gnn_mean[nt],
                'loo_rank': loo_rank_of.get(nt),
                'loo_mean_abs_shift_hours': type_loo_mean.get(nt),
            })
        type_table = pd.DataFrame(type_rows)
        type_csv_path = os.path.join(save_dir, "aggregate_loo_gnn_by_type.csv")
        type_table.to_csv(type_csv_path, index=False)

        print(f"\nTop {top_k} node types by aggregate GNNExplainer importance "
              f"(n={len(table)} traces):")
        print(type_table.to_string(index=False))

        col_labels = ["Node type", "GNNExp. rank", "GNNExp. mean score",
                      "LOO rank", "LOO mean |shift| (h)"]
        header_colors = ["#EAEAEA", "#DD8452", "#DD8452", "#4C72B0", "#4C72B0"]
        cell_text = [[
            r['node_type'], str(r['gnn_rank']), f"{r['gnn_mean_score']:.4f}",
            str(r['loo_rank']) if r['loo_rank'] is not None else "—",
            f"{r['loo_mean_abs_shift_hours']:.2f}" if r['loo_mean_abs_shift_hours'] is not None else "—",
        ] for r in type_rows]

        fig, ax = plt.subplots(figsize=(8.5, 0.55 * len(type_rows) + 1.4))
        ax.axis('off')
        ax.set_title(f"Top {top_k} node types by aggregate GNNExplainer importance, "
                     f"with LOO's aggregate shift (n={len(table)} traces)", fontsize=10)
        ttable = ax.table(cellText=cell_text, colLabels=col_labels, loc='center', cellLoc='center')
        ttable.auto_set_font_size(False)
        ttable.set_fontsize(9)
        ttable.auto_set_column_width(col=list(range(len(col_labels))))
        ttable.scale(1, 1.6)
        for col in range(len(col_labels)):
            hc = ttable[0, col]
            hc.set_facecolor(header_colors[col])
            hc.set_text_props(weight='bold', color='white' if col in (1, 2, 3, 4) else 'black')
        for r_idx, r in enumerate(type_rows, start=1):
            if r['loo_rank'] is not None and r['loo_rank'] <= top_k:
                ttable[r_idx, 3].set_text_props(color="#2E7D32", weight='bold')
                ttable[r_idx, 4].set_text_props(color="#2E7D32", weight='bold')

        plt.tight_layout()
        type_png_path = os.path.join(save_dir, "aggregate_loo_gnn_by_type.png")
        plt.savefig(type_png_path, dpi=150, bbox_inches='tight')
        plt.close()

        print(f"Saved {type_csv_path}")
        print(f"Saved {type_png_path}")

        return {
            'table': table,
            'type_table': type_table,
            'mean_overlap': mean_overlap,
            'std_overlap': std_overlap,
            'n_failed': n_failed,
            'n_skipped_empty_edges': n_skipped_empty_edges,
            'save_dir': save_dir,
        }

    def validate_fidelity_comparison(self, n_traces=50, top_k=5, epochs=200, lr=0.01, save_dir=None):
        """Paired statistical validation of the LOO-vs-GNNExplainer-primary fidelity
        gap that explain_aggregate() and explain_gnn_primary_aggregate() each already
        report as an independent mean +/- std (e.g. order_management Characterization:
        exhaustive LOO ~0.84 vs. GNNExplainer-primary 0.6845 +/- 0.1817) -- but never
        checked for statistical significance, and never on a genuine per-trace paired
        basis despite both functions already sampling the identical
        last_event_graphs[:n_traces] (same self.test_data ordering). Computes both
        pathways' Fidelity+/-/Characterization for the SAME trace in the SAME pass
        (mirroring compare_loo_gnn_importance_aggregate()'s lower-level-primitives
        pattern above, rather than reconciling two separately-saved CSVs after the
        fact -- aggregate_metrics.csv's LOO rows are keyed by a bare positional
        'trace' index, not order_id, so they can't safely be joined post hoc), then
        runs a paired Wilcoxon signed-rank test (primary -- no normality assumption
        on a bounded/possibly-skewed metric) and a paired t-test (secondary) on the
        per-trace difference for each metric.

        Pairing is naturally limited to the intersection of traces both pathways can
        process: LOO has no restriction, but GNNExplainer can't initialize masks for
        a trace with a legitimately-empty edge type (same PyG limitation guarded by
        _check_gnn_explainer_edges(), reused here instead of a second ad hoc check).
        """
        import numpy as np
        import pandas as pd
        from scipy import stats

        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "fidelity_validation")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        vp = self.kpi_viewpoint
        last_event_graphs = [g for g in self.test_data if g[vp]['last_event'][0].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running paired fidelity validation on {len(sample)} traces…")

        gnn_explainer = self._get_gnn_explainer(epochs, lr)

        rows = []
        n_skipped = 0
        n_failed = 0
        for g in sample:
            oid = int(g[vp]['id'][0].item())
            object_idx = 0

            try:
                self._check_gnn_explainer_edges(g, oid)
            except ValueError:
                n_skipped += 1
                continue

            try:
                baseline_value = self._predict_value_for_graph(g, object_idx)

                # Exhaustive LOO.
                node_imp, edge_imp, _, _, _ = self.reg_explanation(g, object_idx, None, top_k)
                loo_quality = self.evaluate_explanation_quality(
                    g, object_idx, node_imp, edge_imp, node_top_k=10, edge_top_k=15, verbose=False
                )

                # GNNExplainer-primary.
                x_dict = {nt: g[nt].x for nt in g.node_types}
                explanation = self._run_gnn_explainer(gnn_explainer, x_dict, g.edge_index_dict,
                                                       index=object_idx)
                node_mask_dict = {nt: explanation.node_mask_dict[nt].detach().cpu().numpy()
                                  for nt in g.node_types if nt in explanation.node_mask_dict}
                gnn_ranking = self._gnn_node_instance_ranking(node_mask_dict)
                identified_keys = [(nt, idx) for nt, idx, _ in gnn_ranking
                                   if not (nt == vp and idx == object_idx)][:top_k]
                gnn_node_imp = self._loo_shift_for_nodes(g, object_idx, baseline_value, identified_keys)
                included_keys = set(identified_keys) | {(vp, object_idx)}
                induced_edges = self._induced_edges(g, included_keys)
                gnn_quality = self.evaluate_explanation_quality(
                    g, object_idx, gnn_node_imp, induced_edges,
                    node_top_k=top_k, edge_top_k=max(len(induced_edges), 1), verbose=False
                )

                rows.append({
                    'order_id': oid,
                    'loo_characterization': loo_quality['characterization_score'],
                    'gnn_characterization': gnn_quality['characterization_score'],
                    'loo_fidelity_plus': loo_quality['fidelity_plus'],
                    'gnn_fidelity_plus': gnn_quality['fidelity_plus'],
                    'loo_fidelity_minus': loo_quality['fidelity_minus'],
                    'gnn_fidelity_minus': gnn_quality['fidelity_minus'],
                })
            except Exception as ex:
                n_failed += 1
                print(f"  [trace failed] order={oid}: {type(ex).__name__}: {ex}")
                continue

        table = pd.DataFrame(rows)
        csv_path = os.path.join(save_dir, "fidelity_validation_paired.csv")
        table.to_csv(csv_path, index=False)

        print(f"\nPaired fidelity validation (n={len(table)} traces, {n_skipped} skipped "
              f"[empty edge type], {n_failed} failed [other]):")

        def _bootstrap_ci(diff, n_boot=2000, seed=42):
            rng = np.random.default_rng(seed)
            n = len(diff)
            boot_means = np.array([rng.choice(diff, size=n, replace=True).mean()
                                   for _ in range(n_boot)])
            return float(np.percentile(boot_means, 2.5)), float(np.percentile(boot_means, 97.5))

        results = {}
        for metric in ['characterization', 'fidelity_plus', 'fidelity_minus']:
            loo_vals = table[f'loo_{metric}'].values
            gnn_vals = table[f'gnn_{metric}'].values
            diff = loo_vals - gnn_vals
            ci_low, ci_high = _bootstrap_ci(diff)
            # Wilcoxon requires at least one non-zero difference.
            if np.any(diff != 0):
                _, p_wilcoxon = stats.wilcoxon(loo_vals, gnn_vals)
            else:
                p_wilcoxon = float('nan')
            _, p_ttest = stats.ttest_rel(loo_vals, gnn_vals)

            verdict = ("statistically significant" if p_wilcoxon < 0.05
                       else "NOT statistically significant")
            print(f"\n  {metric}:")
            print(f"    LOO mean    : {loo_vals.mean():.4f} ± {loo_vals.std():.4f}")
            print(f"    GNNExp mean : {gnn_vals.mean():.4f} ± {gnn_vals.std():.4f}")
            print(f"    Paired mean diff (LOO - GNNExp): {diff.mean():.4f}  "
                  f"[95% CI {ci_low:.4f}, {ci_high:.4f}]")
            print(f"    Wilcoxon signed-rank p={p_wilcoxon:.2e}   Paired t-test p={p_ttest:.2e}")
            print(f"    -> {verdict} at α=0.05")

            results[metric] = {
                'loo_mean': float(loo_vals.mean()), 'loo_std': float(loo_vals.std()),
                'gnn_mean': float(gnn_vals.mean()), 'gnn_std': float(gnn_vals.std()),
                'mean_diff': float(diff.mean()), 'ci_low': ci_low, 'ci_high': ci_high,
                'wilcoxon_p': float(p_wilcoxon), 'ttest_p': float(p_ttest),
            }

        print(f"\nSaved {csv_path}")
        return {'table': table, 'results': results, 'save_dir': save_dir}

    # ── GNNExplainer as PRIMARY structural identifier, LOO as targeted impact ───
    # estimator. Distinct from compare_loo_gnn_importance() above, which runs both
    # methods independently and compares their rankings -- here GNNExplainer's
    # ranking IS the explanation, and LOO only ever evaluates the specific node
    # instances GNNExplainer flagged (never an exhaustive sweep). Additive: does
    # not replace explain_trace()/explain_aggregate(), which remain the only
    # source of edge importance (GNNExplainer has none on this architecture, see
    # _get_gnn_explainer's docstring) and of full exhaustive-LOO rankings.

    def explain_gnn_primary(self, order_id, top_k=5, epochs=200, lr=0.01,
                            n_events=None, save_dir=None):
        """Explain a single trace with GNNExplainer as the primary identifier of
        important structural elements, and LOO reduced to a targeted impact
        estimate over exactly the node instances GNNExplainer identifies --
        inverting explain_trace()'s roles, where exhaustive LOO does both jobs.

        Node-only scope: GNNExplainer has no edge signal on this architecture --
        use explain_trace() for edge importance.

        n_events: None (default) explains the order's last recorded prefix; an
                     int explains the prefix with exactly that many Events nodes
                     (see _locate_test_graph), matching explain_trace()'s
                     convention.
        """
        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'],
                                    f"order_{order_id}{suffix}_gnnprimary")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        graph = self._locate_test_graph(order_id, n_events)
        self._check_gnn_explainer_edges(graph, order_id)
        object_idx = 0
        baseline_value = self._predict_value_for_graph(graph, object_idx)
        n_q = graph['Events'].x.size(0) if 'Events' in graph.node_types else '?'

        # Decode node indices to a real-world identity (activity name for Events,
        # company/department/vehicle id for any other encoding-listed type), same
        # decoder already used by explain_trace()'s console/CSV output -- reused
        # here rather than duplicated, since these are also "top nodes" tables
        # that previously lacked it.
        id_map = self._decode_all_identifiers(graph, order_id, n_events)

        def _node_label(nt, idx):
            if (nt, idx) in id_map:
                return f"{nt}[{idx}]({id_map[(nt, idx)]})"
            return f"{nt}[{idx}]"

        # GNNExplainer identifies the important node instances.
        x_dict = {nt: graph[nt].x for nt in graph.node_types}
        gnn_explainer = self._get_gnn_explainer(epochs, lr)
        explanation = self._run_gnn_explainer(gnn_explainer, x_dict, graph.edge_index_dict, index=object_idx)
        node_mask_dict = {nt: explanation.node_mask_dict[nt].detach().cpu().numpy()
                          for nt in graph.node_types if nt in explanation.node_mask_dict}
        gnn_ranking = self._gnn_node_instance_ranking(node_mask_dict)
        gnn_score_map = {(nt, idx): score for nt, idx, score in gnn_ranking}
        identified_keys = [(nt, idx) for nt, idx, _ in gnn_ranking
                           if not (nt == self.kpi_viewpoint and idx == object_idx)][:top_k]

        # LOO estimates the impact of exactly these identified nodes -- not an
        # exhaustive sweep over the whole graph.
        node_importances = self._loo_shift_for_nodes(graph, object_idx, baseline_value, identified_keys)
        node_importances.sort(key=lambda t: t[2], reverse=True)

        # Joint impact of masking all identified nodes out together -- reuses
        # evaluate_explanation_quality()'s Fidelity+/- as-is. GNNExplainer has no
        # edge importance signal to feed it, but the real edges among the
        # identified nodes + seed still need to be passed (see _induced_edges'
        # docstring) so Fidelity- reflects "does this node selection reproduce
        # the prediction" rather than "can the model run with zero edges".
        included_keys = set(identified_keys) | {(self.kpi_viewpoint, object_idx)}
        induced_edges = self._induced_edges(graph, included_keys)
        quality = self.evaluate_explanation_quality(
            graph, object_idx, node_importances, induced_edges,
            node_top_k=top_k, edge_top_k=max(len(induced_edges), 1), verbose=False
        )

        seed_feature_importances = self.reg_feature_importance_for_node_in_graph(
            graph, self.kpi_viewpoint, object_idx, baseline_value, object_idx, top_k=top_k
        )
        if identified_keys:
            top_node_type, top_node_idx = identified_keys[0]
            top_node_feature_importances = self.reg_feature_importance_for_node_in_graph(
                graph, top_node_type, top_node_idx, baseline_value, object_idx, top_k=top_k
            )
        else:
            top_node_type, top_node_idx, top_node_feature_importances = None, None, []

        print(f"\n{'='*60}")
        print(f"GNNExplainer-primary explanation for {self.kpi_viewpoint} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours "
              f"| prefix length: {n_q} events")
        print(f"  {len(identified_keys)} node(s) identified by GNNExplainer (epochs={epochs}, "
              f"lr={lr}); shifts below are LOO's TARGETED impact estimate for exactly these "
              f"nodes, not an exhaustive graph-wide ranking (see explain_trace() for that).")

        print(f"\nIdentified nodes (GNNExplainer rank → LOO impact):")
        for rank, (nt, idx, shift, large, signed_shift) in enumerate(node_importances, 1):
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {_node_label(nt, idx)}  gnn_score={gnn_score_map.get((nt, idx), float('nan')):.4f}  "
                  f"shift={signed_shift/3600:+.2f}h{flag}")

        print(f"\nEdge importance: not available in this pathway -- GNNExplainer has no edge "
              f"signal on this architecture (see _get_gnn_explainer docstring). Use "
              f"explain_trace() for edge importance.")

        print(f"\nJoint impact of masking all {len(identified_keys)} identified node(s) together:")
        print(f"  Fidelity+        : {quality['fidelity_plus']:.4f}h")
        print(f"  Fidelity−        : {quality['fidelity_minus']:.4f}h")
        print(f"  Characterization : {quality['characterization_score']:.4f}")
        print(f"  Node sparsity    : {quality['node_sparsity']:.2%}")

        self.plot_node_type_summary(node_importances, os.path.join(save_dir, "node_type_summary.png"))
        if seed_feature_importances:
            self.plot_feature_importances(
                self.kpi_viewpoint, seed_feature_importances,
                os.path.join(save_dir, f"feat_importance_{self.kpi_viewpoint}.png"), order_id=order_id
            )
        if top_node_feature_importances:
            self.plot_feature_importances(
                top_node_type, top_node_feature_importances,
                os.path.join(save_dir, f"feat_importance_{top_node_type}.png"), order_id=order_id
            )
        G = self.reg_explanation_subgraph(graph, object_idx, node_importances, [], node_top_k=top_k)
        self.reg_visualize_explanation_subgraph(
            G, os.path.join(save_dir, "explanation_subgraph.png"), id_map=id_map
        )

        import pandas as pd
        csv_path = os.path.join(save_dir, "gnnprimary_node_importance.csv")
        pd.DataFrame([
            {'rank': r, 'node_type': nt, 'node_idx': idx,
             'identifier': id_map.get((nt, idx), ''),
             'gnn_score': gnn_score_map.get((nt, idx)),
             'loo_signed_shift_hours': signed_shift / 3600.0, 'large_shift': large}
            for r, (nt, idx, shift, large, signed_shift) in enumerate(node_importances, 1)
        ]).to_csv(csv_path, index=False)

        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            'order_id': order_id,
            'predicted_hours': baseline_value / 3600.0,
            'n_events': n_q,
            'identified_keys': identified_keys,
            'node_importances': node_importances,
            'quality': quality,
            'save_dir': save_dir,
        }

    def explain_ig_primary(self, order_id, top_k=5, method='InputXGradient',
                           n_events=None, save_dir=None):
        """Explain a single trace with IG (or another Captum attribution method) as
        the primary identifier of important structural elements, and LOO reduced to
        a targeted impact estimate over exactly the node instances IG identifies --
        the same restricted-LOO pattern explain_gnn_primary() uses for GNNExplainer,
        but with a cheaper identification step: IG is one attribution pass per graph
        vs. GNNExplainer's per-trace optimization loop (see EXPLAINABILITY.md section
        6, "LOO and IG are disconnected" -- this closes that gap). Reuses
        _gnn_node_instance_ranking() unchanged since _compute_attribution_for_graph()
        returns the same {node_type: [n_nodes, n_feats]} shape GNNExplainer's
        node_mask_dict does -- the ranking function is attribution-method-agnostic
        despite its name.

        Node-only scope: like GNNExplainer, this pathway has no edge signal on this
        architecture -- use explain_trace() for edge importance.

        n_events: None (default) explains the order's last recorded prefix; an
                     int explains the prefix with exactly that many Events nodes
                     (see _locate_test_graph), matching explain_trace()'s
                     convention.
        """
        if save_dir is None:
            suffix = f"_ev{n_events}" if n_events is not None else ""
            save_dir = os.path.join(self.path_dict['explainer_path'],
                                    f"order_{order_id}{suffix}_igprimary")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        graph = self._locate_test_graph(order_id, n_events)
        object_idx = 0
        baseline_value = self._predict_value_for_graph(graph, object_idx)
        n_q = graph['Events'].x.size(0) if 'Events' in graph.node_types else '?'

        id_map = self._decode_all_identifiers(graph, order_id, n_events)

        def _node_label(nt, idx):
            if (nt, idx) in id_map:
                return f"{nt}[{idx}]({id_map[(nt, idx)]})"
            return f"{nt}[{idx}]"

        # IG identifies the important node instances.
        attribution = self._compute_attribution_for_graph(graph, method=method)
        ig_ranking = self._gnn_node_instance_ranking(attribution)
        ig_score_map = {(nt, idx): score for nt, idx, score in ig_ranking}
        identified_keys = [(nt, idx) for nt, idx, _ in ig_ranking
                           if not (nt == self.kpi_viewpoint and idx == object_idx)][:top_k]

        # LOO estimates the impact of exactly these identified nodes -- not an
        # exhaustive sweep over the whole graph.
        node_importances = self._loo_shift_for_nodes(graph, object_idx, baseline_value, identified_keys)
        node_importances.sort(key=lambda t: t[2], reverse=True)

        included_keys = set(identified_keys) | {(self.kpi_viewpoint, object_idx)}
        induced_edges = self._induced_edges(graph, included_keys)
        quality = self.evaluate_explanation_quality(
            graph, object_idx, node_importances, induced_edges,
            node_top_k=top_k, edge_top_k=max(len(induced_edges), 1), verbose=False
        )

        seed_feature_importances = self.reg_feature_importance_for_node_in_graph(
            graph, self.kpi_viewpoint, object_idx, baseline_value, object_idx, top_k=top_k
        )
        if identified_keys:
            top_node_type, top_node_idx = identified_keys[0]
            top_node_feature_importances = self.reg_feature_importance_for_node_in_graph(
                graph, top_node_type, top_node_idx, baseline_value, object_idx, top_k=top_k
            )
        else:
            top_node_type, top_node_idx, top_node_feature_importances = None, None, []

        print(f"\n{'='*60}")
        print(f"{method}-primary explanation for {self.kpi_viewpoint} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours "
              f"| prefix length: {n_q} events")
        print(f"  {len(identified_keys)} node(s) identified by {method}; shifts below "
              f"are LOO's TARGETED impact estimate for exactly these nodes, not an "
              f"exhaustive graph-wide ranking (see explain_trace() for that).")

        print(f"\nIdentified nodes ({method} rank → LOO impact):")
        for rank, (nt, idx, shift, large, signed_shift) in enumerate(node_importances, 1):
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {_node_label(nt, idx)}  ig_score={ig_score_map.get((nt, idx), float('nan')):.4f}  "
                  f"shift={signed_shift/3600:+.2f}h{flag}")

        print(f"\nEdge importance: not available in this pathway -- {method} has no edge "
              f"signal in this configuration (node_mask_type='attributes' only, see "
              f"_get_pyg_explainer). Use explain_trace() for edge importance.")

        print(f"\nJoint impact of masking all {len(identified_keys)} identified node(s) together:")
        print(f"  Fidelity+        : {quality['fidelity_plus']:.4f}h")
        print(f"  Fidelity−        : {quality['fidelity_minus']:.4f}h")
        print(f"  Characterization : {quality['characterization_score']:.4f}")
        print(f"  Node sparsity    : {quality['node_sparsity']:.2%}")

        self.plot_node_type_summary(node_importances, os.path.join(save_dir, "node_type_summary.png"))
        if seed_feature_importances:
            self.plot_feature_importances(
                self.kpi_viewpoint, seed_feature_importances,
                os.path.join(save_dir, f"feat_importance_{self.kpi_viewpoint}.png"), order_id=order_id
            )
        if top_node_feature_importances:
            self.plot_feature_importances(
                top_node_type, top_node_feature_importances,
                os.path.join(save_dir, f"feat_importance_{top_node_type}.png"), order_id=order_id
            )
        G = self.reg_explanation_subgraph(graph, object_idx, node_importances, [], node_top_k=top_k)
        self.reg_visualize_explanation_subgraph(
            G, os.path.join(save_dir, "explanation_subgraph.png"), id_map=id_map
        )

        import pandas as pd
        csv_path = os.path.join(save_dir, "igprimary_node_importance.csv")
        pd.DataFrame([
            {'rank': r, 'node_type': nt, 'node_idx': idx,
             'identifier': id_map.get((nt, idx), ''),
             'ig_score': ig_score_map.get((nt, idx)),
             'loo_signed_shift_hours': signed_shift / 3600.0, 'large_shift': large}
            for r, (nt, idx, shift, large, signed_shift) in enumerate(node_importances, 1)
        ]).to_csv(csv_path, index=False)

        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            'order_id': order_id,
            'predicted_hours': baseline_value / 3600.0,
            'n_events': n_q,
            'identified_keys': identified_keys,
            'node_importances': node_importances,
            'quality': quality,
            'save_dir': save_dir,
        }

    def explain_gnn_primary_aggregate(self, n_traces=50, top_k=5, epochs=200,
                                      lr=0.01, save_dir=None):
        """Aggregate version of explain_gnn_primary(): for each of the first
        n_traces last-event test graphs (same deterministic sampling as
        explain_aggregate()), GNNExplainer identifies the top-k important node
        instances and LOO estimates their impact -- individually and jointly
        (Fidelity+/-/characterization) -- reporting mean +/- std across traces.
        Directly comparable to aggregate_metrics.csv's exhaustive-LOO numbers
        from explain_aggregate(), answering whether this cheaper, GNNExplainer-
        driven explanation loses fidelity relative to full LOO.

        Uses the lower-level primitives directly (as
        compare_loo_gnn_importance_aggregate() does) rather than looping
        explain_gnn_primary()'s full wrapper, to avoid writing per-trace plot
        files n_traces times.
        """
        import numpy as np
        import pandas as pd
        from collections import defaultdict

        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate_gnnprimary")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        vp = self.kpi_viewpoint
        last_event_graphs = [g for g in self.test_data if g[vp]['last_event'][0].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running aggregate GNNExplainer-primary explanation on {len(sample)} traces…")

        gnn_explainer = self._get_gnn_explainer(epochs, lr)

        type_counts = defaultdict(int)
        type_shifts = defaultdict(list)
        type_signed_shifts = defaultdict(list)
        # Per-decoded-identity signed shifts (e.g. "Events=PlaceOrder",
        # "Customers=Nordica Systems GmbH", "Items=i-880001"), for
        # plot_aggregate_explanation_bars() -- a finer granularity than
        # type_signed_shifts above, which only aggregates at the node-TYPE level.
        # _decode_all_identifiers() now resolves every real object type via its
        # OCEL_ID (from ocel.csv) when no richer feature-decoded identity exists,
        # so the bare-node-type fallback below is effectively unreachable for any
        # type with a '::ids' column in ocel.csv -- kept as a defensive fallback
        # only (e.g. a node type ocel.csv genuinely has no id list for).
        explanation_signed_shifts = defaultdict(list)
        rows = []
        n_failed = 0
        n_skipped_empty_edges = 0
        for g in sample:
            oid = int(g[vp]['id'][0].item())

            # Same PyG GNNExplainer._initialize_masks() limitation documented in
            # compare_loo_gnn_importance_aggregate(): crashes on a legitimately-
            # empty edge type, regardless of node-vs-edge masking mode.
            empty_etypes = [et for et in g.edge_types if g[et].edge_index.size(1) == 0]
            if empty_etypes:
                n_skipped_empty_edges += 1
                print(f"  [trace skipped] order={oid}: {len(empty_etypes)} edge type(s) with "
                      f"zero edges -- GNNExplainer can't initialize masks for an empty relation")
                continue

            try:
                object_idx = 0
                baseline_value = self._predict_value_for_graph(g, object_idx)
                x_dict = {nt: g[nt].x for nt in g.node_types}
                explanation = self._run_gnn_explainer(gnn_explainer, x_dict, g.edge_index_dict, index=object_idx)
                node_mask_dict = {nt: explanation.node_mask_dict[nt].detach().cpu().numpy()
                                  for nt in g.node_types if nt in explanation.node_mask_dict}
                gnn_ranking = self._gnn_node_instance_ranking(node_mask_dict)
                identified_keys = [(nt, idx) for nt, idx, _ in gnn_ranking
                                   if not (nt == vp and idx == object_idx)][:top_k]

                node_importances = self._loo_shift_for_nodes(g, object_idx, baseline_value, identified_keys)
                id_map = self._decode_all_identifiers(g, oid)
                for nt, idx, shift, large, signed_shift in node_importances:
                    type_counts[nt] += 1
                    type_shifts[nt].append(shift / 3600.0)
                    type_signed_shifts[nt].append(signed_shift / 3600.0)
                    decoded = id_map.get((nt, idx))
                    label = f"{nt}={decoded}" if decoded else nt
                    explanation_signed_shifts[label].append(signed_shift / 3600.0)

                included_keys = set(identified_keys) | {(vp, object_idx)}
                induced_edges = self._induced_edges(g, included_keys)
                quality = self.evaluate_explanation_quality(
                    g, object_idx, node_importances, induced_edges,
                    node_top_k=top_k, edge_top_k=max(len(induced_edges), 1), verbose=False
                )
                rows.append({
                    'order_id': oid,
                    'n_identified': len(identified_keys),
                    'fidelity_plus': quality['fidelity_plus'],
                    'fidelity_minus': quality['fidelity_minus'],
                    'characterization_score': quality['characterization_score'],
                    'node_sparsity': quality['node_sparsity'],
                })
            except Exception as ex:
                n_failed += 1
                print(f"  [trace failed] order={oid}: {type(ex).__name__}: {ex}")
                continue

        table = pd.DataFrame(rows)
        csv_path = os.path.join(save_dir, "aggregate_gnnprimary_metrics.csv")
        table.to_csv(csv_path, index=False)

        print(f"\nAggregate GNNExplainer-primary explanation (n={len(table)} traces, "
              f"{n_skipped_empty_edges} skipped [empty edge type], {n_failed} failed [other], "
              f"top_k={top_k}):")
        print(f"  Fidelity+        : {table['fidelity_plus'].mean():.4f}h ± "
              f"{table['fidelity_plus'].std():.4f}h")
        print(f"  Fidelity−        : {table['fidelity_minus'].mean():.4f}h ± "
              f"{table['fidelity_minus'].std():.4f}h")
        print(f"  Characterization : {table['characterization_score'].mean():.4f} ± "
              f"{table['characterization_score'].std():.4f}")
        print(f"  Node sparsity    : {table['node_sparsity'].mean():.2%}")

        print(f"\nNode-type selection frequency (share of traces' top-{top_k} identified "
              f"by GNNExplainer):")
        type_table_rows = []
        for nt in sorted(type_counts, key=lambda t: type_counts[t], reverse=True):
            shifts = np.array(type_shifts[nt])
            signed_shifts = np.array(type_signed_shifts[nt])
            freq = type_counts[nt] / (len(table) * top_k) if len(table) else float('nan')
            print(f"  {nt:<12} selected {type_counts[nt]:>4}x  ({freq:.1%} of top-{top_k} slots)  "
                  f"mean shift={shifts.mean():.2f}h ± {shifts.std():.2f}h")
            type_table_rows.append({
                'node_type': nt, 'selection_count': type_counts[nt], 'selection_frequency': freq,
                'mean_shift_hours': shifts.mean(), 'std_shift_hours': shifts.std(),
                'mean_signed_shift_hours': signed_shifts.mean(),
            })
        type_table = pd.DataFrame(type_table_rows)
        type_csv_path = os.path.join(save_dir, "aggregate_gnnprimary_type_summary.csv")
        type_table.to_csv(type_csv_path, index=False)

        png_path = os.path.join(save_dir, "aggregate_gnnprimary_type_summary.png")
        bar_rows = [{'node_type': r['node_type'], 'value': r['mean_shift_hours'],
                    'signed_value': r['mean_signed_shift_hours'], 'std': r['std_shift_hours']}
                    for r in type_table_rows]
        self.plot_node_type_bars(
            bar_rows, png_path,
            f"GNNExplainer-identified node impact by type (n={len(table)} traces)",
            "Mean LOO impact if masked (hours)", show_error_bars=True,
            secondary_series=[r['selection_count'] for r in type_table_rows],
            secondary_label="Times selected",
        )

        # Global, ranked 'attr=value' explanation bars -- cf. Galanti et al. 2023b Fig. 1.
        # A finer-grained companion to the node-TYPE-level chart above: same underlying
        # per-trace data, but bucketed by decoded identity (e.g. "Events=PlaceOrder")
        # instead of just node type, in one combined cross-type ranking.
        explanation_csv_path = os.path.join(save_dir, "aggregate_explanation_bars.csv")
        explanation_png_path = os.path.join(save_dir, "aggregate_explanation_bars.png")
        explanation_summary = self.plot_aggregate_explanation_bars(
            explanation_signed_shifts, explanation_png_path,
            "Global explanations for remaining time prediction",
            dataset_label=self.database, n_traces=len(table),
        )
        pd.DataFrame(explanation_summary).to_csv(explanation_csv_path, index=False)

        print(f"\nSaved {csv_path}")
        print(f"Saved {type_csv_path}")
        print(f"Saved {png_path}")
        print(f"Saved {explanation_csv_path}")
        print(f"Saved {explanation_png_path}")

        return {
            'table': table,
            'type_table': type_table,
            'explanation_summary': explanation_summary,
            'n_failed': n_failed,
            'n_skipped_empty_edges': n_skipped_empty_edges,
            'save_dir': save_dir,
        }

    # ── EXPERIMENTAL: real edge importance via GNNExplainer + HGTConv ───────────
    # explain_gnn_subgraph() above disables edge masking because PyG's
    # set_hetero_masks can't discover HGTConv's fused-relation structure (see
    # _get_gnn_explainer's docstring). This attempts real edge importance anyway,
    # by calling PyG's own set_masks() directly on each HGTConv layer -- reusing
    # the proven MessagePassing.explain_message() hook, not a hand-written
    # message() monkeypatch -- plus a hand-rolled training loop mirroring PyG's
    # own GNNExplainer loss/coefficients. This relies on undocumented internal
    # PyG behavior (construct_bipartite_edge_index's flattened edge ordering) and
    # mutates shared model state (conv.explain/_edge_mask) mid-call, so it is kept
    # separate from the production explain_gnn_subgraph() path pending validation
    # against reg_explanation()'s LOO edge ranking. NOTE: the resulting importance
    # is not directly comparable to LOO's -- this hook fires AFTER message()'s
    # internal softmax, so it reweights an edge's already-normalized contribution
    # rather than triggering the attention renormalization a true ablation (LOO's
    # edge-removal loop) would cause.

    def explain_gnn_edge_importance_experimental(self, order_id, epochs=100, lr=0.01,
                                                  top_k=5, n_events=None):
        """EXPERIMENTAL. See module-level comment above and the plan doc for
        the full rationale, caveats, and validation approach."""
        from torch_geometric.explain.algorithm.utils import set_masks, clear_masks

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        graph = self._locate_test_graph(order_id, n_events)
        x_dict = {nt: graph[nt].x for nt in graph.node_types}
        edge_index_dict = graph.edge_index_dict

        # Replicate HGTConv.forward()'s flattened bipartite edge ordering
        # (hgt_conv.py's _construct_src_node_feat + construct_bipartite_edge_index):
        # iterate edge_index_dict.keys() in order, each edge type contributing a
        # contiguous block of its own edges, unchanged internal ordering -- verified
        # directly against the installed PyG 2.8.0 source, not assumed.
        offset_map = []
        for edge_type, eidx in edge_index_dict.items():
            for local_idx in range(eidx.size(1)):
                offset_map.append((edge_type, local_idx))
        total_edges = len(offset_map)
        true_total_edges = sum(eidx.size(1) for eidx in edge_index_dict.values())
        assert total_edges == true_total_edges, (
            f"offset_map size {total_edges} != actual edge count {true_total_edges} "
            f"-- edge_index_dict iteration order changed unexpectedly"
        )

        # Baseline: real unmasked prediction, computed BEFORE any masks are installed.
        baseline_value = self._predict_value_for_graph(graph, 0)

        std = torch.nn.init.calculate_gain('relu') * (2.0 / (2 * total_edges)) ** 0.5
        edge_mask = torch.nn.Parameter(torch.randn(total_edges) * std)
        dummy_edge_index = torch.zeros((2, total_edges), dtype=torch.long)

        convs = list(self.model.convs)
        try:
            for conv in convs:
                set_masks(conv, edge_mask, dummy_edge_index, apply_sigmoid=True)

            # Sanity check (mitigates the "silent misalignment" risk flagged in the
            # plan): with the mask forced near 1.0 (a no-op), the masked prediction
            # should closely match the true unmasked baseline. A large discrepancy
            # here means the offset/ordering replica above is wrong, and the run
            # should abort loudly rather than proceed to train and report bogus
            # importances.
            with torch.no_grad():
                edge_mask.data.fill_(10.0)  # sigmoid(10) ~= 0.99995, effectively a no-op
                sanity_out = self.model(x_dict, edge_index_dict)
                sanity_pred = (sanity_out[0] * self.target_std + self.target_mean).item()
            rel_err = abs(sanity_pred - baseline_value) / max(abs(baseline_value), 1.0)
            assert rel_err < 0.01, (
                f"Sanity check failed: near-no-op mask changed prediction by "
                f"{rel_err:.1%} ({sanity_pred:.1f} vs baseline {baseline_value:.1f}) -- "
                f"edge mask is likely misaligned with the model's internal edge ordering."
            )

            # Fixed seed (matching training.py's seed=42 convention and
            # _run_gnn_explainer()'s fix for the main GNNExplainer path) --
            # this re-randomization is what actually drives the real optimization
            # below, so it's the draw that needs to be reproducible.
            torch.manual_seed(42)
            edge_mask.data = torch.randn(total_edges) * std
            optimizer = torch.optim.Adam([edge_mask], lr=lr)
            EPS = 1e-15
            for _ in range(epochs):
                optimizer.zero_grad()
                out = self.model(x_dict, edge_index_dict)
                pred = out[0] * self.target_std + self.target_mean
                loss = (pred - baseline_value).pow(2).squeeze()
                m = edge_mask.sigmoid()
                loss = loss + 0.005 * m.sum()           # PyG default edge_size coeff
                ent = -m * torch.log(m + EPS) - (1 - m) * torch.log(1 - m + EPS)
                loss = loss + 1.0 * ent.mean()          # PyG default edge_ent coeff
                loss.backward()
                optimizer.step()

            final_mask = edge_mask.sigmoid().detach().cpu().numpy()
        finally:
            for conv in convs:
                clear_masks(conv)

        edge_rows = [(et, local_idx, float(final_mask[flat_idx]))
                     for flat_idx, (et, local_idx) in enumerate(offset_map)]
        edge_rows.sort(key=lambda r: abs(r[2] - 0.5), reverse=True)

        print(f"\n{'='*60}")
        print(f"[EXPERIMENTAL] GNNExplainer edge importance for "
              f"{self.kpi_viewpoint} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours")
        print(f"\nTop {top_k} edges by |mask - 0.5| (distance from a no-op mask):")
        for rank, (et, local_idx, val) in enumerate(edge_rows[:top_k], 1):
            src, dst = graph[et].edge_index[:, local_idx].tolist()
            print(f"  {rank}. {et[0]}→{et[2]} ({src}→{dst}): mask={val:.4f}")
        print('='*60)

        return {
            "order_id": order_id,
            "predicted_hours": baseline_value / 3600,
            "edge_importance": edge_rows,
        }
