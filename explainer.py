import torch
import torch.nn.functional as F
import matplotlib.pyplot as plt
import os
from training import Modelling
from torch_geometric.explain import Explainer as PyGExplainer, CaptumExplainer, GNNExplainer


class Explainer(Modelling):
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

    def reg_visualize_explanation_subgraph(self, G, save_path="explanation_subgraph_regression.png"):
        """Draw regression explanation subgraph with node-size proportional to value shift."""
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

        # Sign color: green = this element's actual value pushed the prediction toward a
        # LONGER remaining time than the population-mean substitute would; red = toward
        # SHORTER. Neutral gray for the seed/connector nodes, where signed_importance isn't
        # meaningful. Kept on a separate visual channel (border color) from large_shift
        # (border linewidth, below) rather than overloading one color for both.
        POS_COLOR, NEG_COLOR, NEUTRAL_COLOR = "#2ca02c", "#d62728", "#888888"

        def sign_color(attrs):
            if attrs.get("is_seed") or attrs.get("is_connector"):
                return NEUTRAL_COLOR
            signed = attrs.get("signed_importance", 0.0)
            if signed > 0:
                return POS_COLOR
            elif signed < 0:
                return NEG_COLOR
            return NEUTRAL_COLOR

        node_colors, node_sizes, edge_colors_outline, node_linewidths, alphas = [], [], [], [], []
        for node, attrs in G.nodes(data=True):
            node_colors.append(type_colors.get(attrs["node_type"], "gray"))
            if attrs.get("is_seed"):
                node_sizes.append(900)
            elif attrs.get("is_connector"):
                node_sizes.append(150)
            else:
                # sqrt scaling (not linear+cap): LOO shifts span orders of magnitude
                # (sub-1h to 280+h in the same trace) -- a linear map either saturates
                # almost everything to one size or needs a cap so high it makes small
                # differences invisible. sqrt compresses large values gracefully while
                # still separating them, so e.g. a 283h and a 75h node stay visually
                # distinct instead of both hitting the same ceiling.
                node_sizes.append(min(250 + 300 * max(attrs.get("importance", 0), 0) ** 0.5, 6000))
            edge_colors_outline.append(sign_color(attrs))
            node_linewidths.append(3.0 if attrs.get("large_shift") else 1.2)
            alphas.append(0.4 if attrs.get("is_connector") else 0.9)

        edge_colors, edge_widths = [], []
        for _, _, attrs in G.edges(data=True):
            edge_colors.append(sign_color(attrs))
            edge_widths.append(min(1 + 1.2 * max(attrs.get("importance", 0), 0) ** 0.5, 14))

        plt.figure(figsize=(10, 8))
        nx.draw_networkx_nodes(
            G, pos, node_color=node_colors, node_size=node_sizes,
            edgecolors=edge_colors_outline, linewidths=node_linewidths, alpha=alphas,
        )
        nx.draw_networkx_edges(
            G, pos, edge_color=edge_colors, width=edge_widths,
            arrows=True, connectionstyle="arc3,rad=0.1", alpha=0.7,
        )
        labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes if not G.nodes[node].get("is_connector")}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=7)

        legend_handles = [
            plt.Line2D([0], [0], marker="o", color="w", label=nt,
                       markerfacecolor=color, markersize=10)
            for nt, color in type_colors.items()
        ]
        legend_handles.append(plt.Line2D([0], [0], color=POS_COLOR, lw=2,
                                         label="border: increases predicted time"))
        legend_handles.append(plt.Line2D([0], [0], color=NEG_COLOR, lw=2,
                                         label="border: decreases predicted time"))
        legend_handles.append(plt.Line2D([0], [0], color="black", lw=3,
                                         label="thick border: large shift (>1 std)"))
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
    # Explainability — visualizations and entry points
    # ------------------------------------------------------------------

    def plot_feature_importances(self, node_type, feature_importances, save_path, order_id=None):
        """Horizontal bar chart of per-feature value shifts for one node. Bar color
        encodes sign (green = this feature's real value pushed the prediction toward
        a longer remaining time than the population-mean substitute would; red =
        toward shorter -- see explain_trace()'s signed_shift docs for the full
        caveat), bar edge (black, thick) flags a >1 std ('large') shift."""
        names = self.feature_names.get(node_type, [])
        feats, shifts, larges, signs = [], [], [], []
        for f, shift, large, signed_shift in feature_importances:
            label = names[f] if f < len(names) else f"feat_{f}"
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
        ax.set_xlabel("Value shift if removed (hours)")
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

    def plot_node_type_summary(self, node_importances, save_path):
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
        total_shifts = [type_shift[t] for t in types]
        counts = [type_count[t] for t in types]
        bar_colors = ["#2ca02c" if type_signed_shift[t] > 0
                      else ("#d62728" if type_signed_shift[t] < 0 else "#888888")
                      for t in types]

        fig, ax1 = plt.subplots(figsize=(8, 4))
        x = range(len(types))
        bars = ax1.bar(x, total_shifts, color=bar_colors, alpha=0.8, label="Total shift (hours)")
        ax1.set_xticks(x)
        ax1.set_xticklabels(types, rotation=30, ha="right", fontsize=9)
        ax1.set_ylabel("Cumulative value shift if type removed (hours)")
        ax1.set_title("Node type importance summary")

        ax2 = ax1.twinx()
        ax2.plot(x, counts, "o--", color="#e74c3c", label="Node count")
        ax2.set_ylabel("Number of nodes of this type", color="#e74c3c")
        ax2.tick_params(axis="y", labelcolor="#e74c3c")

        from matplotlib.patches import Patch
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        sign_handles = [Patch(color="#2ca02c", label="net: increases predicted time"),
                        Patch(color="#d62728", label="net: decreases predicted time")]
        ax1.legend(lines1 + lines2 + sign_handles, labels1 + labels2 +
                   [h.get_label() for h in sign_handles], fontsize=8, loc="upper right")
        ax1.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

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

        exp_graph = self.reg_explanation_subgraph(
            explain_subgraph, 0, node_importances, edge_importances, node_top_k=10
        )
        self.reg_visualize_explanation_subgraph(
            exp_graph, save_path=os.path.join(save_dir, "explanation_subgraph.png")
        )

        names = self.feature_names

        # Decode Events node indices to their activity type name (e.g. "Events[1]"
        # -> "Events[1](PlaceOrder)") wherever an Events node is printed below, reusing
        # the same decoder already used for counterfactual output (_decode_event_types*)
        # instead of leaving raw indices unexplained in LOO's console/CSV output.
        ev_idx_to_name = {}
        for _name, _idxs in self._decode_event_types_with_indices(explain_subgraph).items():
            for _i in _idxs:
                ev_idx_to_name[_i] = _name

        def _node_label(nt, idx):
            if nt == 'Events' and idx in ev_idx_to_name:
                return f"{nt}[{idx}]({ev_idx_to_name[idx]})"
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
                (f"({ev_idx_to_name[idx]})" if nt == 'Events' and idx in ev_idx_to_name else "")
                + f"[{idx}]={signed_shift/3600:+.2f}h" + (" [LARGE]" if large else "")
                for idx, shift, large, signed_shift in top_per_type[nt]
            )
            print(f"  {nt:<12}: {entries}")

        import pandas as pd
        pd.DataFrame([
            {'node_type': nt, 'rank': rank, 'node_idx': idx,
             'activity_name': ev_idx_to_name.get(idx, '') if nt == 'Events' else '',
             'shift_hours': shift / 3600, 'signed_shift_hours': signed_shift / 3600,
             'large_shift': large}
            for nt, entries in top_per_type.items()
            for rank, (idx, shift, large, signed_shift) in enumerate(entries, 1)
        ]).to_csv(os.path.join(save_dir, "top_nodes_per_type.csv"), index=False)

        def _idx_label(nt, idx):
            """Bare-index label for one endpoint of an edge, decorated with the
            activity name only when nt is Events (no point repeating other types'
            names, since et[0]/et[2] already print the type once for the whole edge)."""
            return f"{idx}({ev_idx_to_name[idx]})" if nt == 'Events' and idx in ev_idx_to_name else str(idx)

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

        from collections import defaultdict
        type_shifts = defaultdict(list)
        feat_shifts = defaultdict(lambda: defaultdict(list))
        all_metrics = []

        n_failed = 0
        for g in sample:
            try:
                (node_imp, edge_imp, seed_feats, _, _) = self.reg_explanation(g, 0, None, top_k)
            except Exception as ex:
                n_failed += 1
                oid = g[self.kpi_viewpoint]['id'][0].item() if self.kpi_viewpoint in g.node_types else '?'
                print(f"  [trace failed] order={oid}: {type(ex).__name__}: {ex}")
                continue

            for nt, idx, shift, _, _ in node_imp:
                type_shifts[nt].append(shift / 3600)

            for f, shift, _, _ in seed_feats:
                feat_shifts[self.kpi_viewpoint][f].append(shift / 3600)

            m = self.evaluate_explanation_quality(g, 0, node_imp, edge_imp,
                                                   node_top_k=10, edge_top_k=15, verbose=False)
            all_metrics.append(m)

        import statistics

        types = sorted(type_shifts, key=lambda t: -sum(type_shifts[t]) / max(len(type_shifts[t]), 1))
        mean_shifts = [sum(type_shifts[t]) / len(type_shifts[t]) for t in types]

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(types, mean_shifts, color="#4C72B0", alpha=0.85)
        ax.set_ylabel("Mean value shift if removed (hours)")
        ax.set_title(f"Aggregate node type importance (n={len(sample)} traces)")
        ax.set_xticklabels(types, rotation=30, ha="right", fontsize=9)
        ax.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(save_dir, "aggregate_node_type_importance.png"), dpi=150)
        plt.close()

        names = self.feature_names
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
        self._plot_cf_graph_structures(query_graph, results[0]['graph'], order_id,
                                       results[0]['order_id'], query_pred / 3600.0,
                                       results[0]['predicted_hours'], save_dir)
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

    def _draw_hetero_nx(self, G, ax, type_colors, seed_key=None, title=""):
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
            labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes}
            nx.draw_networkx_labels(G, pos, ax=ax, labels=labels, font_size=6)

        ax.set_title(title, fontsize=10)
        ax.axis("off")

    def _plot_cf_graph_structures(self, query_graph, cf_graph, query_id, cf_id,
                                   query_hours, cf_hours, save_dir):
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
                              title=f"Query #{query_id}\n{n_q} events, {query_hours:.1f}h predicted")
        self._draw_hetero_nx(G_cf, axes[1], type_colors, seed_key=seed_key_cf,
                              title=f"CF #{cf_id}\n{n_cf} events, {cf_hours:.1f}h predicted")

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
        object_idx = 0
        baseline_value = self._predict_value_for_graph(graph, object_idx)
        n_q = graph['Events'].x.size(0) if 'Events' in graph.node_types else '?'

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
            print(f"  {rank}. {nt}[{idx}]  gnn_score={gnn_score_map.get((nt, idx), float('nan')):.4f}  "
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
        self.reg_visualize_explanation_subgraph(G, os.path.join(save_dir, "explanation_subgraph.png"))

        import pandas as pd
        csv_path = os.path.join(save_dir, "gnnprimary_node_importance.csv")
        pd.DataFrame([
            {'rank': r, 'node_type': nt, 'node_idx': idx,
             'gnn_score': gnn_score_map.get((nt, idx)),
             'loo_signed_shift_hours': signed_shift / 3600.0, 'large_shift': large}
            for r, (nt, idx, shift, large, signed_shift) in enumerate(node_importances, 1)
        ]).to_csv(csv_path, index=False)

        print(f"\nOutputs saved to: {save_dir}")
        print('='*60)

        return {
            'order_id': order_id,
            'predicted_hours': baseline_value / 3600.0,
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
                for nt, idx, shift, large, signed_shift in node_importances:
                    type_counts[nt] += 1
                    type_shifts[nt].append(shift / 3600.0)

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
            freq = type_counts[nt] / (len(table) * top_k) if len(table) else float('nan')
            print(f"  {nt:<12} selected {type_counts[nt]:>4}x  ({freq:.1%} of top-{top_k} slots)  "
                  f"mean shift={shifts.mean():.2f}h ± {shifts.std():.2f}h")
            type_table_rows.append({
                'node_type': nt, 'selection_count': type_counts[nt], 'selection_frequency': freq,
                'mean_shift_hours': shifts.mean(), 'std_shift_hours': shifts.std(),
            })
        type_table = pd.DataFrame(type_table_rows)
        type_csv_path = os.path.join(save_dir, "aggregate_gnnprimary_type_summary.csv")
        type_table.to_csv(type_csv_path, index=False)

        fig, ax = plt.subplots(figsize=(7, 4.5), facecolor='#fcfcfb')
        ax.set_facecolor('#fcfcfb')
        ax.bar(type_table['node_type'], type_table['mean_shift_hours'],
              yerr=type_table['std_shift_hours'], color='#2a78d6', alpha=0.9, capsize=4)
        ax.set_xlabel("Node type")
        ax.set_ylabel("Mean LOO impact if masked (hours)")
        ax.set_title(f"GNNExplainer-identified node impact by type (n={len(table)} traces)",
                    fontsize=11, loc='left')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.xticks(rotation=30, ha='right')
        plt.tight_layout()
        png_path = os.path.join(save_dir, "aggregate_gnnprimary_type_summary.png")
        plt.savefig(png_path, dpi=150)
        plt.close()

        print(f"\nSaved {csv_path}")
        print(f"Saved {type_csv_path}")
        print(f"Saved {png_path}")

        return {
            'table': table,
            'type_table': type_table,
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
