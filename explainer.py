import torch
import torch.nn.functional as F
import matplotlib.pyplot as plt
import os
from model_classes import HGT
from training import Modelling


class Explainer(Modelling):
    # ------------------------------------------------------------------
    # PGExplainer for heterogeneous regression (PyG-native)
    # ------------------------------------------------------------------

    def train_pg_explainer(self, n_epochs=20, lr=0.003, edge_size=0.0005, edge_ent=0.1, save_path=None):
        """Train PyG's PGExplainer on the heterogeneous regression model.

        PGExplainer learns a small MLP that maps GNN node embeddings to
        per-edge soft importance scores. After training it can explain any
        graph in a single forward pass (much faster than LOO at inference time).

        The trained explainer is saved to
        ``{model_path}/Hetero/{kpi}_pgexplainer.pt``.
        """
        import json
        import random
        import torch.nn.functional as F
        from torch_geometric.explain import Explainer as PyGExplainer, ModelConfig
        from torch_geometric.explain.algorithm import PGExplainer
        from torch_geometric.utils import get_embeddings_hetero
        from model_classes.pg_explainer_utils import SeedNodeWrapper

        # Load the trained HGT model
        arch_path = self.model_path.replace(".pth", "_arch.json")
        arch = {"hidden_channels": 64, "num_layers": 3, "num_heads": 4}
        if os.path.exists(arch_path):
            with open(arch_path) as f:
                arch = json.load(f)
        model = HGT.HGT(
            hidden_channels=arch["hidden_channels"],
            out_channels=1,
            num_layers=arch["num_layers"],
            num_heads=arch["num_heads"],
            data=self.train_data[0],
            viewpoint=self.viewpoint_object,
        ).to(self.device)
        model.load_state_dict(torch.load(self.model_path, weights_only=False))
        model.eval()

        # Wrap so PGExplainer sees a single scalar output per graph
        seed_model = SeedNodeWrapper(model).to(self.device)

        # Warm up lazy layers with one forward pass
        sample = self.train_data[0]
        with torch.no_grad():
            seed_model(sample.x_dict, sample.edge_index_dict)

        pg_algo = PGExplainer(epochs=n_epochs, lr=lr)
        pg_algo.coeffs['edge_size'] = edge_size
        pg_algo.coeffs['edge_ent'] = edge_ent
        explainer = PyGExplainer(
            model=seed_model,
            algorithm=pg_algo,
            explanation_type='phenomenon',
            edge_mask_type='object',
            model_config=ModelConfig(mode='regression', task_level='graph'),
        )

        sample_size = min(len(self.train_data), 200)
        print(f"\n=== Training PGExplainer ({n_epochs} epochs, {sample_size} graphs/epoch) ===")
        for epoch in range(n_epochs):
            epoch_losses = []
            graphs = random.sample(self.train_data, sample_size)
            for graph in graphs:
                vp_mask = graph[self.viewpoint_object].mask.squeeze(-1).bool()
                if not vp_mask.any():
                    continue

                # Skip graphs where the model produces NaN (empty edge types
                # can cause NaN attention in HGTConv).
                with torch.no_grad():
                    target = seed_model(graph.x_dict, graph.edge_index_dict)
                if not torch.isfinite(target).all():
                    continue

                # Capture last-layer node embeddings via forward hooks on HGTConv.
                raw_embs = get_embeddings_hetero(
                    seed_model,
                    pg_algo.SUPPORTED_HETERO_MODELS,
                    graph.x_dict,
                    graph.edge_index_dict,
                )
                last_emb = {nt: embs[-1] for nt, embs in raw_embs.items() if embs}
                if not last_emb:
                    continue

                # Custom training step — PyG's set_hetero_masks is a no-op for
                # HGTConv (it only targets to_hetero() ModuleDict models). We apply
                # masks by aggregating soft edge scores to node-level scales and
                # multiplying the input features, keeping the whole path differentiable.
                pg_algo.optimizer.zero_grad()
                temperature = pg_algo._get_temperature(epoch)

                # Generate per-edge mask logits via the PGExplainer MLP.
                mask_logits = {}
                for etype, ei in graph.edge_index_dict.items():
                    src, _, dst = etype
                    if src not in last_emb or dst not in last_emb:
                        continue
                    edge_emb = torch.cat(
                        [last_emb[src][ei[0]], last_emb[dst][ei[1]]], dim=-1
                    )
                    logits = pg_algo.mlp(edge_emb).view(-1)
                    mask_logits[etype] = pg_algo._concrete_sample(logits, temperature)

                if not mask_logits:
                    continue

                # Aggregate soft sigmoid mask values to a per-node mean scale.
                node_scale = {}
                for etype, logits in mask_logits.items():
                    src, _, dst = etype
                    ei = graph.edge_index_dict[etype]
                    soft = logits.sigmoid()
                    n_src = graph[src].x.size(0)
                    n_dst = graph[dst].x.size(0)
                    ones = torch.ones_like(soft)
                    src_agg = (
                        torch.zeros(n_src, device=self.device).scatter_add_(0, ei[0], soft)
                        / torch.zeros(n_src, device=self.device).scatter_add_(0, ei[0], ones).clamp(min=1)
                    )
                    dst_agg = (
                        torch.zeros(n_dst, device=self.device).scatter_add_(0, ei[1], soft)
                        / torch.zeros(n_dst, device=self.device).scatter_add_(0, ei[1], ones).clamp(min=1)
                    )
                    node_scale[src] = node_scale.get(src, torch.zeros(n_src, device=self.device)) + src_agg
                    node_scale[dst] = node_scale.get(dst, torch.zeros(n_dst, device=self.device)) + dst_agg

                # Normalise per-node scale to [0, 1] and multiply input features.
                masked_x_dict = {}
                for nt in graph.node_types:
                    x = graph[nt].x
                    if nt in node_scale and node_scale[nt].numel() > 0:
                        max_val = node_scale[nt].max().clamp(min=1e-8)
                        scale = (node_scale[nt] / max_val).unsqueeze(-1)
                        masked_x_dict[nt] = x * scale
                    else:
                        masked_x_dict[nt] = x

                # Masked forward pass (differentiable via the scaled x_dict).
                y_hat = seed_model(masked_x_dict, graph.edge_index_dict)
                if not torch.isfinite(y_hat).all():
                    continue

                # Fidelity: masked prediction should match original.
                fid_loss = F.mse_loss(y_hat, target.detach())

                # Edge regularisation: size (sparsity) + entropy (binariness).
                reg_loss = torch.tensor(0.0, device=self.device)
                for logits in mask_logits.values():
                    m = logits.sigmoid()
                    reg_loss = reg_loss + m.sum() * pg_algo.coeffs['edge_size']
                    mc = 0.99 * m + 0.005
                    entropy = -(mc * mc.log() + (1 - mc) * (1 - mc).log())
                    reg_loss = reg_loss + entropy.mean() * pg_algo.coeffs['edge_ent']

                loss = fid_loss + reg_loss
                if not torch.isfinite(loss):
                    continue

                loss.backward()
                pg_algo.optimizer.step()
                epoch_losses.append(loss.item())

            if (epoch + 1) % 5 == 0 or epoch == 0:
                mean_loss = sum(epoch_losses) / len(epoch_losses) if epoch_losses else float('nan')
                print(f"  Epoch {epoch + 1:3d}/{n_epochs} | Loss: {mean_loss:.4f} ({len(epoch_losses)} valid graphs)")

        # Mark as fully trained so PGExplainer.forward() doesn't raise on inference.
        pg_algo._curr_epoch = n_epochs - 1

        # Save the trained explainer algorithm (MLP weights)
        if save_path is None:
            kpi = f"TimeFrom_{self.viewpoint_object}_to_{self.path_dict['kpi_event']}"
            save_path = f"{self.path_dict['model_path']}/Hetero/{kpi}_pgexplainer.pt"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        torch.save(pg_algo.state_dict(), save_path)
        print(f"PGExplainer saved to {save_path}")
        return explainer

    def pg_explain_trace(self, order_id, top_k=5, save_dir=None):
        """Generate a PGExplainer-based explanation for one order trace.

        Loads the trained PGExplainer from disk, finds the last-event snapshot
        for ``order_id`` in the test set, generates soft edge masks, then feeds
        them into the existing evaluation and visualization pipeline.

        Returns a dict with keys: node_importances, edge_importances, metrics,
        save_dir.
        """
        import json
        from torch_geometric.explain import Explainer as PyGExplainer, ModelConfig
        from torch_geometric.explain.algorithm import PGExplainer
        from model_classes.pg_explainer_utils import SeedNodeWrapper, hetero_explanation_to_importances

        # --- Load model ---
        arch_path = self.model_path.replace(".pth", "_arch.json")
        arch = {"hidden_channels": 64, "num_layers": 3, "num_heads": 4}
        if os.path.exists(arch_path):
            with open(arch_path) as f:
                arch = json.load(f)
        model = HGT.HGT(
            hidden_channels=arch["hidden_channels"],
            out_channels=1,
            num_layers=arch["num_layers"],
            num_heads=arch["num_heads"],
            data=self.train_data[0],
            viewpoint=self.viewpoint_object,
        ).to(self.device)
        model.load_state_dict(torch.load(self.model_path, weights_only=False))
        model.eval()

        seed_model = SeedNodeWrapper(model).to(self.device)
        with torch.no_grad():
            seed_model(self.train_data[0].x_dict, self.train_data[0].edge_index_dict)

        # --- Load trained explainer ---
        kpi = f"TimeFrom_{self.viewpoint_object}_to_{self.path_dict['kpi_event']}"
        explainer_path = f"{self.path_dict['model_path']}/Hetero/{kpi}_pgexplainer.pt"
        if not os.path.exists(explainer_path):
            raise FileNotFoundError(
                f"Trained PGExplainer not found at {explainer_path}. "
                "Call train_pg_explainer() first."
            )

        pg_algo = PGExplainer(epochs=1, lr=0.003)
        # connect() must be called (via PyGExplainer.__init__) before any pg_algo method
        # that accesses self.model_config (e.g. train, forward).
        explainer = PyGExplainer(
            model=seed_model,
            algorithm=pg_algo,
            explanation_type='phenomenon',
            edge_mask_type='object',
            model_config=ModelConfig(mode='regression', task_level='graph'),
        )
        # Initialize the lazy Linear(-1, 64) in pg_algo.mlp so load_state_dict
        # can match tensor shapes (input dim = 2 * hidden_channels).
        with torch.no_grad():
            dummy_emb = torch.zeros(1, 2 * arch['hidden_channels'], device=self.device)
            pg_algo.mlp(dummy_emb)
        pg_algo.load_state_dict(torch.load(explainer_path, weights_only=False))
        pg_algo._curr_epoch = 0  # epochs=1 → epochs-1=0; satisfies "fully trained" check

        # --- Find last-event graph for this order ---
        explain_subgraph = None
        for g in self.test_data:
            if (g[self.viewpoint_object].last_event.item()
                    and g[self.viewpoint_object].id.item() == order_id):
                explain_subgraph = g
                break
        if explain_subgraph is None:
            raise ValueError(f"No last-event graph found for order_id={order_id} in test set.")

        # --- Generate explanation ---
        with torch.no_grad():
            target = explain_subgraph[self.viewpoint_object].y[0:1]
        explanation = explainer(
            explain_subgraph.x_dict,
            explain_subgraph.edge_index_dict,
            target=target,
        )

        # --- Convert to existing pipeline format ---
        node_importances, edge_importances = hetero_explanation_to_importances(
            explanation, explain_subgraph, threshold=0.5
        )

        # --- Save directory ---
        if save_dir is None:
            save_dir = f"{self.path_dict['explainer_path']}order_{order_id}_pg/"
        os.makedirs(save_dir, exist_ok=True)

        # Baseline prediction (for summary)
        baseline_value = self._predict_value_for_graph(explain_subgraph, 0)
        pred_h = baseline_value / 3600

        print(f"\n=== PGExplainer: Order #{order_id} (predicted: {pred_h:.1f}h remaining) ===")
        print(f"Top {top_k} edges by PGExplainer importance:")
        for rank, (et, e_idx, score, large) in enumerate(edge_importances[:top_k], 1):
            src_t, _, dst_t = et
            ei = explain_subgraph[et].edge_index
            src_id = ei[0, e_idx].item()
            dst_id = ei[1, e_idx].item()
            flag = "  [HIGH]" if large else ""
            print(f"  {rank}. {src_t}[{src_id}]→{dst_t}[{dst_id}]  score={score:.3f}{flag}")

        print(f"\nTop {top_k} nodes by aggregated edge importance:")
        for rank, (nt, nid, score, large) in enumerate(node_importances[:top_k], 1):
            flag = "  [HIGH]" if large else ""
            print(f"  {rank}. {nt}[{nid}]  score={score:.3f}{flag}")

        # --- Explanation quality metrics (reuse existing method) ---
        metrics = self.evaluate_explanation_quality(
            explain_subgraph, 0, node_importances, edge_importances,
            node_top_k=top_k, edge_top_k=top_k * 3, verbose=True,
        )

        # --- Visualizations (reuse existing pipeline) ---
        self.plot_node_type_summary(
            node_importances,
            save_path=os.path.join(save_dir, "pg_node_type_summary.png"),
        )

        explanation_G = self.reg_explanation_subgraph(
            explain_subgraph, 0, node_importances, edge_importances, node_top_k=top_k
        )
        self.reg_visualize_explanation_subgraph(
            explanation_G,
            save_path=os.path.join(save_dir, "pg_explanation_subgraph.png"),
        )

        results = {
            "order_id": order_id,
            "node_importances": node_importances,
            "edge_importances": edge_importances,
            "metrics": metrics,
            "save_dir": save_dir,
        }
        print(f"\nPGExplainer artifacts saved to {save_dir}")
        return results

    # ------------------------------------------------------------------
    # Explanation helpers
    # ------------------------------------------------------------------

    @torch.no_grad()
    def _predict_proba(self, batch):
        batch = batch.to(self.device)
        out = self.model(batch.x_dict, batch.edge_index_dict)
        seed_out = out[:1]
        return F.softmax(seed_out, dim=-1)[0]

    def feature_importance_for_node(self, batch, node_type, node_idx, baseline_confidence,
                                    predicted_class, top_k=10):
        """Leave-one-out at the FEATURE level for classification."""
        x = batch[node_type].x[node_idx]
        num_features = x.size(0)
        feature_importances = []

        for f in range(num_features):
            if x[f].item() == 0.0:
                continue
            perturbed = batch.clone()
            perturbed[node_type].x[node_idx, f] = 0.0
            proba = self._predict_proba(perturbed)
            confidence_drop = baseline_confidence - proba[predicted_class].item()
            flips = proba.argmax().item() != predicted_class
            feature_importances.append((f, confidence_drop, flips))

        feature_importances.sort(key=lambda t: t[1], reverse=True)
        return feature_importances[:top_k]

    def build_explanation_subgraph(self, batch, node_importances, edge_importances,
                                   node_top_k=10, edge_top_k=15):
        """Build a NetworkX subgraph from LOO classification importance scores."""
        import networkx as nx

        G = nx.MultiDiGraph()

        G.add_node((self.viewpoint_object, 0), node_type=self.viewpoint_object, importance=1.0, is_seed=True, flips=False)

        top_nodes = node_importances[:node_top_k]
        for nt, i, drop, flips in top_nodes:
            G.add_node((nt, i), node_type=nt, importance=drop, is_seed=False, flips=flips)

        top_edges = edge_importances[:edge_top_k]
        for edge_type, e, drop, flips in top_edges:
            src_type, _, dst_type = edge_type
            edge_index = batch[edge_type].edge_index
            src, dst = edge_index[:, e].tolist()
            src_key, dst_key = (src_type, src), (dst_type, dst)

            for key, ntype in [(src_key, src_type), (dst_key, dst_type)]:
                if key not in G.nodes:
                    is_seed = (ntype == self.viewpoint_object and key[1] == 0)
                    G.add_node(key, node_type=ntype, importance=0.0, is_seed=is_seed, flips=False)

            G.add_edge(src_key, dst_key, edge_type=edge_type[1], importance=drop, flips=flips)

        return G

    def visualize_explanation_subgraph(self, G, save_path="explanation_subgraph.png"):
        """Draw classification explanation subgraph."""
        import matplotlib.pyplot as plt
        import networkx as nx

        type_colors = {"author": "#4C72B0", "paper": "#DD8452", "term": "#55A868", "conference": "#C44E52"}

        pos = nx.spring_layout(G, seed=42, k=0.9)

        node_colors, node_sizes, edge_colors_outline = [], [], []
        for node, attrs in G.nodes(data=True):
            node_colors.append(type_colors.get(attrs["node_type"], "gray"))
            base_size = 900 if attrs.get("is_seed") else 250
            node_sizes.append(base_size + max(attrs.get("importance", 0), 0) * 2500)
            edge_colors_outline.append("red" if attrs.get("flips") else "black")

        edge_colors, edge_widths = [], []
        for _, _, attrs in G.edges(data=True):
            edge_colors.append("red" if attrs.get("flips") else "gray")
            edge_widths.append(1 + max(attrs.get("importance", 0), 0) * 12)

        plt.figure(figsize=(10, 8))
        nx.draw_networkx_nodes(
            G, pos, node_color=node_colors, node_size=node_sizes,
            edgecolors=edge_colors_outline, linewidths=1.5, alpha=0.9,
        )
        nx.draw_networkx_edges(
            G, pos, edge_color=edge_colors, width=edge_widths,
            arrows=True, connectionstyle="arc3,rad=0.1", alpha=0.7,
        )
        labels = {node: f"{node[0]}[{node[1]}]" for node in G.nodes}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=7)

        legend_handles = [
            plt.Line2D([0], [0], marker="o", color="w", label=nt,
                       markerfacecolor=color, markersize=10)
            for nt, color in type_colors.items()
        ]
        legend_handles.append(plt.Line2D([0], [0], color="red", lw=2, label="flips prediction"))
        plt.legend(handles=legend_handles, loc="best", fontsize=8)

        plt.title("Counterfactual Explanation Subgraph")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"Saved explanation subgraph visualization to {save_path}")

    def class_evaluate_explanation(self, batch, node_importances, edge_importances,
                                   node_top_k=10, edge_top_k=15, verbose=True):
        """Compute fidelity+/-, characterization, and sparsity for classification explanations."""
        baseline_proba = self._predict_proba(batch)
        predicted_class = baseline_proba.argmax().item()
        baseline_confidence = baseline_proba[predicted_class].item()

        explanation_nodes_by_type = {}
        for nt, i, _drop, _flips in node_importances[:node_top_k]:
            explanation_nodes_by_type.setdefault(nt, set()).add(i)
        explanation_nodes_by_type.setdefault(self.viewpoint_object, set()).add(0)

        explanation_edges_by_type = {}
        for et, e, _drop, _flips in edge_importances[:edge_top_k]:
            explanation_edges_by_type.setdefault(et, set()).add(e)

        # Fidelity+: remove the explanation, keep everything else
        complement = batch.clone()
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
        proba_complement = self._predict_proba(complement)
        fidelity_plus = baseline_confidence - proba_complement[predicted_class].item()

        # Fidelity-: keep ONLY the explanation
        subgraph = batch.clone()
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
        proba_subgraph = self._predict_proba(subgraph)
        fidelity_minus = baseline_confidence - proba_subgraph[predicted_class].item()

        denom = fidelity_plus + abs(fidelity_minus)
        characterization_score = fidelity_plus / denom if denom > 1e-8 else 0.0

        total_nodes = sum(batch[nt].x.size(0) for nt in batch.node_types)
        explanation_node_count = sum(len(s) for s in explanation_nodes_by_type.values())
        node_sparsity = 1 - (explanation_node_count / total_nodes)

        total_edges = sum(batch[et].edge_index.size(1) for et in batch.edge_types)
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
            print(f"  Fidelity+        : {fidelity_plus:+.4f}  "
                  f"(higher is better -- removing the explanation should hurt confidence)")
            print(f"  Fidelity-        : {fidelity_minus:+.4f}  "
                  f"(closer to 0 is better -- the explanation alone should reproduce the prediction)")
            print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
            print(f"  Node sparsity    : {node_sparsity:.2%}  (share of sampled nodes excluded from the explanation)")
            print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of sampled edges excluded from the explanation)")

        return metrics

    def class_explanation(self, explain_subgraph, object_idx, top_k):
        baseline_proba = self._predict_proba(explain_subgraph)
        predicted_class = baseline_proba.argmax().item()
        baseline_confidence = baseline_proba[predicted_class].item()

        print(f"\nExplaining {self.viewpoint_object} node #{object_idx}")
        print(f"  Predicted class: {predicted_class} (confidence {baseline_confidence:.4f})")
        print(f"  Sampled neighborhood: " +
              ", ".join(f"{nt}={explain_subgraph[nt].num_nodes}" for nt in explain_subgraph.node_types))

        node_importances = []
        for node_type in explain_subgraph.node_types:
            n = explain_subgraph[node_type].x.size(0)
            start = 1 if node_type == self.viewpoint_object else 0
            for i in range(start, n):
                perturbed = explain_subgraph.clone()
                perturbed[node_type].x[i] = 0.0
                proba = self._predict_proba(perturbed)
                confidence_drop = baseline_confidence - proba[predicted_class].item()
                flips = proba.argmax().item() != predicted_class
                node_importances.append((node_type, i, confidence_drop, flips))

        edge_importances = []
        for edge_type in explain_subgraph.edge_types:
            edge_index = explain_subgraph[edge_type].edge_index
            num_edges = edge_index.size(1)
            for e in range(num_edges):
                perturbed = explain_subgraph.clone()
                keep = torch.ones(num_edges, dtype=torch.bool, device=self.device)
                keep[e] = False
                perturbed[edge_type].edge_index = edge_index[:, keep]
                proba = self._predict_proba(perturbed)
                confidence_drop = baseline_confidence - proba[predicted_class].item()
                flips = proba.argmax().item() != predicted_class
                edge_importances.append((edge_type, e, confidence_drop, flips))

        node_importances.sort(key=lambda t: t[2], reverse=True)
        edge_importances.sort(key=lambda t: t[2], reverse=True)

        seed_feature_importances = self.feature_importance_for_node(
            explain_subgraph, self.viewpoint_object, 0, baseline_confidence, predicted_class, top_k=top_k
        )

        if node_importances:
            top_node_type, top_node_idx, _, _ = node_importances[0]
            top_node_feature_importances = self.feature_importance_for_node(
                explain_subgraph, top_node_type, top_node_idx, baseline_confidence, predicted_class, top_k=top_k
            )
        else:
            top_node_type, top_node_idx = None, None
            top_node_feature_importances = []

        print(f"\n  Top {top_k} most important NODES (confidence drop if removed):")
        for node_type, i, drop, flips in node_importances[:top_k]:
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {node_type}[{i}]: confidence drop = {drop:+.4f}{flag}")

        print(f"\n  Top {top_k} most important EDGES (confidence drop if removed):")
        for edge_type, e, drop, flips in edge_importances[:top_k]:
            src, dst = explain_subgraph[edge_type].edge_index[:, e].tolist()
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {edge_type} edge ({src} -> {dst}): confidence drop = {drop:+.4f}{flag}")

        print(f"\n  Top {top_k} most important FEATURES on the seed {self.viewpoint_object} itself:")
        for f, drop, flips in seed_feature_importances:
            flag = "  <-- FLIPS PREDICTION" if flips else ""
            print(f"    {self.viewpoint_object}[0].x[{f}]: confidence drop = {drop:+.4f}{flag}")

        if top_node_type is not None:
            print(f"\n  Top {top_k} most important FEATURES on the most influential neighbor "
                  f"({top_node_type}[{top_node_idx}]):")
            for f, drop, flips in top_node_feature_importances:
                flag = "  <-- FLIPS PREDICTION" if flips else ""
                print(f"    {top_node_type}[{top_node_idx}].x[{f}]: confidence drop = {drop:+.4f}{flag}")

        any_flips = (
                any(f for *_, f in node_importances)
                or any(f for *_, f in edge_importances)
                or any(f for *_, f in seed_feature_importances)
                or any(f for *_, f in top_node_feature_importances)
        )
        if any_flips:
            print("\n  >>> A genuine counterfactual exists above: removing that single")
            print("      node, edge, or feature changes the model's predicted class entirely.")
        else:
            print("\n  >>> No single node/edge/feature removal flips the prediction -- the")
            print("      model's decision here is robust to any one removal (it relies on")
            print("      combined, redundant evidence rather than one critical piece).")

        explanation_graph = self.build_explanation_subgraph(
            explain_subgraph, node_importances, edge_importances, node_top_k=10, edge_top_k=15
        )
        self.visualize_explanation_subgraph(explanation_graph, save_path="explanation_subgraph.png")

        self.class_evaluate_explanation(explain_subgraph, node_importances, edge_importances,
                                   node_top_k=10, edge_top_k=15, verbose=True)

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
            shift = abs(baseline_value - pred)
            large_shift = shift > self.target_std.item()
            feature_importances.append((f, shift, large_shift))
        feature_importances.sort(key=lambda t: t[1], reverse=True)
        return feature_importances[:top_k]

    def reg_explanation_subgraph(self, graph, seed_paper_idx, node_importances,
                                 edge_importances, node_top_k=10):
        """Build a NetworkX subgraph from LOO regression importance scores."""
        import networkx as nx

        seed_key = (self.viewpoint_object, seed_paper_idx)
        G = nx.MultiDiGraph()
        G.add_node(seed_key, node_type=self.viewpoint_object, importance=1.0,
                   is_seed=True, large_shift=False, is_connector=False)

        included = {seed_key}
        for nt, i, shift, large in node_importances[:node_top_k]:
            key = (nt, i)
            G.add_node(key, node_type=nt, importance=shift, is_seed=False,
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
        for edge_type, e, shift, large in edge_importances:
            edge_index = graph[edge_type].edge_index
            src, dst = edge_index[:, e].tolist()
            edge_importance_lookup[(edge_type, src, dst)] = (shift, large)

        def add_real_edge(edge_type, src_key, dst_key):
            shift, large = edge_importance_lookup.get(
                (edge_type, src_key[1], dst_key[1]), (0.0, False)
            )
            G.add_edge(src_key, dst_key, edge_type=edge_type[1], importance=shift, large_shift=large)

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
                    G.add_node(n, node_type=n[0], importance=0.0, is_seed=False,
                               large_shift=False, is_connector=True)

            for a, b in zip(path[:-1], path[1:]):
                if G.has_edge(a, b) or G.has_edge(b, a):
                    continue
                edge_type = full_nx[a][b]["edge_type"]
                shift, large = edge_importance_lookup.get(
                    (edge_type, a[1], b[1]),
                    edge_importance_lookup.get((edge_type, b[1], a[1]), (0.0, False)),
                )
                G.add_edge(a, b, edge_type=edge_type[1], importance=shift, large_shift=large)

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
        except Exception:
            pos = nx.spring_layout(G, seed=42, k=0.9)

        node_colors, node_sizes, edge_colors_outline, alphas = [], [], [], []
        for node, attrs in G.nodes(data=True):
            node_colors.append(type_colors.get(attrs["node_type"], "gray"))
            if attrs.get("is_seed"):
                node_sizes.append(900)
            elif attrs.get("is_connector"):
                node_sizes.append(150)
            else:
                node_sizes.append(250 + max(attrs.get("importance", 0), 0) * 1500)
            edge_colors_outline.append("red" if attrs.get("large_shift") else "black")
            alphas.append(0.4 if attrs.get("is_connector") else 0.9)

        edge_colors, edge_widths = [], []
        for _, _, attrs in G.edges(data=True):
            edge_colors.append("red" if attrs.get("large_shift") else "gray")
            edge_widths.append(1 + max(attrs.get("importance", 0), 0) * 8)

        plt.figure(figsize=(10, 8))
        nx.draw_networkx_nodes(
            G, pos, node_color=node_colors, node_size=node_sizes,
            edgecolors=edge_colors_outline, linewidths=1.5, alpha=alphas,
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
        legend_handles.append(plt.Line2D([0], [0], color="red", lw=2, label="large shift (>1 std)"))
        legend_handles.append(plt.Line2D([0], [0], marker="o", color="w", label="connector (faded)",
                                         markerfacecolor="gray", alpha=0.4, markersize=8))
        plt.legend(handles=legend_handles, loc="best", fontsize=8)

        plt.title("Counterfactual Explanation Subgraph (Regression)")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"Saved explanation subgraph visualization to {save_path}")

    def evaluate_explanation_quality(self, graph, paper_idx, node_importances, edge_importances,
                                     node_top_k=10, edge_top_k=15, verbose=True):
        """Compute fidelity+/-, characterization, and sparsity for regression explanations."""
        baseline_value = self._predict_value_for_graph(graph, paper_idx)

        explanation_nodes_by_type = {}
        for nt, i, _shift, _large in node_importances[:node_top_k]:
            explanation_nodes_by_type.setdefault(nt, set()).add(i)
        explanation_nodes_by_type.setdefault(self.viewpoint_object, set()).add(paper_idx)

        explanation_edges_by_type = {}
        for et, e, _shift, _large in edge_importances[:edge_top_k]:
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
        pred_complement = self._predict_value_for_graph(graph, paper_idx, perturbed_graph=complement)
        fidelity_plus = abs(baseline_value - pred_complement)

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
        pred_subgraph = self._predict_value_for_graph(graph, paper_idx, perturbed_graph=subgraph)
        fidelity_minus = abs(baseline_value - pred_subgraph)

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
            print(f"  Fidelity+        : {fidelity_plus:.4f}  "
                  f"(higher is better -- removing the explanation should shift the prediction)")
            print(f"  Fidelity-        : {fidelity_minus:.4f}  "
                  f"(closer to 0 is better -- the explanation alone should reproduce the prediction)")
            print(f"  Characterization : {characterization_score:.4f}  (higher is better, in [0, 1])")
            print(f"  Node sparsity    : {node_sparsity:.2%}  (share of the graph excluded from the explanation)")
            print(f"  Edge sparsity    : {edge_sparsity:.2%}  (share of edges excluded from the explanation)")

        return metrics

    def reg_explanation(self, explain_subgraph, object_idx, graph_id, top_k):
        baseline_value = self._predict_value_for_graph(explain_subgraph, object_idx)

        node_importances = []
        for node_type in explain_subgraph.node_types:
            n = explain_subgraph[node_type].x.size(0)
            for idx in range(n):
                if node_type == self.viewpoint_object and idx == object_idx:
                    continue
                perturbed = explain_subgraph.clone()
                perturbed[node_type].x[idx] = 0.0
                pred = self._predict_value_for_graph(explain_subgraph, object_idx, perturbed_graph=perturbed)
                shift = abs(baseline_value - pred)
                large_shift = shift > self.target_std.item()
                node_importances.append((node_type, idx, shift, large_shift))

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
                shift = abs(baseline_value - pred)
                large_shift = shift > self.target_std.item()
                edge_importances.append((edge_type, e, shift, large_shift))

        node_importances.sort(key=lambda t: t[2], reverse=True)
        edge_importances.sort(key=lambda t: t[2], reverse=True)

        seed_feature_importances = self.reg_feature_importance_for_node_in_graph(
            explain_subgraph, self.viewpoint_object, object_idx, baseline_value, object_idx, top_k=top_k
        )

        if node_importances:
            top_node_type, top_node_idx, _, _ = node_importances[0]
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

    def plot_feature_importances(self, node_type, feature_importances, save_path):
        """Horizontal bar chart of per-feature value shifts for one node."""
        names = self.feature_names.get(node_type, [])
        feats, shifts, larges = [], [], []
        for f, shift, large in feature_importances:
            label = names[f] if f < len(names) else f"feat_{f}"
            feats.append(label)
            shifts.append(shift / 3600)
            larges.append(large)

        if not feats:
            return

        fig, ax = plt.subplots(figsize=(7, max(3, len(feats) * 0.45)))
        colors = ["#e74c3c" if l else "#7f8c8d" for l in larges]
        bars = ax.barh(range(len(feats)), shifts, color=colors)
        ax.set_yticks(range(len(feats)))
        ax.set_yticklabels(feats, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel("Value shift if removed (hours)")
        ax.set_title(f"Feature importance — {node_type} node")
        for bar, val in zip(bars, shifts):
            ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                    f"{val:.2f}h", va="center", fontsize=8)
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color="#e74c3c", label=">1 std shift"),
                            Patch(color="#7f8c8d", label="≤1 std shift")],
                  fontsize=8, loc="lower right")
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def plot_node_type_summary(self, node_importances, save_path):
        """Bar chart of total influence per node type across the whole trace."""
        from collections import defaultdict
        type_shift = defaultdict(float)
        type_count = defaultdict(int)
        for node_type, idx, shift, _ in node_importances:
            type_shift[node_type] += shift / 3600
            type_count[node_type] += 1

        types = sorted(type_shift, key=lambda t: type_shift[t], reverse=True)
        total_shifts = [type_shift[t] for t in types]
        counts = [type_count[t] for t in types]

        fig, ax1 = plt.subplots(figsize=(8, 4))
        x = range(len(types))
        bars = ax1.bar(x, total_shifts, color="#4C72B0", alpha=0.8, label="Total shift (hours)")
        ax1.set_xticks(x)
        ax1.set_xticklabels(types, rotation=30, ha="right", fontsize=9)
        ax1.set_ylabel("Cumulative value shift if type removed (hours)")
        ax1.set_title("Node type importance summary")

        ax2 = ax1.twinx()
        ax2.plot(x, counts, "o--", color="#e74c3c", label="Node count")
        ax2.set_ylabel("Number of nodes of this type", color="#e74c3c")
        ax2.tick_params(axis="y", labelcolor="#e74c3c")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8, loc="upper right")
        ax1.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

    def explain_trace(self, order_id, top_k=5, save_dir=None):
        """Full LOO explanation for a single order trace (last-event snapshot)."""
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], f"order_{order_id}")
        os.makedirs(save_dir, exist_ok=True)

        import json
        arch_cfg_path = self.model_path.replace(".pth", "_arch.json")
        if os.path.exists(arch_cfg_path):
            with open(arch_cfg_path) as f:
                arch = json.load(f)
            self.model = HGT.HGT(
                hidden_channels=arch["hidden_channels"],
                out_channels=1,
                num_layers=arch["num_layers"],
                num_heads=arch["num_heads"],
                data=self.test_data[0],
                viewpoint=self.viewpoint_object,
            ).to(self.device)
        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        explain_subgraph = None
        for g in self.test_data:
            if (g[self.viewpoint_object]['last_event'].item()
                    and g[self.viewpoint_object]['id'].item() == order_id):
                explain_subgraph = g
                break
        if explain_subgraph is None:
            raise ValueError(f"Order ID {order_id} with last_event=True not found in test data.")

        (node_importances, edge_importances,
         seed_feats, top_neighbor_feats, baseline_value) = self.reg_explanation(
            explain_subgraph, 0, order_id, top_k
        )

        metrics = self.evaluate_explanation_quality(
            explain_subgraph, 0, node_importances, edge_importances,
            node_top_k=10, edge_top_k=15, verbose=False
        )

        self.plot_feature_importances(
            self.viewpoint_object, seed_feats,
            os.path.join(save_dir, f"feat_importance_{self.viewpoint_object}.png")
        )
        if node_importances:
            top_nt, top_ni, _, _ = node_importances[0]
            self.plot_feature_importances(
                top_nt, top_neighbor_feats,
                os.path.join(save_dir, f"feat_importance_{top_nt}.png")
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
        print(f"\n{'='*60}")
        print(f"Explanation for {self.viewpoint_object} #{order_id}")
        print(f"  Predicted remaining time : {round(baseline_value / 3600)} hours")
        print(f"  Graph size : " +
              ", ".join(f"{nt}={explain_subgraph[nt].num_nodes}" for nt in explain_subgraph.node_types))
        print(f"\nTop {top_k} nodes by influence (value shift if removed):")
        for rank, (nt, idx, shift, large) in enumerate(node_importances[:top_k], 1):
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
            print(f"  {rank}. {nt}[{idx}]{feat_vals}: shift={shift/3600:+.2f}h{flag}")

        print(f"\nTop {top_k} edges by influence:")
        for rank, (et, e, shift, large) in enumerate(edge_importances[:top_k], 1):
            src, dst = explain_subgraph[et].edge_index[:, e].tolist()
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {et[0]}→{et[2]} ({src}→{dst}): shift={shift/3600:+.2f}h{flag}")

        print(f"\nTop {top_k} features on seed {self.viewpoint_object} node:")
        seed_names = names.get(self.viewpoint_object, [])
        for rank, (f, shift, large) in enumerate(seed_feats[:top_k], 1):
            fname = seed_names[f] if f < len(seed_names) else f"feat_{f}"
            flag = "  [LARGE SHIFT]" if large else ""
            print(f"  {rank}. {fname}: shift={shift/3600:+.2f}h{flag}")

        print(f"\nExplanation quality metrics:")
        print(f"  Fidelity+       : {metrics['fidelity_plus']:.4f}  (↑ better)")
        print(f"  Fidelity−       : {metrics['fidelity_minus']:.4f}  (↓ better)")
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
            "metrics": metrics,
            "save_dir": save_dir,
        }

    def explain_aggregate(self, n_traces=50, top_k=5, save_dir=None):
        """Run LOO explanation on n_traces test graphs and aggregate results."""
        if save_dir is None:
            save_dir = os.path.join(self.path_dict['explainer_path'], "aggregate")
        os.makedirs(save_dir, exist_ok=True)

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))
        self.model.eval()

        last_event_graphs = [g for g in self.test_data
                             if g[self.viewpoint_object]['last_event'].item()]
        sample = last_event_graphs[:n_traces]
        print(f"Running aggregate explanation on {len(sample)} traces…")

        from collections import defaultdict
        type_shifts = defaultdict(list)
        feat_shifts = defaultdict(lambda: defaultdict(list))
        all_metrics = []

        for g in sample:
            try:
                (node_imp, edge_imp, seed_feats, _, _) = self.reg_explanation(g, 0, None, top_k)
            except Exception:
                continue

            for nt, idx, shift, _ in node_imp:
                type_shifts[nt].append(shift / 3600)

            for f, shift, _ in seed_feats:
                feat_shifts[self.viewpoint_object][f].append(shift / 3600)

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
            ax.barh(range(len(feats)), means, color="#DD8452", alpha=0.85)
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

        print(f"\nAggregate explanation quality (n={len(all_metrics)} traces):")
        for k in metric_keys:
            vals = [m[k] for m in all_metrics]
            mean_v = sum(vals) / len(vals) if vals else float("nan")
            std_v = statistics.stdev(vals) if len(vals) > 1 else 0.0
            print(f"  {k:25s}: {mean_v:.4f} ± {std_v:.4f}")
        print(f"\nAggregate outputs saved to: {save_dir}")

        return all_metrics

    def cf_explanation(self):
        object_idx = 1971
        top_k = 5

        self.model.load_state_dict(torch.load(self.model_path, weights_only=False))

        for x in self.test_data:
            if x['Orders']['last_event'] == True:
                if x['Orders']['id'] == object_idx:
                    explain_subgraph = x
                    break

        if self.path_dict['kpi_type'] == 0:
            self.reg_explanation(explain_subgraph, 0, object_idx, top_k)
        elif self.path_dict['kpi_type'] == 1:
            self.class_explanation(explain_subgraph, object_idx, top_k)
