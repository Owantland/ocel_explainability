# Explainability Layer

This document describes the four post-hoc explainability techniques implemented in `explainer.py`
for the HGT remaining-time regression model. All techniques are applied to a **trained, frozen
model** and treat it as a black box — no retraining or model modification is involved.

The explainability goal is: *given a predicted remaining time for an order, which nodes, edges,
and features drove that prediction, and how faithful is the explanation to the model's actual
behaviour?*

Three of the four techniques address regression (the primary task). A classification variant of
Leave-One-Out exists in code for the binary-KPI setting but is not the focus here.

---

## 1. Leave-One-Out (LOO)

### What it does

LOO measures the importance of each node, edge, and feature by **removing it and measuring the
resulting shift in predicted remaining time** (reported in hours after de-normalization). A large
shift means the element was load-bearing for the prediction; a zero shift means the model ignored
it.

The core logic lives in `reg_explanation()`, which:
1. Computes a **baseline prediction** for the seed Orders node.
2. Sequentially **zeroes each non-seed node's feature vector** → records `|Δpred|`.
3. Sequentially **removes each edge** from the graph → records `|Δpred|`.
4. For the seed Orders node and the top-ranked neighbor node, **zeroes each feature dimension
   in turn** → records `|Δpred|` per dimension.

All shifts are sorted descending; the top-k are returned as explanation.

### Single trace — `explain_trace(order_id)`

Locates the last-event graph for the given order ID in the test set, runs the full LOO pipeline,
and produces:

- **Console output**: ranked tables of top-k nodes, edges, and features with their hour shifts.
- **`feat_importance_{node_type}.png`**: horizontal bar charts for seed and top-neighbor features
  (red = shift exceeds 1 std of training targets, ~115h; gray = smaller).
- **`node_type_summary.png`**: cumulative LOO shift per node type + node count per type.
- **`explanation_subgraph.png`**: the top-k nodes and edges as a NetworkX graph, with node size
  proportional to shift magnitude and red highlighting for large-shift elements.
- **Fidelity metrics** (see §3).

**Example (cant=1000, order #1794, true=33h, predicted=35h):**

```
Top 5 nodes:
  Events[1]   [type_0=3.16, type_1=-0.33]  shift=+7.1h
  Events[16]  [type_0=-0.32, type_1=2.99]  shift=+4.6h
  Events[13]  [type_0=-0.32, type_1=-0.33] shift=+3.9h

Top 5 edges:
  Packages→Items (0→3)  shift=+55.5h
  Packages→Items (1→4)  shift=+54.1h
  Packages→Items (0→5)  shift=+54.0h

Top 4 features on Orders seed:
  n_products   shift=+0.54h
  price        shift=+0.52h
  total_weight shift=+0.17h
  n_items      shift=+0.17h
```

The model is primarily sensitive to: (a) which specific Events occurred (event type identity),
(b) the connectivity between Packages and Items (which items are packed), and (c) the aggregate
complexity of the order (number of distinct products, total weight).

### Aggregated — `explain_aggregate(n_traces=50)`

Runs the same LOO pipeline on up to 50 randomly sampled last-event graphs, accumulates per-node-
type and per-feature shifts, and reports **mean ± std** across traces.

Outputs:
- `aggregate_node_type_importance.png` — mean LOO shift per node type across all traces.
- Per-type bar charts of mean feature-level shifts.
- `aggregate_metrics.csv` — one row per trace with all fidelity scores, plus a summary row.

**Example (cant=1000, n=50):**

```
Characterization score : 0.887 ± 0.137
Node sparsity          : 61.4% ± 11.1%
Edge sparsity          : 90.7% ± 4.3%
```

The aggregate confirms that explanations are compact (only ~10% of edges are needed) and that
the explanation subgraph genuinely drives the prediction (characterization 0.887/1.0).

---

## 2. Explanation Quality — Fidelity Metrics

### What it does

Fidelity metrics answer: *is the top-k explanation subgraph actually responsible for the
prediction, or did LOO select irrelevant elements?* They are computed in
`evaluate_explanation_quality()` and called automatically by `explain_trace` and
`explain_aggregate`.

| Metric | Formula | Interpretation |
|---|---|---|
| **Fidelity+** | `\|pred_full − pred_without_explanation\|` | Removing the explanation should shift the prediction (↑ better) |
| **Fidelity−** | `\|pred_full − pred_with_only_explanation\|` | The explanation alone should reproduce the prediction (↓ better) |
| **Characterization** | `F+ / (F+ + F−)` | Combined score in [0, 1] (↑ better) |
| **Node sparsity** | `1 − explained_nodes / total_nodes` | Fraction of nodes *excluded* (↑ = more compact explanation) |
| **Edge sparsity** | `1 − explained_edges / total_edges` | Same for edges |

### Single trace

Reported at the end of every `explain_trace` call. Example (order #1794):

```
Fidelity+       : 318,952s   (removing top-10 nodes + 15 edges shifts pred by ~88h)
Fidelity−       :  99,118s   (explanation alone reproduces pred with ~27h drift)
Characterization: 0.763
Node sparsity   : 76.1%
Edge sparsity   : 95.8%
```

### Aggregated

Mean ± std across all traces in `explain_aggregate`, saved to `aggregate_metrics.csv`.
The 0.887 mean characterization score indicates that across the dataset, the explanation
subgraphs are reliably capturing what the model responds to.

---

## 3. InputXGradient — Feature Attribution

### What it does

InputXGradient computes `∂output/∂x_i × x_i` for each input feature — the gradient of the
predicted remaining time with respect to each feature, scaled by the feature's value. This is a
**gradient-based** complement to LOO: instead of measuring what happens when an element is
removed, it measures how sensitively the output responds to small changes in each feature
dimension.

The computation is implemented via Captum (`_compute_ig_for_graph()`):
- The model is called with `requires_grad_(True)` on all node feature tensors.
- A single backward pass produces a signed attribution `[N_nodes, F_features]` per node type.
- Attributions are mean-pooled over nodes within each type → one vector per type per graph.
- **Positive** attribution: increasing that feature increases predicted remaining time.
- **Negative** attribution: increasing that feature decreases predicted remaining time.

### Single trace

No standalone single-trace entry point is currently exposed. `_compute_ig_for_graph(graph)` is
callable directly and returns `{node_type: ndarray}`, but there is no `explain_trace_ig(order_id)`
wrapper (see §4 for the improvement proposal).

### Aggregated — `explain_feature_attribution(n_traces=None)`

Runs InputXGradient across all last-event test graphs (or a subset), averages signed and absolute
attributions across graphs, and outputs:

- **Per-type bar charts** (`ig_{node_type}_importance.png`): features sorted by mean absolute
  attribution; blue bars = positive contribution, orange = negative.
- **Heatmap** (`ig_heatmap.png`): all node types × feature dimensions in one view.
- **CSV** (`ig_attribution.csv`): `[node_type, feature_dim, feature_name, mean_signed, mean_abs]`.
- **Perturbation fidelity check**: zeroes the top-K and bottom-K features by attribution across all
  graphs, measures mean `|Δpred|` for each group — top-K should shift predictions more.

**Example (cant=1000, n=356 last-event graphs):**

```
Orders   top-3: n_products=0.0035, n_items=0.0017, total_weight=0.0016
Items    top-3: price=0.0002
Events   top-3: type_8≈0, elapsed_h≈0

Perturbation fidelity:
  Top-K (Orders[n_products], Orders[n_items]):   mean |Δpred| = 0.43h  ✓
  Bot-K (Employees[feat_1], Employees[feat_2]):  mean |Δpred| = 0.00h
  → PASS
```

### Relationship to LOO

| Dimension | LOO | InputXGradient |
|---|---|---|
| What it measures | Structural importance (which node/edge matters) | Feature sensitivity (how much each feature dimension drives output) |
| Cost | O(N_nodes + N_edges) per trace | Single backward pass per trace |
| Scope | Single trace or aggregate | Aggregate only (currently) |
| Masking | Hard removal (zero / edge deletion) | Soft gradient signal |

In the order_management runs, both methods agree: Orders aggregate features (`n_products`,
`n_items`, `total_weight`) are the most informative inputs. LOO additionally reveals that
**Packages→Items edge connectivity** is the dominant structural signal — something IG cannot
capture because it operates on features, not topology.

---

## 4. Counterfactual Explanations

### What it does

Counterfactual explanations answer: *what does a similar-looking order with a very different
predicted remaining time look like?* This shifts from "why this prediction?" to "what would need
to be different to get a better outcome?".

Implementation: `find_counterfactuals()` searches the test set for traces in the **opposite
outcome quartile** (slowest → fastest, or vice versa) that are structurally most similar to the
query trace.

### Dissimilarity metric

Four components, equal weight (range [0, 4] total):

| Component | Formula | What it captures |
|---|---|---|
| `d_feat` | Mean per-type avg(L2 dist, cosine dist) between node feature vectors | Feature similarity |
| `d_type` | Jaccard distance over per-type node counts | Object composition similarity |
| `d_edge` | Jaccard distance over per-edge-type counts | Process structure similarity |
| `d_struct` | `\|E(g1) − E(g2)\| / max(E(g1), E(g2))` | Overall graph size similarity |

**Prefix-length stratification**: candidates are pre-filtered to traces within
`max(2, 0.2 × N_events(query))` events of the query, preventing stage-mismatch (a 3-event trace
matching a 15-event trace just because both are in Q4). The window doubles progressively
(up to 4× the original) if fewer than `min_candidates=5` candidates are found.

### Single trace — `explain_counterfactual(order_id)`

1. Identifies the query trace's outcome quartile.
2. Defines the target band as the opposite quartile (or an explicit `(low_s, high_s)` tuple).
3. Filters candidates by band + prefix-length window.
4. Ranks by total dissimilarity; returns top-3.
5. Prints a comparison table (node-type counts, edge-type counts, predicted times).
6. Saves a side-by-side bar chart of node-type compositions (`cf_node_type_comparison.png`).

### Aggregated

No aggregate counterfactual entry point currently exists. See §5 for the improvement proposal.

---

## 5. Proposed Improvements

### Technique gaps

| Gap | Impact | Recommendation |
|---|---|---|
| No single-trace IG entry point | Users cannot get gradient-based feature attribution for a specific order alongside LOO | Add `explain_trace_ig(order_id)` — one backward pass, output bar chart per node type |
| No aggregate counterfactual | Cannot characterize *what systematically differs* between fast and slow orders | Add `explain_aggregate_counterfactuals(n=50)`: cluster slow orders, find their CFs, report mean structural differences |
| LOO and IG are disconnected | LOO costs O(all nodes + all edges) per trace even when most nodes are irrelevant | Use IG top-K attribution to pre-select candidate nodes, then run LOO only on those |

### Masking strategy (edge LOO)

Feature LOO zeroes the feature vector — for z-normalized features this sets them to the training
mean, which is a reasonable neutral baseline. **Edge LOO is more problematic**: deleting an edge
changes graph topology, a much stronger perturbation than feature masking. This explains why
`Packages→Items` edges produce 53–55h shifts even though the feature-level attribution for Items
and Packages is near zero — topology change and feature change are qualitatively different
interventions but both measured on the same `|Δpred|` scale. A cleaner design would report
node/edge and feature importances on separate scales, or normalize edge shifts by the expected
impact of removing one edge from a graph of that size.

### Readable event names in LOO output

Events are currently printed as `Events[1] [type_0=3.16, type_1=-0.33]`. The one-hot dimensions
map directly to event types in the order management process (PlaceOrder, ConfirmOrder, PickItem,
etc.) but this mapping is not decoded in the output. Adding a lookup from the event encoding in
`ocel.csv` / `tensor_dict.json` would make explanations read:
> *"PlaceOrder event shifted prediction by +7.1h"*
instead of raw indices.

### Depth-stratified aggregation

`explain_aggregate` pools LOO shifts across all prefix depths. A 3-event prefix has ~20 nodes
while a 15-event prefix may have 60+, so their raw `|Δpred|` magnitudes are not directly
comparable. Adding a `depth_bin` breakdown (e.g. 1–3, 4–6, 7–9, 10+ events) to the aggregate
output would reveal whether the relative importance of node types changes as the process matures
— for example, Employee nodes might matter more at the start of a trace when the assignee is
being decided.

### Fidelity metric units

`evaluate_explanation_quality` reports Fidelity+ and Fidelity− in **seconds** (raw de-normalized
model output). Dividing by 3600 before printing and saving to CSV would align them with every
other metric in the system (hours), making `aggregate_metrics.csv` directly comparable to test
MAE and RMSE values without manual conversion.

### Counterfactual quality validation

The counterfactual search currently has no fidelity metric of its own — there is no measure of
whether the returned CF is a "good" explanation (i.e. that the feature differences between query
and CF actually account for the prediction difference). A simple addition: after finding the top-1
CF, run LOO on the *delta features* (features that differ most between query and CF) and verify
they produce shifts in the direction of the prediction gap.
