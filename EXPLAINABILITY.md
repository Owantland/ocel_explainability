# Explainability Layer

This document describes the four post-hoc explainability techniques implemented in `explainer.py`
for the HGT remaining-time regression model. All techniques are applied to a **trained, frozen
model** and treat it as a black box — no retraining or model modification is involved.

The explainability goal is: *given a predicted remaining time for an order, which nodes, edges,
and features drove that prediction, and how faithful is the explanation to the model's actual
behaviour?*

Three of the four techniques address regression (the primary task). A classification variant of
Leave-One-Out exists in code for the binary-KPI setting, but — confirmed via a full audit of
`explainer.py`, 2026-07-12 — that whole path is unreachable in practice: no active dataset config
sets `kpi_type: 1`, and its methods have no other caller in the repo. Treat it as dead scaffolding,
not a supported second mode.

**Document scope/staleness note (added 2026-07-12):** this document was last substantively edited
2026-07-02 and re-verified against current code this session — several claims had drifted (see
inline "updated"/"RESOLVED" notes throughout) and are now corrected. Worked numeric examples
throughout are from `order_management` at a specific historical checkpoint and **predate** the
`weight_decay`/`num_layers`/epoch-patience fixes documented in `TRAINING_VS_HOEG.md` and
`EXPLAINABILITY_DEPTH.md` — they illustrate *how each method works*, not current citable numbers.
Check `EXPLAINABILITY_DEPTH.md` for the most recently verified figures, or re-run the relevant
`explain_*` method fresh, before citing any specific number from this document in the thesis.
Every technique described here is dataset-generic (verified working on both `order_management` and
`logistics`) even though the worked examples below only show `order_management`.

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

### Single trace — `explain_trace(order_id, top_k=5, save_dir=None, n_events=None)`

Locates the last-event graph for the given order ID in the test set by default — **or, since a
later addition than this doc's original writing, an arbitrary earlier prefix via `n_events`** (the
graph with exactly that many events observed, not just the final one) — runs the full LOO
pipeline, and produces:

- **Console output**: ranked tables of top-k nodes, edges, and features with their hour shifts.
- **`feat_importance_{node_type}.png`**: horizontal bar charts for seed and top-neighbor features
  (red = shift exceeds 1 std of training targets, ~115h; gray = smaller).
- **`node_type_summary.png`**: cumulative LOO shift per node type + node count per type.
- **`explanation_subgraph.png`**: the top-k nodes and edges as a NetworkX graph, with node size
  proportional to shift magnitude and red highlighting for large-shift elements.
- **Fidelity metrics** (see §2 — corrected 2026-07-12, this previously pointed to §3/InputXGradient
  instead of the actual Fidelity Metrics section, a pre-existing cross-reference error).

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

**Example (cant=1000, n=50) — predates the weight_decay/num_layers/epoch-patience fixes; kept for
mechanism illustration only, NOT a current citable figure (see caveat below):**

```
Characterization score : 0.887 ± 0.137
Node sparsity          : 61.4% ± 11.1%
Edge sparsity          : 90.7% ± 4.3%
```

The aggregate confirms that explanations are compact (only ~10% of edges are needed) and that
the explanation subgraph genuinely drives the prediction.

**Staleness note (added 2026-07-12):** this specific 0.887 figure is superseded — per
`EXPLAINABILITY_DEPTH.md`'s more recent, verified tracking, the characterization score moved
substantially across retrains (0.7008, then a spuriously perfect 1.0000 that turned out to signal
a dead-feature bug rather than a good explanation, then a verified 0.8033 at n=50 after that bug
was fixed, then **0.8426 ± 0.1818 at the full n=235 last-event test set** — the most recent
verified figure as of that document). Do not cite 0.887 in the thesis; check
`EXPLAINABILITY_DEPTH.md` for the latest verified number, or re-run `explain_aggregate()` fresh
against the current checkpoint before citing anything from this section.

### Depth-stratified — `explain_loo_by_depth(n_traces=200)` (added 2026-07-13)

Unlike `explain_aggregate()` above (last-event graphs only), this runs full LOO across ALL test
prefixes at every depth, bins by prefix length using the same `_DEPTH_BINS` as
`explain_feature_attribution`'s depth-stratified mode, and produces a node-type × depth-bin
heatmap (`loo_depth_heatmap.png`) + CSV (`loo_depth_importance.csv`) instead of one pooled-across-
all-depths mean — answering whether a node type's relative importance shifts as a trace matures.
Defaults to a bounded 200-prefix sample rather than the whole test set, since full LOO ablation is
far more expensive per graph than feature attribution's single backward pass.

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
dimension. **IntegratedGradients is also supported as a second attribution method** (see below),
not just InputXGradient — updated 2026-07-12, this section previously described InputXGradient
only.

The computation is implemented via PyTorch Geometric's `Explainer` wrapping Captum
(`_get_pyg_explainer(method)` builds/caches a `torch_geometric.explain.Explainer` around
`CaptumExplainer('InputXGradient')` or `CaptumExplainer('IntegratedGradients')`;
`_compute_attribution_for_graph(graph, method)` runs it for one graph and returns a signed
`{node_type: [N_nodes, F_features]}` attribution dict) — **not** a hand-rolled
`requires_grad_(True)`/manual-backward-pass implementation as an earlier version of this doc
described.
- Attributions are mean-pooled over nodes within each type → one vector per type per graph.
- **Positive** attribution: increasing that feature increases predicted remaining time.
- **Negative** attribution: increasing that feature decreases predicted remaining time.

### Single trace — `explain_trace_ig(order_id, methods=('InputXGradient','IntegratedGradients'), n_events=None, top_k_display=10)`

**Updated 2026-07-12 — this entry point now exists; it did not when this section was originally
written.** Mirrors `explain_trace()`'s single-order framing but for gradient attribution instead of
LOO: runs the chosen method(s) on one order's graph and produces the same style of per-node-type
bar charts/CSV as the aggregate path below, scoped to a single trace. Like `explain_trace`, it
accepts `n_events` to explain an arbitrary (non-final) prefix depth, not just the last event.

### Aggregated — `explain_feature_attribution(n_traces=None, methods=('InputXGradient',), depth_stratify=True)`

Runs the chosen attribution method(s) across all last-event test graphs (or a subset), averages signed and absolute
attributions across graphs, and outputs:

- **Per-type bar charts** (`ig_{node_type}_importance.png`): features sorted by mean absolute
  attribution; blue bars = positive contribution, orange = negative.
- **Heatmap** (`ig_heatmap.png`): all node types × feature dimensions in one view.
- **CSV** (`ig_attribution.csv`): `[node_type, feature_dim, feature_name, mean_signed, mean_abs]`.
- **Perturbation fidelity check**: zeroes the top-K and bottom-K features by attribution across all
  graphs, measures mean `|Δpred|` for each group — top-K should shift predictions more; when
  multiple `methods` are passed, this is reported per method with a PASS/FAIL and a gap comparison
  across methods (added since this doc's original example below was generated).
- **Depth-stratified breakdown** (`depth_stratify=True`, the default) — added since this doc's
  original example below: also calls `_explain_attribution_by_depth()`, producing a depth-bin ×
  feature-dim heatmap for both the viewpoint node type and `Events`, addressing what §6's
  "Depth-stratified aggregation" gap originally asked for (that gap is resolved for feature
  attribution; LOO's `explain_aggregate` still pools across all prefix depths without a depth-bin
  breakdown — see §6).

**Example (cant=1000, n=356 last-event graphs) — order_management, predates the
weight_decay/num_layers/epoch-patience fixes documented in `TRAINING_VS_HOEG.md`; illustrative of
the mechanism only, not a current citable figure:**

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
2. Defines the target band as the opposite quartile — on the side controlled by `direction` — or an
   explicit `(low_s, high_s)` tuple.
3. Filters candidates by band + prefix-length window.
4. Ranks by total dissimilarity; returns top-3.
5. Prints a comparison table (node-type counts, edge-type counts, predicted times).
6. Saves a side-by-side bar chart of node-type compositions (`cf_node_type_comparison.png`).

### Direction — `direction='lower'|'higher'` (added 2026-07-14)

`find_counterfactuals()`, `explain_counterfactual()`, and `explain_aggregate_counterfactuals()` all
accept `direction='lower'` (default): which side of the query's own prediction `target_band='opposite'`
searches. `'lower'` (the only behavior before this parameter existed, still the default) looks for
traces below Q1 — or below the query's own value if the query is already in the fastest quartile.
`'higher'` is the mirror image: traces above Q3, or above the query's own value if the query is
already in the slowest quartile. Invalid values raise `ValueError`. Only meaningful for
`target_band='opposite'` — a no-op when an explicit `(low_s, high_s)` tuple is passed, since the
tuple already fully specifies the band.

**Sign note**: `explain_aggregate_counterfactuals()`'s reported `predicted_hours_gap` is always
`query_predicted_hours − cf_predicted_hours` (unchanged formula). With `direction='higher'` the
candidate's prediction exceeds the query's, so this gap comes out **negative** — expected, not a
bug; the sign itself tells you which direction was searched.

### Minimum predicted-time gap — `min_gap_hours` (added 2026-07-13)

`find_counterfactuals()`, `explain_counterfactual()`, and `explain_aggregate_counterfactuals()` all
accept `min_gap_hours=0.0`: a candidate is only eligible if
`|query_predicted_hours − candidate_predicted_hours| ≥ min_gap_hours`. This guards against a
structurally-closest candidate being returned as "the" counterfactual when its predicted outcome is
barely different from the query's — the point of a counterfactual is a *meaningfully* different
outcome, not just a nearby one. The default (`0.0`) is a no-op, preserving prior behavior exactly.

It composes as an independent, additional constraint on top of the existing band filter (including
`target_band='opposite'`'s implicit `p < query_pred`), applied inside `band_and_candidates()` so it
automatically covers both the last-event and `n_events=<int>` code paths.

**Hard filter, never relaxed**: unlike the prefix-length window (which widens progressively when
too few candidates survive), `min_gap_hours` is never loosened by that widening loop or by the
last-resort skip-length-gate fallback. A threshold no candidate can clear yields fewer than
`n_results` results — down to none — rather than silently substituting a below-threshold candidate.
`explain_counterfactual()` prints a threshold-specific message ("No counterfactuals found with a
predicted-time gap ≥ Xh.") in that case; `explain_aggregate_counterfactuals()` counts it as an
ordinary per-query failure, same as any other "no counterfactual found" case.

### Aggregated — `explain_aggregate_counterfactuals(n_traces=50, target_band='opposite', min_gap_hours=0.0, direction='lower')` (added 2026-07-13)

Runs `find_counterfactuals()` across `n_traces` last-event query traces (same sampling convention
as `explain_aggregate()`), retrieves each query's single best counterfactual, and aggregates the 4
dissimilarity components plus the predicted-hours gap across all queries — mean ± std, saved to
`aggregate_cf_dissimilarity.csv` plus a component-breakdown bar chart
(`aggregate_cf_components.png`). Answers "what does a counterfactual typically look like across
this dataset?" rather than one worked example at a time. Cost note: each `find_counterfactuals()`
call recomputes predictions for the entire last-event candidate pool (needed to determine the
opposite-outcome quartile), so this is O(n_traces × pool_size) forward passes, not O(n_traces).

---

## 5. GNNExplainer Comparison (added since this document was originally written)

Not one of the original four techniques, but a fifth capability added later: a comparison between
LOO's node-importance ranking and PyG's `GNNExplainer` (a learned soft node-feature mask), used as
an independent cross-check on LOO rather than a replacement for it.

- **`explain_gnn_subgraph(order_id, epochs=200, lr=0.01, top_k=5, n_events=None)`** — runs
  GNNExplainer's learned node-feature mask for one trace, reports top node types/features.
  Edge importance is unavailable for this method: `HGTConv` fuses all relations into a single
  `propagate()` call over a manually-constructed bipartite edge index, so PyG's edge-mask
  injection has nothing per-relation to patch onto (node masking works because it intercepts
  `x_dict` directly instead).
- **`compare_loo_gnn_importance(order_id, top_k=5, n_events=None)`** — single-trace: reuses
  `explain_trace()` and `explain_gnn_subgraph()`, builds a rank/overlap comparison table + styled
  PNG. LOO's score (signed hours) and GNNExplainer's score (unitless [0,1] soft mask) are **not
  comparable in magnitude** — only rank/overlap is meaningful, and the method explicitly caveats
  this rather than plotting them on one axis.
- **`compare_loo_gnn_importance_aggregate(n_traces=235, top_k=5, epochs=200, lr=0.01)`** —
  full-test-set version. Handles a real PyG limitation directly: `GNNExplainer._initialize_masks()`
  crashes on graphs with a legitimately-empty edge type, so such traces are explicitly skipped
  (logged, not silently dropped) rather than crashing the whole aggregate run.
  **Re-run 2026-07-14** (order_management, `n_traces=235` → 218 used, 17 skipped for empty edge
  type, after the GNNExplainer seeding fix below): mean top-5 node overlap is **2.54/5** (σ=0.87) —
  still partial, not strong, agreement (distribution: 0/5 in 0.0% of traces, 1/5 in 11.0%, 2/5 in
  37.2%, 3/5 in 39.9%, 4/5 in 10.6%, 5/5 in 1.4%). This *supersedes* an earlier `n=235` figure of
  2.33/5 (per `EXPLAINABILITY_DEPTH.md`), which predates the seeding fix. The per-type ranking also
  changed materially: `Events` is now GNNExplainer's #1 too, not just LOO's (previously reported as
  only GNNExplainer's #4) — `Items` #2, `Orders` #3, `Packages` #4, `Products` #5 for both methods,
  a much closer type-level agreement than previously described.
- **`explain_gnn_edge_importance_experimental(...)`** — a separate, explicitly experimental
  edge-importance method (reweights post-softmax `HGTConv` attention via `set_masks()`/
  `clear_masks()`, not true ablation) kept apart from the production `explain_gnn_subgraph` path
  pending validation. Not comparable to LOO's edge ranking; not called by any pipeline script.

### GNNExplainer as primary identifier, LOO as targeted impact estimator (added 2026-07-13)

A sixth capability, distinct from the comparison methods above: instead of running LOO and
GNNExplainer independently and comparing their rankings, GNNExplainer's node ranking **is** the
explanation, and LOO is reduced to a **targeted** impact estimate — computing `|Δpred|` only for
the node instances GNNExplainer identified, not an exhaustive sweep over the whole graph. This is
additive, not a replacement for `explain_trace()`/`explain_aggregate()`, which remain the only
source of edge importance (GNNExplainer has none on this architecture, see `explain_gnn_subgraph`'s
entry above) and of full exhaustive-LOO rankings.

- **`explain_gnn_primary(order_id, top_k=5, epochs=200, lr=0.01, n_events=None)`** — runs
  GNNExplainer, ranks node instances by its per-node aggregated soft mask (reusing the same
  per-instance aggregation `compare_loo_gnn_importance()` uses), takes the top-k (excluding the
  seed), then runs LOO's zero-and-repredict shift on exactly those nodes — individually (one shift
  per node, same table format as `explain_trace()`) and jointly (`evaluate_explanation_quality()`'s
  Fidelity+/-/characterization, reused as-is, over the identified set masked out together). Node-only
  scope: the console output explicitly states edge importance isn't available in this pathway.
- **`explain_gnn_primary_aggregate(n_traces=50, top_k=5, epochs=200, lr=0.01)`** — same idea across
  `n_traces` last-event traces (same deterministic sampling as `explain_aggregate()`), reporting
  mean±std joint Fidelity+/-/characterization (directly comparable to `aggregate_metrics.csv`'s
  exhaustive-LOO numbers — "does the cheaper, GNNExplainer-driven explanation lose fidelity vs. full
  LOO?") and per-node-type selection frequency + mean shift. Shares
  `compare_loo_gnn_importance_aggregate()`'s empty-edge-type skip guard (same PyG
  `GNNExplainer._initialize_masks()` limitation).
- **Fidelity- fix specific to node-only explanations**: `evaluate_explanation_quality()`'s Fidelity-
  ("keep ONLY the explanation") strips every edge not explicitly passed in `edge_importances` — with
  no edge signal from GNNExplainer, passing `edge_importances=[]` directly would strip *all* edges,
  making Fidelity- measure "can the model run with zero edges" rather than "does this node selection
  reproduce the prediction" (confirmed empirically: Characterization collapsed to 0.48 on a real
  order until fixed, vs. 0.95 after). `explain_gnn_primary()`/`_aggregate()` instead pass the real
  edges connecting the identified nodes + seed (`_induced_edges()`, with a placeholder zero
  importance score) so Fidelity- reflects real graph topology.
- **Verified working on both datasets** (order_management, logistics): identified nodes largely
  agree with `compare_loo_gnn_importance()`'s independent GNNExplainer ranking for the same order.
  An early 10-trace smoke sample at `epochs=30` (well below the `epochs=200` default) put
  characterization at a misleadingly low 0.19 (order_management) / 0.02 (logistics) — under-optimized
  masks, not a fair reading of the method. **Re-run 2026-07-14 at the real citable scale
  (`n_traces=50`, `epochs=200`, matching the default, after the GNNExplainer seeding fix below)**:
  - order_management: Characterization **0.6845 ± 0.1817** (n=47/50 traces — 3 skipped, empty edge
    type; `Events` selected in 53.2% of top-5 slots) — much closer to `explain_aggregate()`'s
    exhaustive-LOO figure (~0.84, `EXPLAINABILITY_DEPTH.md`) than the smoke test suggested. At a
    proper epoch count, GNNExplainer-primary is a meaningfully more competitive, cheaper alternative
    to exhaustive LOO than first believed.
  - logistics: Characterization **0.2437 ± 0.2295** (n=50/50 traces; `Events` selected in 44.8% of
    top-5 slots, `HandlingUnit` in 40.8%) — also a large improvement over the smoke figure, but still
    notably lower than order_management's, consistent with logistics being the harder dataset
    throughout this project (deeper graphs, tighter reachability margins — see `TRAINING_VS_HOEG.md`).
  Full per-trace figures: `aggregate_gnnprimary_metrics.csv` under each dataset's
  `explainer_outputs/*/aggregate_gnnprimary/`. **These figures were independent-sample means with
  large recorded std devs, not yet checked for statistical significance — see "Paired statistical
  validation of the fidelity gap" below for the rigorous version.**

### ~~GNNExplainer non-determinism~~ RESOLVED (2026-07-14)

PyG's `GNNExplainer._initialize_node_mask()` draws its initial mask parameters via
`torch.randn(N, F) * 0.1` off the *global* torch RNG stream, and nothing in `explainer.py` ever
seeded it — unlike `training.py`, which consistently sets `torch.manual_seed(42)` (plus
`random.seed(42)`/`np.random.seed(42)`) for model training and data-split reproducibility. Confirmed
empirically: six repeated calls to `explain_gnn_primary(1781, epochs=50)` in the same process
returned a stable top-4 identified nodes but a genuinely different 5th-ranked node in 4 of the 6 runs.

Fixed by `_run_gnn_explainer()`, a shared helper that calls `torch.manual_seed(42)` immediately before
every GNNExplainer invocation (reset-per-call, not once-per-script, so a standalone single-trace call
reproduces the same result whether it's the first or the Nth GNNExplainer call in a session — matching
what it would produce inside an aggregate loop too). Wired into all four raw call sites:
`explain_gnn_subgraph()`, `compare_loo_gnn_importance_aggregate()`, `explain_gnn_primary()`, and
`explain_gnn_primary_aggregate()` (`compare_loo_gnn_importance()` inherits the fix transitively via
`explain_gnn_subgraph()`). The separate custom-`randn` mask in the explicitly-experimental
`explain_gnn_edge_importance_experimental()` was seeded the same way for consistency.

Verified: the original 6x-repetition test is now stable; results also match byte-for-byte across
independent process invocations; `explain_gnn_subgraph`/`compare_loo_gnn_importance`/`explain_gnn_
primary_aggregate` all reproduce identically on repeat calls; the experimental method's own sanity
check still passes; and different orders still produce genuinely different (non-collapsed) masks, so
the fix removes run-to-run noise without flattening genuine cross-order variation. Since this changes
GNNExplainer's entire random-initialization sequence, the `compare_loo_gnn_importance_aggregate`
overlap figure and the `explain_gnn_primary_aggregate` characterization figures cited above both
predate this fix and were re-verified under the new seeded regime — see the updated numbers in the
bullets above and below.

### Paired statistical validation of the fidelity gap (added 2026-07-17)

The fidelity comparison above (exhaustive LOO's ~0.84/0.68 vs. GNNExplainer-primary's 0.68/0.24
Characterization) was reported as two **independent**-sample means with large recorded std devs —
never checked for statistical significance, and never computed on a genuine per-trace **paired**
basis, even though `explain_aggregate()` and `explain_gnn_primary_aggregate()` already sample the
identical `last_event_graphs[:n_traces]` ordering. The existing `aggregate_metrics.csv` (LOO) keys
its rows by a bare positional index, not `order_id`, so the two existing aggregate CSVs couldn't
safely be joined after the fact to exploit that.

`validate_fidelity_comparison(n_traces=50, epochs=200)` computes both pathways' Fidelity+/-/
Characterization for the *same* trace in the *same* pass, then runs a paired Wilcoxon signed-rank
test (primary — no normality assumption on a bounded/possibly-skewed metric) and a paired t-test
(secondary) on the per-trace difference. Run on both datasets at the same `n_traces=50` scale as the
figures above:

| Dataset (n paired) | Metric | LOO mean | GNNExp-primary mean | Wilcoxon *p* | Verdict (α=0.05) |
|---|---|---|---|---|---|
| order_management (n=47) | Characterization | 0.8147 ± 0.1819 | 0.6845 ± 0.1797 | 1.85e-4 | significant |
| order_management (n=47) | Fidelity+ | 102.69h ± 46.91h | 84.70h ± 36.62h | 6.95e-3 | significant |
| order_management (n=47) | Fidelity− | 17.94h ± 13.36h | 40.92h ± 31.65h | 7.87e-5 | significant |
| logistics (n=50) | Characterization | 0.3957 ± 0.2173 | 0.2437 ± 0.2272 | 1.54e-3 | significant |
| logistics (n=50) | Fidelity+ | 246.10h ± 4.76h | 131.25h ± 138.69h | 4.88e-7 | significant |
| logistics (n=50) | Fidelity− | 594.51h ± 454.18h | 343.81h ± 507.97h | 1.49e-4 | significant |

All six (2 datasets × 3 metrics) are significant at α=0.05 under both tests — the previously-observed
fidelity gap is real, not an artifact of comparing two noisy independent means. `n` excludes the same
empty-edge-type traces `explain_gnn_primary_aggregate()` already skips (3 for order_management, 0 for
logistics — consistent with the unpaired runs above).

Incidental new finding: logistics's own **exhaustive-LOO** Characterization (0.3957) is markedly
lower than order_management's (0.8147) — not a previously-cited figure (prior citations only covered
order_management's exhaustive-LOO Characterization, ~0.84 at n=235). Consistent with logistics being
the harder dataset throughout this project, but worth citing directly now that it's been measured.

Verification: spot-checked two paired rows (order #1773, order_management) against direct
`explain_trace()`/`explain_gnn_primary()` calls — exact match, since both LOO and the now-seeded
GNNExplainer are fully deterministic. Full per-trace CSV: `fidelity_validation_paired.csv` under each
dataset's `explainer_outputs/*/fidelity_validation/`.

---

## 6. Proposed Improvements

### Technique gaps

| Gap | Impact | Recommendation |
|---|---|---|
| ~~No single-trace IG entry point~~ | **RESOLVED** — `explain_trace_ig(order_id)` now exists (§3). | — |
| ~~No aggregate counterfactual~~ | **RESOLVED (2026-07-13)** — `explain_aggregate_counterfactuals(n_traces=50, target_band='opposite')` now exists (§4); reports mean/std of the 4 dissimilarity components + predicted-hours gap across N query traces, not a cluster-based approach as originally proposed here, but answers the same underlying question. | — |
| LOO and IG are disconnected | LOO costs O(all nodes + all edges) per trace even when most nodes are irrelevant | Use IG top-K attribution to pre-select candidate nodes, then run LOO only on those |
| ~~LOO's `explain_aggregate` doesn't depth-stratify~~ | **RESOLVED (2026-07-13)** — `explain_loo_by_depth(n_traces=200)` now exists (§1); node-type × depth-bin heatmap, not a feature-dim one (see rationale in §1). | — |

### Masking strategy (edge LOO)

Feature LOO zeroes the feature vector — for z-normalized features this sets them to the training
mean, which is a reasonable neutral baseline. **Edge LOO is more problematic**: deleting an edge
changes graph topology, a much stronger perturbation than feature masking. This explains why
`Packages→Items` edges produce 53–55h shifts even though the feature-level attribution for Items
and Packages is near zero — topology change and feature change are qualitatively different
interventions but both measured on the same `|Δpred|` scale. A cleaner design would report
node/edge and feature importances on separate scales, or normalize edge shifts by the expected
impact of removing one edge from a graph of that size.

### ~~Readable event names in LOO output~~ RESOLVED (2026-07-13)

Events are now printed as `Events[6](PaymentReminder)` in `explain_trace`'s console output (top
nodes, top-3-per-type, and top edges), and `top_nodes_per_type.csv` gained a new `activity_name`
column (empty for non-`Events` rows). Implemented by reusing the existing
`_decode_event_types_with_indices()` decoder — previously only wired into the counterfactual
plotting functions — rather than building a new lookup; verified working on a real order on both
`order_management` and `logistics`.

**Coverage extended (2026-07-17)**: `explain_trace()` was the only "top nodes" surface with this —
`explain_gnn_primary()`'s console output/`gnnprimary_node_importance.csv` and the Streamlit
dashboard's Local-tab "Top nodes" table (both LOO and GNNExplainer-primary modes) still showed raw
`Events[N]` indices. Both now reuse the same `_decode_event_types_with_indices()` decoder (the
dashboard re-derives it from the graph directly, since the underlying `node_importances` tuples don't
carry it) — verified to produce byte-identical activity names to `explain_trace()`'s own output for
the same order. Deliberately still Events-only: `Customers`/`Employees` are also fully one-hot
(company name / department respectively) and could get the same treatment later, but `Items`/
`Products`/`Packages`/`Orders` are purely numeric with no identity to decode — out of scope here.

### ~~Depth-stratified aggregation~~ RESOLVED (2026-07-13)

`explain_aggregate` (LOO) pools shifts across all prefix depths. A 3-event prefix has ~20 nodes
while a 15-event prefix may have 60+, so their raw `|Δpred|` magnitudes are not directly
comparable. `explain_feature_attribution` (IG) already had this via
`_explain_attribution_by_depth()`/`depth_stratify=True` (§3); LOO now has its own analogue,
`explain_loo_by_depth()` (§1) — a node-type × depth-bin heatmap (not feature-dim × depth-bin like
IG's, since LOO's natural aggregation unit across a whole prefix is per-node-type, matching what
this gap's own framing asked for: "whether the relative importance of node types changes as the
process matures").

### ~~Fidelity metric units~~ RESOLVED (2026-07-13)

`evaluate_explanation_quality` now divides Fidelity+/Fidelity− by 3600 at the source (before
they're returned, printed, or written to `aggregate_metrics.csv`), aligning them with every other
metric in the system (hours) — `aggregate_metrics.csv` is now directly comparable to test MAE/RMSE
without manual conversion. Verified no other file in the repo reads `fidelity_plus`/
`fidelity_minus` or `aggregate_metrics.csv`, so nothing downstream depended on the old unit.

### Counterfactual quality validation

The counterfactual search currently has no fidelity metric of its own — there is no measure of
whether the returned CF is a "good" explanation (i.e. that the feature differences between query
and CF actually account for the prediction difference). A simple addition: after finding the top-1
CF, run LOO on the *delta features* (features that differ most between query and CF) and verify
they produce shifts in the direction of the prediction gap.
