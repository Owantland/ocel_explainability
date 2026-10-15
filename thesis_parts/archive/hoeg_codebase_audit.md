# Audit: OCPPM-master (HOEG's reference codebase) vs. how this thesis currently describes HOEG

`OCPPM-master/` is Smit et al. 2024's own released implementation of HOEG — this thesis's direct
comparison baseline. Everything the project previously knew about HOEG's architecture came from the
paper's *prose* (already partly verified against the PDF text in the `related_work.txt` review: "k-
dimensional GNN, not HGT"). This is the first audit of the actual *code*. Same method as the Zhai et
al. `GNN-land-use-main/` audit: read the real implementation rather than trusting the paper's
narrative description alone. Findings ordered by how much they change existing text.

## 1. NEW, more precise detail: HOEG's "k-dimensional GNN" is a homogeneous operator retrofitted via PyG's generic `to_hetero()` — not a natively heterogeneous architecture

`models/definitions/geometric_models.py`'s `HigherOrderGNN` (the actual k-GNN, Morris et al. 2019,
that HOEG uses) has a plain `forward(self, x, edge_index, batch)` signature built entirely from
`pygnn.GraphConv` — a homogeneous operator with no node/edge-type awareness whatsoever. Every one of
HOEG's three experiment scripts does exactly this (via the shared
`utilities/hetero_experiment_utils.py:41`):

```python
model = HigherOrderGNN(hidden_channels=hidden_dim, ...)
model = pygnn.to_hetero(model, hoeg_config["meta_data"])
```

`to_hetero()` is PyG's generic wrapper that duplicates a homogeneous layer once per node/edge type and
message-passes accordingly — that's how HOEG becomes "heterogeneous-capable," not a purpose-built
type-aware architecture the way HGT is (per-type projection matrices, cross-type attention, designed
in from the start).

**Where this should feed in**: `related_work.txt` §2.1.3 already has an identified gap (never names
"HOEG" or its architecture explicitly — see `related_work_review.md` Finding 3). This detail should
be added there, and it directly strengthens the Models chapter's planned "Why HGT over other
heterogeneous architectures" argument: it's not just "a different GNN," it's "a homogeneous GNN made
heterogeneous-shaped by a generic wrapper, vs. an architecture designed for heterogeneity from the
ground up."

## 2. Confirmed via code (previously only assumed from the paper/memory): no object-object edges, anywhere in HOEG's actual data

Checked the `meta_data` edge-type list actually used in **all three** of HOEG's experiment configs:

- **BPI17** (`experiments/loan_application/feature_encodings/hoeg/run_hoeg_bpi17.py`):
  `[(event,follows,event), (application,interacts,event), (offer,interacts,event)]`
- **OTC** (`experiments/order_management/feature_encodings/hoeg/run_hoeg_otc.py`):
  `[(event,follows,event), (order,interacts,event), (item,interacts,event), (package,interacts,event)]`
- **Case study/FI** (`experiments/case_study/feature_encodings/hoeg/run_hoeg_cs.py`):
  `[(event,follows,event), (krs,interacts,event), (krv,interacts,event), (cv,interacts,event)]`

**Never** an object-to-object edge type, in any of the three. `utilities/hetero_data_utils.py`'s
`AddObjectSelfLoops` transform (applied in every run script's transform pipeline) only ever creates
*self*-loops on object nodes (`data[ot, 'updates', ot].edge_index`, a node connecting to itself) —
never a genuine edge between two distinct object instances.

**Where this should feed in**: same `related_work.txt` §2.1.3 gap as Finding 1 — this can now be
stated as code-confirmed fact rather than inferred from the paper's prose.

## 3. Confirmed and strengthened via code: num_layers is fixed at 2 across HOEG's entire experimental suite, not just the one dataset previously assumed

Grepped every `run_hoeg_*.py` script and `utilities/hetero_experiment_utils.py` for
`no_messagepassing_layers` (the depth parameter on `HigherOrderGNN`/`HeteroHigherOrderGNN`): **zero
hits, in any script.** It's never overridden anywhere — all three of HOEG's experiments (BPI17, OTC,
case-study) use the class's hardcoded default of 2, and each script's own hyperparameter sweep only
varies `lr` and `hidden_dim`, never depth.

This thesis currently justifies Order Management's own `num_layers=2` as "matching HOEG's fixed
message-passing depth" (`dataset.txt`/`thesis_structure.txt`) — previously grounded only in a
partial/ambiguous reading of the paper's own Table 5 (which lists both a used value "2" and a
considered range "[2,10]," without making clear which applies to the actually-reported experiments).

**Where this should feed in**: no rewording needed in `dataset.txt`/`thesis_structure.txt` — the
existing claim is now fully code-confirmed, and could optionally be stated even more confidently
("HOEG never tunes message-passing depth in any of its three reported experiments" rather than just
"HOEG's fixed depth").

## 4. NEW finding, not previously documented anywhere in the project: HOEG predicts remaining time at every event node in a graph simultaneously, not once per prefix

Checked `utilities/hetero_training_utils.py`'s training loop:
`loss = loss_fn(outputs[target_node_type], labels)` where `labels = batch[target_node_type].y` — no
masking to a single "current"/"last" event anywhere. Combined with `"graph_level_target": False` and
`"target_node_type": "event"` in every one of HOEG's configs, this means **each of HOEG's graphs
represents one whole case, with every event node in it carrying its own remaining-time label, and the
model predicts all of them at once** in a single forward pass — not one graph per prefix with a
single target at a viewpoint node, which is how this thesis's own pipeline is built.

This is a genuine **task-formulation** difference, not just an architecture difference, and it isn't
stated anywhere in the current `related_work.txt`, `thesis_structure.txt`, or memory.

**Where this should feed in**: this needs a new sentence wherever HOEG is introduced as the "direct
comparison baseline" — there isn't an existing gap to slot it into, unlike Findings 1–3. Suggested
framing: the *encoding* comparison (heterogeneous node types, no object-object edges, k-GNN vs. HGT)
is a fair one-to-one comparison; the *task formulation* (whole-case multi-target-per-graph vs.
per-prefix single-target-per-graph) isn't identical, and saying so explicitly makes the comparison's
scope more defensible, not weaker — it heads off a "wait, is this really the same task?" objection
from a careful reader before it's raised.

## 5. Minor: HOEG's OTC dataset is thematically similar to, but not the same as, this thesis's own `order_management` dataset

OTC's object types are `item`, `order`, `package` (3 types, matching HOEG's own Table 6, confirmed
earlier this session). This thesis's `order_management` database has 6 object types (`Orders`,
`Items`, `Packages`, `Customers`, `Employees`, `Products` — confirmed via
`dataset_summary_table.py`'s structural summary). Both are synthetic "order management"/order-to-cash
-style OCELs, but they are not the same dataset or generator. Worth a one-line clarifying note if
`dataset.txt`'s eventual §3.2 (Order Management dataset) ever draws a direct comparison to HOEG's
OTC, so a reader doesn't assume they're identical.

## Summary: does this change how the HOEG reference should be handled?

Yes, in three concrete ways:
1. The architecture description should be more specific — "k-dimensional GNN made heterogeneous via
   a generic `to_hetero()` wrapper," not just "k-dimensional GNN, not HGT."
2. The "no object-object edges" claim can now be cited as code-confirmed rather than inferred.
3. A new, previously-undocumented caveat is needed: HOEG's task formulation (whole-case,
   multi-target-per-graph) differs from this thesis's own (per-prefix, single-target-per-graph) — the
   comparison baseline is fair at the *encoding* level, and should say so explicitly rather than
   silently treating the two setups as identical.

The `num_layers=2` justification (Finding 3) doesn't need to change — it's now on firmer ground, not
different ground.
