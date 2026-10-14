# CargoPickup_IoT validation — does the current `process_generation`/`ocel_generator` configuration work?

**Update, 2026-10-13 (same day, branch `cargo-pickup-iot-fixes`): RESOLVED.** Findings 1–4 below
are fixed; Finding 5 is confirmed real (not fixed, by design) and now precisely quantified instead
of just suspected. See "Resolution" at the bottom for what changed, the final numbers, and three
additional bugs that only surfaced while actually making the fix work (none were anticipated by
the original plan).

**Date**: 2026-10-13
**Scope**: validate whether `process_generation.py` and `ocel_generator.py`, as they stand today,
work correctly against the `CargoPickup_IoT.sqlite` database (viewpoint: `Pickup Plan`). No
changes were made to either file. `hetero_graphs.py`/`training.py`/`explainer.py` were not
exercised beyond one optional stretch check of `split` (see below) — out of scope by design.

**Verdict: partially.** Both classes run to completion with no exception — the regenerate stage
is, on its face, a success. But the output is not usable as-is: traces are severely contaminated
and bloated, one object type (`Silo`) never receives feature data despite being configured, two
traces are silently dropped entirely, and a downstream stretch check (`split`) produces a
**0% validation set** — a hard blocker for any eventual training run. The root cause is a latent,
dataset-specific assumption in `process_generation.py`'s object-object neighbor discovery, not a
config mistake on my part (the config was built carefully to avoid every *known* pitfall from the
investigation phase — see "What was configured" below — and the problems below only emerged by
actually running it).

## What was configured

Added `cargo_pickup_iot` to `files/config.yml` and to `run_pipeline.py`'s `--database` choices
(see the `git diff` for exact values). Key choices, made to avoid every compatibility gap found
during investigation (not guesses):
- `viewpoint: "Pickupplan"` — the cleaned (space-free) `OCEL_TYPE_MAP` form; the raw `"Pickup
  Plan"` (with a space) fails `process_generation.py`'s `_validate_identifier()` check immediately.
- `added_depth: 1` — confirmed via direct SQL against `object_object` that `Silo` is two hops
  from `Pickupplan` (`Pickupplan↔Cargo` and `Truck↔Pickupplan` are direct; `Silo↔Truck` is one
  hop further).
- `attributes: {}`, `encoding: ['Pickupplan', 'Cargo']`, `time_attributes: {Silo: ['Temperature',
  'Humidity']}`, `role_encoding: {Truck: 'Is_normal'}` — every traversed type given exactly one
  "home," deliberately avoiding `ocel_generator.py`'s confirmed unquoted-SQL-identifier bug (every
  one of `Pickupplan`/`Cargo`'s native attribute columns has a space or parenthesis, e.g. `"Num of
  trucks"`, `"Cargo stock weight(scheduled)"` — directly confirmed to raise a SQL syntax error).
- `kpi_event: "EvaluateTruckExit"` — the best-populated plausible completion milestone (3,322
  occurrences across 2,000 plans).

## Command run

```
PYTHONPATH=. python3 run_pipeline.py --database cargo_pickup_iot --cant 2000 --stages regenerate
```
Exit code 0. No exceptions. `files/graph_structures/cargo_pickup_iot/2000/` was produced:
`ev_log.csv`, `all_kpis.csv`, `ocel.csv`, `edges.csv`, `tensor_dict.json`.

## Findings

### 1. Traces are severely contaminated and bloated (the headline finding)

`ev_log.csv` has 419,282 rows across 1,998 traces — **~210 events/trace on average**, against a
raw DB total of only 74,217 events over 2,000 Pickup Plans (~37/plan if evenly spread). The
regenerate run itself emitted **2,886 console warnings** of the form:
```
[get_ev_log] object Pcp1891 (viewpoint Pcp908) has no 'EvaluateTruckExit' event of its own; falling back to the trace's last instance.
```
`Pcp1891` here is a **different Pickup Plan object**, appearing inside `Pcp908`'s own trace. This
means traces are pulling in events/objects belonging to *other, unrelated* Pickup Plans.

Output file sizes confirm the scale of this: `ocel.csv` is **2.5 GB** and `edges.csv` is **5.1 GB**
for this run, versus 23 MB/20 MB (order_management, 2000 traces) and 39 MB/64 MB (logistics, 1000
traces) — a 60–250× blow-up, not proportional to the trace count.

**Most likely mechanism** (confirmed in part by direct code reading, inferred in part from the
symptoms — flagged as such): `process_generation.py:related_nodes()`'s *direct-neighbor* pass is
asymmetric:
```python
rltd_objects = o2o_table[o2o_table['src_id'] == vwpnt_object[0]]   # process_generation.py:~106
```
This only follows `object_object` edges where the **viewpoint object is the source**. For
`order_management`/`logistics`, the case object is always modeled as the edge source (parent→child),
so this is invisible. CargoPickup_IoT's schema is the other way around for its dominant
relationship: **`Truck→Pickupplan`** (confirmed via direct SQL: `Truck` is the source, `Pickupplan`
is the target, 13,779 rows) and **`Silo→Truck`** likewise. So a Pickup Plan's own trucks are only
discovered one iteration later, through the (correctly bidirectional) `added_depth` loop — and once
a Truck id enters the trace's object set, **every event that Truck ever participated in, across
every one of the ~46 other Pickup Plans it has served (300 trucks ÷ 13,779 assignments), is pulled
into this trace**, and from there each such event's *other* participants (including those other
Pickup Plans) ride along. This is consistent with every symptom observed — the exact final step by
which another Pickup Plan ends up back in `events_by_objects` was not traced line-by-line to 100%
certainty (the relevant `private_types` filtering logic is more involved than first appears), but
the directional asymmetry above is a directly-confirmed code fact and a directly-confirmed cause of
at least the Truck-via-depth-loop-only discovery.

### 2. `Silo` never receives feature data, despite being configured

`tensor_dict.json` only contains `Pickupplan`, `Cargo`, `Truck` (and their edge types) —
**`Silo` is absent**, and `ocel.csv` has no `Silo::ids`/`Silo::attributes`/`Silo::idx` columns,
even though `config.yml` gives `Silo` a valid `time_attributes` home and `Silo_to_Truck` *does*
appear as an edge-type key in `edges.csv`'s header. This means `Silo` is structurally referenced
(as an edge type) but has no node feature vectors — a configuration like this would almost
certainly crash `hetero_graphs.py`'s unguarded `self.tensor_dict[key]` lookup (confirmed during
investigation, not re-tested here, since `hetero_graphs.py` is out of scope) the first time a
`Silo` node is encountered while building a prefix graph.

This appears to share the same root cause as Finding 1: `Silo` is two hops from `Pickupplan`
(`Pickupplan→Truck→Silo`), and with `added_depth: 1` providing only one bidirectional expansion
pass, `Truck` is only added to the traversal frontier *during* that single pass — too late within
the same pass for `Silo` to be reached. A `added_depth: 2` was not tried in this round (kept to
one validation pass per the plan's scope), but is the natural next thing to try, with the caveat
that it would likely increase the Finding 1 contamination further, not reduce it.

### 3. Two traces are silently and completely dropped

`vwpnt_id` 1887 and 1992 (of the full 1..2000 range) never appear in `ev_log.csv` at all — 1,998
distinct, not 2,000. `TrainTestBuilder` (see Finding 4) confirms the same two ids are empty.

### 4. Stretch check — `split` runs, but produces a 0% validation set

Per the plan's optional stretch check:
```
PYTHONPATH=. python3 run_pipeline.py --database cargo_pickup_iot --cant 2000 --stages split
```
```
WARN [TrainTestBuilder]: skipped 2 trace(s) with no rows or no 'EvaluateTruckExit' event in window: [1887, 1992]
[TrainTestBuilder] Split for 2000 traces:
  Train : 1455  (72.8%)
  Val   :    0  (0.0%)
  Test  :    9  (0.5%)
  Gap   :  536  (26.8%)  [temporal no-overlap exclusions]
```
No exception — but **0 validation traces** is a hard blocker for any eventual `training.py` run
(no held-out signal for checkpoint selection), and only 9 test traces is too few to be meaningful.
The unusually high "Gap" fraction (26.8%, vs. a few percent typically) is consistent with Finding
1: once traces span bloated, overlapping time windows (because they absorb other Pickup Plans'
event history via shared trucks), the temporal no-leakage split logic has far less genuinely
non-overlapping material to assign to val/test.

### 5. `kpi_event` choice is a poor fit for `kpi_viewpoint`

`EvaluateTruckExit` is a **Truck**-level milestone, not a **Pickup Plan**-level one — confirmed by
the 2,886 "has no 'EvaluateTruckExit' event of its own" fallback warnings, which fire for nearly
every (contaminated) Pickup-Plan-typed row encountered. `process_generation.py`'s documented
fallback (use the trace's last instance of the event, trace-wide, rather than the object's own) is
working as designed, but its near-universal use here means the KPI target is effectively always
the trace-level fallback, not a genuine per-Pickup-Plan measurement — not itself a crash, but a
real quality concern, independent of Findings 1–4, and worth re-picking given more exploration time
(e.g. `PrintWeighingTicket`/`InputTallySheet`, or reconsidering `kpi_viewpoint` itself).

## What would be needed to go further (explicitly not done here)

- Fix (or at least diagnose precisely) the asymmetric direct-neighbor discovery in
  `process_generation.py:related_nodes()` — likely needs to check both `src_id ==` and
  `trgt_id ==` the viewpoint object in the initial pass, not just `src_id ==`. This is a genuine
  code change to a function shared by every database, so it needs regression-testing against
  `order_management`/`logistics` too, not just CargoPickup_IoT.
- Quote/validate attribute and time-attribute column names in `ocel_generator.py`'s
  `get_attributes()`/`_build_time_attr_cache()` so CargoPickup_IoT's richer, space-containing
  columns (`"Num of trucks"`, `"Total pickup weight"`, etc.) become usable.
- Re-examine whether `added_depth: 2` reaches `Silo` without making Finding 1 worse, once Finding 1
  itself is addressed.
- Pick a better `kpi_event`/`kpi_viewpoint` pairing (Finding 5).
- Everything beyond `regenerate`/`split`: `hetero_graphs.py`, `sweep`/`train`, `explainer.py`,
  `dashboard/` — entirely unvalidated by this pass.

## Where the generated artifacts are

`files/graph_structures/cargo_pickup_iot/2000/` (gitignored, ~7.6 GB total at the time of this
original pass — see Resolution below for the post-fix size).

## Resolution (branch `cargo-pickup-iot-fixes`, same day)

**Root cause, confirmed precisely** (the original "most likely mechanism" above was on the right
track but incomplete): `Truck` is a many-to-many shared resource (300 trucks serve ~13,779
plan-assignments across 2,000 plans). Putting it in `unique_ids` let `related_nodes()` pull in
every event that truck ever took part in, across every other plan — correct for a strictly-owned
hierarchy member (an Item belongs to one Order), wrong for a shared one. A second, independently
confirmed issue: **`Cargo` is *also* shared** — each cargo serves exactly 2 different Pickup
Plans, not 1:1 as the aggregate row count (2,000 `Pickupplan→Cargo` rows) misleadingly suggested.
Both were acting as contamination pivots.

**What changed** (`process_generation.py`, `ocel_generator.py`, `sup_funcs.py`, `files/config.yml`
— full detail in the `cargo-pickup-iot-fixes` branch's commit):
1. Fixed `related_nodes()`'s asymmetric initial neighbor pass (now checks both edge directions,
   gated by `unique_ids` membership so it's a no-op for `order_management`/`logistics`).
2. Added an opt-in `episode_scoped_ids` config mechanism: a shared type's event/traversal
   inclusion is scoped to a `[start, end]` window read from `object_object`'s own
   qualifier/timestamp columns (`"Assigned truck for pickup plan"` → `"Dropped truck from pickup
   plan"`), instead of its entire cross-case history. `added_depth` corrected from `1` to `2`
   (confirmed via precise loop-iteration tracing that `1` was simply insufficient to reach `Silo`).
3. **A general invariant that turned out to matter more than the episode-scoping mechanism
   itself**: a trace must never admit a *different* instance of the viewpoint's own type. This is
   what actually closes the `Cargo` leak (`Cargo` was never going to get its own
   `episode_scoped_ids` entry — it has no clean assign/drop qualifier pair) and is a strictly
   stronger, dataset-agnostic safety net than enumerating every shared type in config.
4. Two robustness fixes, found only by running the fix to completion rather than by review:
   `get_ev_log()` now skips (logs + continues) a trace whose assigned truck(s) never reached
   `kpi_event` at all anywhere, instead of crashing with an `IndexError` — a real possibility once
   contamination no longer artificially guaranteed every trace *some* match via cross-contamination.
   `ocel_generator.py` now pre-seeds every event row's object-type columns from the database's
   full configured type set, opt-in behind `episode_scoped_ids`, so a trace legitimately lacking a
   type (e.g. no `Silo` visit) doesn't build a different-shaped row than one that has it.
5. A boundary-inclusivity bug in the episode-window clamp (for "open," never-dropped episodes):
   the clamped end was the *next* assignment's own timestamp, and since the window check is
   inclusive on both ends, that next-assignment edge itself (to a different plan) slipped through
   as if still inside this episode. Fixed by nudging the clamp 1 second earlier.

**Regression discipline**: every one of the 5 changes above was diffed against a byte-identical
snapshot of `order_management`/`logistics` output, re-established from a *freshly regenerated*
baseline after discovering the first on-disk snapshot was itself stale (predated an unrelated
earlier code change, briefly causing a false "regression" signal). All 5 changes are confirmed
byte-for-byte inert for both existing databases.

**Final numbers for CargoPickup_IoT** (same command as the original run):
- `ocel.csv`/`edges.csv`: **3.4 MB / 3.3 MB** (was 2.5 GB / 5.1 GB — contamination fully resolved,
  now comparable to or smaller than `order_management`/`logistics`).
- `tensor_dict.json` now includes `"Silo": 2, "Truck": 2` — Finding 2 fixed.
- Average events/trace: **5.4** (median 4) — and event-type counts now match the raw database
  *exactly* where directly comparable: `EvaluateTruckExit` = 3,322 rows (the database's total
  count of that event, with zero duplication and zero omission); `ArriveatSilo`/
  `DetermineContinuanceofPickup` = 100 each, `LoadTruck`/`WeighLoadedTruck`/`InputTallySheet`/
  `PrintWeighingTicket` = 300 each — all exact matches to the DB-wide totals.
- `--stages split`: **Train 70.0% / Val 6.8% / Test 7.1% / Gap 16.1%** — Finding 4 fixed (was
  0.0% / 0.5%).
- **320 of 2,000 traces (16%) are genuinely, correctly dropped** — not a bug, but Finding 5's real
  consequence now precisely measured: these plans' assigned truck(s) never reached
  `EvaluateTruckExit` at all (only ~24% of truck-assignments ever do; a plan needs at least one
  success among however many reassignments it got). This supersedes the original Finding 3 (2
  dropped traces) — that number was itself an artifact of the contamination masking the real
  drop rate, not a separate issue.
- Finding 5 itself (the `kpi_event`/`kpi_viewpoint` granularity mismatch) remains **not fixed, by
  design** — this pass fixed the plumbing bugs that were making it worse than it needed to be and
  precisely measured its real cost (16% of traces); re-picking `kpi_event` is still a separate,
  deliberately deferred decision.

## Training results (branch `cargo-pickup-iot-training`, same database, same day)

With the fixes above in place, a full HGT model was trained on `cargo_pickup_iot` and compared
against `order_management`'s PayOrder result and `logistics`'s Depart/LoadToVehicle results, using
this repo's existing pipeline stages and comparison tooling unchanged (`graphs` → `sweep` → `train`
→ `homo` → `kdim` → `validate` → `compare`, `cant=2000`, `selection_metric='pooled'` — all
established defaults, no deviation needed).

**Two more previously-unexercised bugs surfaced and were fixed along the way** (both committed on
`cargo-pickup-iot-training`, both confirmed byte-for-byte/numerically inert for
`order_management`/`logistics` via the same clean-baseline regression methodology used throughout
this project):
- `hetero_graphs.py`'s `preprocessing_steps()` assumed every `vwpnt_id` 1..cant has at least one
  `ev_log.csv` row — crashed on the 320 traces this database legitimately drops (`order_management`/
  `logistics`'s kpi_event is reached by virtually every trace, so this was never exercised before).
  Fixed by skipping empty traces, mirroring `TrainTestBuilder`'s own existing pattern.
- `ocel_generator.py`'s `_lookup_time_attrs()` already coalesced a null "fixed" attribute reading
  from any non-null row, but had no equivalent fallback for the time-sensitive attribute — a `None`
  there crashed `hetero_graphs.py`'s `torch.tensor()` call. Confirmed real for `Silo`/`Humidity` (not
  every changelog snapshot records every sensor reading). Added the same coalesce, preferring the
  closest-in-time non-null row.

**Also added**: an opt-in MPS (Apple GPU) device branch in `training.py` (unreachable on any CUDA
machine), plus two small fixes to device-mismatch bugs it exposed in inference code
(`run_pipeline.py`'s `_predict_all()` and `baselines.py`'s `hgt_predictions()` never moved graphs to
the model's device — invisible when everything was CPU-only, a real crash once MPS was in play).

### Results

Final HGT checkpoint: `hidden_channels=24, num_layers=2, num_heads=2, lr=0.001` (16-trial grid
sweep, same default non-logistics search space `order_management` uses — no need for logistics'
extended layer search; no sign of a capacity collapse in the results below). Training converged in
23 epochs (~6.5 min on MPS).

| Dataset / KPI | Model | MAE (all) [h] | R² (all) | MAE (last) [h] | R² (last) |
|---|---|---|---|---|---|
| Order Management / PayOrder | **HGT** | 167.2 | -0.317 | 98.0 | 0.051 |
| Logistics / Depart | **HGT** | 112.0 | 0.304 | 6.1 | -0.170 |
| Logistics / LoadToVehicle | **HGT** | 115.0 | 0.281 | 8.4 | -30.8 |
| **CargoPickup_IoT / EvaluateTruckExit** | **HGT** | **13.3** | **0.085** | **7.1** | **-0.668** |

(Full 5-model breakdown — HGT/HomoGNN/k-dim GNN/Mean/GBT, all 5 KPIs — in
`files/explainer_outputs/combined_baseline_comparison.csv`/`_table.png`, regenerated to include this
database alongside the other two.)

**R² (the primary cross-database axis, since raw MAE/RMSE scale with each dataset's own target
variance — PayOrder's mean true remaining time is ~341h vs. CargoPickup_IoT's ~25h on the
all-prefix test subset, so the much smaller MAE here is not directly comparable to the others'
without R² for context):** CargoPickup_IoT's all-prefix R²=0.085 is **positive** — worse than
Logistics' two KPIs (0.304, 0.281) but clearly better than PayOrder's (-0.317). Last-event R²
(-0.668) sits in the same rough range as Logistics/Depart (-0.170) and is far better than
Logistics/LoadToVehicle's (-30.8) — none of the four last-event R² values are strongly positive,
consistent with the small last-event test-set sizes (n=142 here) making this metric noisy across
every KPI in this project, not a CargoPickup_IoT-specific weakness.

**Statistical significance** (`baseline_significance.csv`): HGT beats HomoGNN, k-dim GNN, and the
Mean predictor decisively (p < 1e-20 on all three, Wilcoxon and t-test agree) on the last-event
subset. Against GBT specifically, HGT (7.1h) and GBT (8.4h) are **not** statistically distinguishable
on last-event MAE (p=0.40/0.43) — GBT's feature importances show it essentially collapsed to
predicting from `n_events` alone (importance 1.000, every other feature ~0), so this is "HGT ties a
simple event-count heuristic at the very end of a trace," the same pattern order_management's
k-dim-GNN-beats-HGT-on-last-event result reflects: simple baselines can be competitive in the
low-data, high-variance last-event regime even when HGT wins clearly everywhere else (all-prefix
MAE: HGT 13.3h vs. GBT 24.1h, HGT vs. every other baseline in both MAE_all and MAE_last).

**No sign of the dead-`lin_dict`/reachability pathology** ([[project_hgt_dead_features_finding]]):
MAE-by-depth decreases monotonically and substantially (36.7h → 12.4h → 8.1h → 5.0h across the four
depth bins), exactly the expected "closer to completion ⇒ less remaining-time variance ⇒ lower MAE"
shape, with HGT below every baseline at every depth (see `mae_by_depth_summary.png`'s 5th panel).

**Bottom line**: CargoPickup_IoT's HGT model is a genuinely competitive, well-behaved result —
not the best R² of the three databases, but positive and mid-pack, with no architectural red flags,
confirming the contamination/Silo/split fixes earlier in this document produced a database that's
now fully usable for the same comparative analysis as the other two.
