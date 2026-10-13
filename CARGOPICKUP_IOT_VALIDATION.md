# CargoPickup_IoT validation — does the current `process_generation`/`ocel_generator` configuration work?

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

`files/graph_structures/cargo_pickup_iot/2000/` (gitignored, ~7.6 GB total — left in place for
further inspection; delete if disk space is needed, it's fully regenerable via the command above).
