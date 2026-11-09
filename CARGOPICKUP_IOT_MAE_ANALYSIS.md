# Why is CargoPickup_IoT's MAE so much lower than the other two databases?

CargoPickup_IoT's HGT checkpoint (Pickupplan → EvaluateTruckExit) reports MAE_all=13.3h,
versus order_management/PayOrder's 167.2h and logistics' 112.0h (Depart) / 115.0h
(LoadToVehicle) — an 8-12x gap. This doc root-causes that gap with numbers pulled directly
from this repo's own artifacts (`_norm.json`, the cached `.pt` graphs, `ocel.csv`,
`config.yml`), rather than assuming it reflects the model being 8-12x more accurate.

Extends `CARGOPICKUP_IOT_VALIDATION.md`'s "Training results" section, which already notes
R² (not raw MAE) is the right cross-database axis and that PayOrder's test-set mean
(~341h) dwarfs CargoPickup_IoT's (~25h) — this doc quantifies *why* that scale gap exists
and exactly how much of the headline MAE gap it explains.

## Summary: it's overwhelmingly a test-split scale effect, not primarily a modeling win

**The single biggest factor by far**: CargoPickup_IoT's test split has a dramatically
smaller target scale than its own training split — a drift not seen (or seen far more
mildly) in the other three KPIs. Once every KPI's MAE/RMSE is normalized by **its own
test-split standard deviation** (not training std — using training std, as an earlier
draft of this analysis did, overstates the gap), EvaluateTruckExit's relative error is
*modestly* better than Depart/LoadToVehicle, not an order of magnitude better:

| KPI | test mean (h) | test std (h) | MAE_all (h) | MAE/test_std | RMSE/test_std | implied R² | actual R²_all |
|---|---|---|---|---|---|---|---|
| PayOrder | 341.4 | 221.6 | 167.2 | 0.754 | 1.147 | -0.316 | -0.317 |
| Depart | 309.8 | 171.7 | 112.0 | 0.652 | 0.834 | 0.304 | 0.304 |
| LoadToVehicle | 288.8 | 169.7 | 115.0 | 0.678 | 0.848 | 0.281 | 0.281 |
| **EvaluateTruckExit** | **24.5** | **25.7** | **13.3** | **0.517** | **0.956** | **0.086** | **0.085** |

(`implied R²` = `1 - (RMSE/test_std)²`, i.e. reconstructing R² from the reported RMSE and
each KPI's own test-split std, with no access to the raw per-prediction residuals. It
matches the actual reported R²_all to within rounding for all four KPIs — confirming R² is
computed against each split's own variance, and that this reconstruction is a reliable way
to reason about what's driving it.)

**Reading this table**: EvaluateTruckExit's MAE is ~20-25% tighter relative to its own
test-set spread than Depart/LoadToVehicle's (0.517 vs. 0.652/0.678) — a real, modest
improvement — but its R² (0.085) is actually *worse* than Depart's (0.304) and
LoadToVehicle's (0.281). **The 8-12x raw MAE gap is overwhelmingly explained by
EvaluateTruckExit's test split simply having a much smaller target scale (std=25.7h vs.
170-222h for the others) — not by HGT fitting this KPI 8-12x better.**

## Root cause of the scale difference: a severe train→test target-variance drift, unique to this KPI

Full train/val/test target stats per KPI (raw hours, pulled directly from each split's
cached `.pt` graphs' `y` field — see Methodology note below):

| KPI | train mean/std (h) | val mean/std (h) | test mean/std (h) |
|---|---|---|---|
| PayOrder | 254.1 / 219.6 | 264.5 / 135.8 | 341.4 / 221.6 |
| Depart | 327.9 / 167.3 | 319.8 / 167.0 | 309.8 / 171.6 |
| LoadToVehicle | 306.5 / 164.0 | 297.5 / 159.8 | 288.8 / 169.7 |
| **EvaluateTruckExit** | **110.1 / 101.6** | **63.5 / 72.2** | **24.5 / 25.7** |

Depart and LoadToVehicle are essentially flat across all three splits (std within ~5% of
each other). PayOrder's val split is narrower than train/test (the already-resolved
train/val gap, [[project_payorder_trainval_gap_resolved]]) but nowhere near this
magnitude. **EvaluateTruckExit's std shrinks monotonically and by ~4x, train→test**
(101.6h → 72.2h → 25.7h) — a qualitatively different, much more severe pattern than
anything in the other databases.

**Why**: a direct check (Pearson correlation between each trace's temporal position in
`ocel.csv` and its total event-to-event duration) finds a strong negative trend —
corr(start-timestamp, trace-duration) = **-0.82**, corr(`vwpnt_id` order, duration) =
**-0.66** — traces starting later in the simulated timeline are systematically shorter.
Bucketing traces into (first 60% / next 15% / last 25%) — a proxy for the pipeline's
chronological train/val/test split — shows the same shrinking pattern directly on trace
duration (39,015h → 21,803h → 15,134h mean). This is consistent with a **finite
simulation window**: traces that happen to start late in a bounded simulated period have
less remaining simulated time available to run long, so they're structurally shorter —
the same kind of temporal-split artifact already established for PayOrder's train/val gap
(resolved as "narrower split variance, not overfitting"), but considerably more extreme
here. This is a property of how this particular synthetic log was generated, not a
general claim about real-world truck pickup/weighing process duration.

## Secondary, smaller contributing factors (real, but minor next to the above)

These were the original hypotheses going into this analysis; each is real and probably
still contributes a little to the lower absolute error, but none of them is the main story
once the test-split-scale effect above is accounted for.

- **Traces are structurally shorter.** Mean/median events per trace (`ocel.csv` row counts
  per `vwpnt_id`): order_management 14.4/14, logistics 62.1/60, **cargo_pickup_iot 7.0/6**
  — ~2x shorter than order_management, ~9x shorter than logistics. Fewer events per trace
  means fewer compounding timing/sequencing decisions.
- **Fewer per-node features.** `config.yml`'s `cargo_pickup_iot` block has `attributes: {}`
  (no numeric object attributes at all, unlike order_management/logistics' populated
  dicts), and 2 of its 4 non-Events node types (`Pickupplan`, `Cargo`) degenerate to dim-1
  placeholders (>50 distinct ids each). Less per-node content for the model to reason over.
- **Activity vocabulary is not a factor.** Distinct `ev_type` counts are comparable across
  all three databases (order_management 11, logistics 14, cargo_pickup_iot 13) — ruled out
  as an explanation.

## Two hypotheses checked and found NOT to hold (reported for completeness)

- **"The steep MAE-by-depth drop (36.7h→12.4h→8.1h→5.0h) is just an artifact of '10+'-bin
  prefixes being almost-complete traces."** Checked directly on the test split: among
  `cargo_pickup_iot` test prefixes in the "10+" depth bin, only **26.6%** are within 2
  events of their trace's actual end (mean gap-to-end = 7.6 events, median = 6). The "10+"
  bin conditions on *longer* traces by construction, so most of these prefixes still have
  meaningful remaining uncertainty — the MAE drop is not a trivial end-of-trace artifact.
- **"CargoPickup_IoT's synthetic generator produces unusually regular/deterministic
  per-activity timing compared to the other two databases."** Checked via coefficient of
  variation (std/mean) of inter-event gaps for each database's most frequent activities:
  cargo_pickup_iot's top activities show CV≈1.50 and 0.12; order_management's show
  CV≈1.34, 2.00, 0.18; logistics' show CV≈3.56, 1.08. These ranges overlap substantially —
  no clear signal that cargo_pickup_iot's timing is more deterministic than the others.
  (One cargo_pickup_iot activity showed an extreme outlier gap distribution — mean ≈18,400h,
  up to ~6.5 years — almost certainly a periodic/global-clock-anchored sensor-type event
  rather than a normal process-timing gap; excluded from the comparison above as
  unrepresentative, and noted here as a data-quality curiosity rather than investigated
  further, since it doesn't bear on the regularity question either way.)

## Recommendation for the thesis

Don't cite the raw 13.3h MAE against the other KPIs' 112-167h without this context — it
invites the reader to conclude the model is dramatically better at this task, which the
R²/normalized-error numbers above don't support (R²_all=0.085 is actually the second-worst
of the four, not the best). The already-established R²-led framing in
`CARGOPICKUP_IOT_VALIDATION.md` is the right instinct; this analysis suggests strengthening
it with one explicit sentence: CargoPickup_IoT's test split has a much smaller and more
tightly-bounded target range than the other databases' (plausibly a finite-simulation-
window effect on this synthetic log, not a property of real truck pickup/weighing
processes), which mechanically produces a smaller MAE independent of model quality — and
that, net of that scale effect, the model's relative performance here is comparable to,
not dramatically better than, the logistics KPIs.

## Methodology note

Target values were read directly from each split's cached `.pt` graphs
(`files/hetero_structures/{db}/{cant}/{split}_graphs_sg.pt`), taking the raw (not
z-normalized) `y` field on the viewpoint node type and dividing by 3600 for hours. The
`.pt` cache stores **raw** target seconds — normalization happens in-place on the loaded
tensors inside `Modelling.__init__` (`training.py`), not in the cached file itself — so no
denormalization arithmetic against `_norm.json`'s `target_mean`/`target_std` was needed or
applied to these per-split numbers (an earlier attempt to denormalize them against
`_norm.json`'s training-set stats produced nonsensical ~40-million-hour figures, since that
would double-apply the scale). `_norm.json`'s `target_mean`/`target_std` are training-split
statistics only, used here just to confirm they match the directly-computed train-split
numbers (they do, exactly). Depart's split stats used the
`files/hetero_structures/logistics/1000_customerorder_depart_backup/` graph cache, since
`config.yml`'s live `logistics` block currently points at LoadToVehicle.
