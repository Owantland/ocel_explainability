# Review: `results.txt` — open items

Fourth review round. `results.txt` has grown substantially since round 3: the entire
§5.2 "Explainability framework" section — previously just a one-sentence stub, the
single biggest gap flagged in every prior round — is now fully written across 6
subsections (perturbation-based, gradient-based, counterfactual, and
explanation-metrics explanation), directly describing this session's new showcase
figures. Read the current file in full, checked which of round 3's 6 open items are
now resolved, and fact-checked every new §5.2 claim directly against the underlying
scripts/data (generating-script source, CSVs, the Optuna sweep database, training
logs) rather than the prose alone, matching the verification depth of prior rounds.

**Resolved since round 3:**
- "three of the 4 KPIs" (old line 87) → now correctly reads "only two of the four
  KPIs" (current line 87) — matches `mae_by_depth_summary.csv` exactly, as round 3
  recommended.
- The Logistics homogeneous-GNN-vs-HGT overstatement (old line 60) → now reads "the
  two models track each other closely" (current line 60) — matches round 3's
  suggested softening. (Introduces a new typo — see item 9 below.)
- The duplicate caption between `combined_baseline_comparison_table.png` and
  `mae_by_depth_summary.png` → now two distinct captions (current lines 68/83).
  (New caption has a typo — see item 9 below.)
- The stray double-quote bug in the LoadToVehicle ablation caption (line 35) —
  clean quotes now, no doubling.
- §5.2 itself — now substantively written, closing the round-2/3 headline gap.

Ten items remain open or new this round.

## 1. Broken cross-reference: `\ref{sec:models_baselines}`

Line 72 still references `\ref{sec:models_baselines}`, and no label of that name
exists anywhere in the thesis (checked via a repo-wide search across
`thesis_parts/*.txt`). Unchanged since round 3 — still the one hard,
compile-affecting bug carried over from that round.

## 2. New broken cross-reference: `\ref{sec:model_counterfactual}`

Line 213 references `\ref{sec:model_counterfactual}` — same bug class as item 1,
new this round with the counterfactual-explanation subsection. No matching
`\label` exists anywhere in `thesis_parts/*.txt` (the only counterfactual-adjacent
label found, `sec:dataset_perturb`, is unrelated and is itself used correctly at
line 100). Two broken refs now instead of one.

## 3. Numeric error, carried forward: GBT is not "second worst" for `LoadToVehicle`'s last-event metrics

Line 78 still claims "the median predictor and GBT maintain their roles as the
worst performing and second worst models" for last-event metrics. Re-checked
directly against the current (regenerated this session) `model_comparison_LoadToVehicle.csv`:
last-event MAE ranking is HGT (14.48h) < GBT (23.70h) < HomoGNN (39.74h) < Mean
(304.19h). GBT is actually the **second-best** model for this KPI, and HomoGNN is
the second-worst — the opposite of the claim. This is the same bug round 3 found,
persisting even though the underlying LoadToVehicle data was regenerated since
then (this session's second-KPI work). Same fix as before: an explicit exception
for LoadToVehicle, the same way line 75 already carves one out for the all-prefix
discussion.

## 4. Counterfactual "10 hours" threshold doesn't match the generating script

Line 213 states "the chosen threshold value was set to 10 hours," but the actual
generating script, `counterfactual_showcase.py`, calls
`explain_counterfactual(1801, n_results=3)` with no `min_gap_hours` argument — the
method's default is 0.0h, not 10h. Either the prose should describe what actually
happened (no minimum gap enforced — the top-3 candidates were simply the closest
by dissimilarity, coincidentally with predictions well below 125.8h), or the
script should be rerun with `min_gap_hours=10` so the artifacts match the claim.
Flagging as a choice, not resolving unilaterally.

## 5. Ablation-figure/text framing mismatch (clarity, not a factual error)

Verified against the actual Optuna sweep database
(`sweep_TimeFrom_CustomerOrder_to_LoadToVehicle.db`): the single best trial
genuinely has `num_layers=4` (0.6413) narrowly beating `num_layers=3`'s best trial
(0.6419), so "the complete sweep eventually selected 4 layers" (line 39) is
technically correct. But `loadtovehcile_ablation.png`'s box plot shows per-group
medians, where `num_layers=3` looks clearly best (median 0.683 vs. 0.721 for
`num_layers=4`). A reader comparing the prose's "no clear winner between 3 and 4"
against the chart could reasonably read them as contradicting each other, since
only the single top trial is a near-tie — the group distributions are not. Worth
a one-clause clarification that the selection criterion is the single best trial,
not the boxplot's central tendency.

## 6. Reproducibility risk: the cited characterization score is not stable across reruns

`explanation_quality_showcase.py`'s cited characterization score (0.565, line 246)
comes from an unseeded Shapley procedure (`n_samples=100`). Confirmed empirically
this session: two consecutive runs of the identical script produced
characterization 0.617 and then 0.565 — a ~9% swing — with different Fidelity+/−
values and a slightly different explanation node set each time. If the script
reruns again before the thesis is finalized, the image will silently change while
the cited number in prose won't follow it. Recommend treating this figure+text
pairing as frozen (do not rerun the script again), or adding a fixed random seed
to the underlying Shapley sampler for genuine reproducibility.

## 7. Generator-script/on-disk filename drift, carried forward unchanged

`num_layers_ablation_logistics.py` and `num_layers_ablation_logistics_loadtovehicle.py`
still write to canonical filenames that don't exist on disk; the actual files
remain the differently-named `depart_ablation.png` and `loadtovehcile_ablation.png`.
Still a reproducibility risk if either script is rerun.

## 8. Filename typo, carried forward unchanged

`loadtovehcile_ablation.png` (line 34) still misspells "LoadToVehicle" in the
filename — every prose occurrence (lines 35, 41) spells it correctly.

## 9. Typos

New this round: "whats's" should be "what's" (line 60, introduced by round 3's
fix for item 2 above); "detph" should be "depth" (line 83's new caption,
introduced by round 3's fix for item 3 above). Still unresolved from round 2,
three rounds later: "shwon"/"Managmenet" (line 20) and "hiden dimensions" (line 41);
also "am much flatter" should be "a much flatter" (line 41).

## 10. Minor completeness note (not an error)

`gradient_shapley_comparison_neighbor_*.png` (feature attribution for a
non-viewpoint neighbor node) exist on disk, fresh, but aren't referenced anywhere
in `results.txt` — available supplementary material if §5.2's gradient subsection
wants a second worked example beyond the Orders-node one it currently shows.

## Also confirmed accurate (no issue found)

Spot-checked directly against the underlying data rather than taken on faith: the
characterization numbers (0.815/0.685/0.396/0.244, line 101) against
`fidelity_comparison_summary.csv`; the counterfactual example's 125.8h/31.4h
figures (line 213) against the actual showcase output; `prefix_evolution.png`/
`single_subgraph.png` (renamed single-KPI crops of the 4-panel showcase figures)
genuinely reflect order 1801/PayOrder as the surrounding prose implies; the
"3 layers" Depart claim (line 39) against `depart_ablation.png`; the
order_management training-curve numbers — HGT's ~0.45 MAE floor and HomoGNN's
epoch-4 local minimum (line 51) — against both training logs directly; and the
last-event target-scale claims for Logistics (2.3h/1.8h for LoadToVehicle, 27h/7h
for Depart, line 78), cross-validated indirectly via each KPI's mean-predictor
last-event MAE (which should equal roughly `global_training_mean − last_event_mean`
— both check out to within rounding).

The chapter's biggest gap from every prior round — an unwritten §5.2 — is now
closed, and its new content is substantially accurate: of the ten items above, only
three are outright factual errors (items 1-3), one is a script/text mismatch the
user needs to resolve one way or the other (item 4), and the rest are clarity notes,
a reproducibility risk worth flagging, filename hygiene, and typos. None of the
new findings undermine the underlying analysis; they are the same class of
precision/reference fixes prior rounds have consistently found in newly-added
content.
