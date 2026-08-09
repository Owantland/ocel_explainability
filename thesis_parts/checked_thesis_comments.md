# Review: supervisor comments on `checked_thesis.pdf`

`thesis_parts/checked_thesis.pdf` (75 pages, `pdfinfo` Author "massimiliano de leoni",
title `otto_wantland_1st_draft.pdf`) is Prof. de Leoni's annotated read of the draft. All
23 comments were extracted with PyMuPDF and catalogued, each mapped to its thesis
location and given a proposed fix.

**Canonical-file correction (2026-08-08), re-verified (2026-08-09)**: every comment
below was re-verified directly against `thesis_parts/latex_template/chapters/*.tex` and
`dissertation.tex` — the actual LaTeX source that compiles (`\include{chapters/...}`) —
**not** against the sibling
`thesis_parts/{introduction,related_work,dataset,models,results,conclusion}.txt` files,
which are separate, diverged working copies that several of these fixes were drafted
against but never landed in. That `.txt`/`.tex` divergence itself (in both directions,
across most chapters) remains unreconciled and is out of scope here — flagged separately,
not tracked as a numbered comment.

The 2026-08-09 pass re-checked every comment's anchor and claim directly against current
file content rather than trusting the prior pass — worthwhile, since the source has kept
moving both from comment-fix work (#7, #10, #11, #12 landed since 2026-08-08) and from
independent edits to `models.tex` and `related_works.tex` unrelated to any of these
comments (see cross-cutting notes). Several statuses and line-number anchors changed as
a result, including #7 a second time within the same day — `related_works.tex` was
restructured again after the first re-check, this time resolving it fully.

**Status**: All 23 comments are resolved in the current `.tex` source. None are
partially resolved or open.

---

## Resolved (23) — verified directly against `latex_template/chapters/*.tex`

- **#1** (p.15, `introduction.tex`): motivate OCPs before OCELs — the chapter now opens
  with why object-centric processes matter (parallel executions, shared objects) before
  introducing OCELs as their recording format (`introduction.tex:6-7`).
- **#2** (p.15, `introduction.tex`): GNN importance now precedes graph-encoding discussion
  — paragraph 3 introduces GNNs generally, paragraph 4 opens with graph-based encodings
  (`introduction.tex:9,11`).
- **#5** (p.17, `related_works.tex`): new `\subsection{Object-centric processes vs.
  traditional processes}` added ahead of predictive-monitoring content, within §2.1
  (`related_works.tex:19`).
- **#6** (p.22, `related_works.tex`): the gap claim now reads "...on object-centric
  processes encoded as heterogeneous graphs" (process-level wording) and cites ~10 named
  works, explicitly noting for each graph-based one that it lacks explainability
  (`related_works.tex:75`).
- **#8** (p.27, `preliminaries.tex`): every formal definition in the chapter now carries
  an inline `\cite{}` in its title, except the thesis's own original "Leave-One-Out
  Perturbation" definition (correctly left uncited, `preliminaries.tex:199`). Minor
  residual: `GNNExplainer`'s cite sits just outside the brackets
  (`preliminaries.tex:237`, `[GNNExplainer]\cite{ying2019}`) rather than inside — cosmetic,
  not worth re-opening.
- **#9** (p.27, `preliminaries.tex`): "sequences of events" (`preliminaries.tex:20`).
- **#13** (p.39, `models.tex`): both datasets now cited — Order Management
  `\cite{knopp2023a}` (`models.tex:11`), Logistics `\cite{knopp2023b}` (`models.tex:40`).
- **#14** (p.39, `models.tex`): "activities" in both occurrences (`models.tex:11,40`).
- **#15** (p.40, `models.tex`): Order Management now opens "The process begins when a
  customer places an order..." (`models.tex:13`), paralleling Logistics'
  "The process begins when a customer's order is registered..." (`models.tex:42`).
- **#16** (p.53, `results.tex`): PayOrder's train/val gap is now attributed to the
  validation split's intrinsically lower target variance (135.8h vs. 219.6h std), not
  overfitting, with the z-normalization and epoch-budget alternatives explicitly ruled
  out (`results.tex:52`).
- **#17** (p.54, `results.tex`): Depart's curve divergence is now attributed to the
  patience-10 discarded epochs past the deployed checkpoint, which itself shows no real
  train/val/test gap (432.1h/435.1h/439.9h) (`results.tex:64-65`).
- **#18** (p.55, `results.tex`): the "Overall" summary no longer claims overfitting for
  either dataset, attributing each pattern to its actual cause instead
  (`results.tex:67`).
- **#19** (p.56, `results.tex`): both parts fixed — the last-event "HGT wins" claim is now
  scoped per KPI with cited Wilcoxon p-values, including PayOrder's tie with k-dim GNN and
  PackageDelivered's outright loss (`results.tex:83`); the depth-summary caption now reads
  "Test MAE..." (`results.tex:89`).
- **#20** (p.56, `results.tex`): "terrible" replaced with a neutral description clarifying
  the -29796 figure belongs to the mean predictor, not HGT's own -30.8
  (`results.tex:84`). Minor residual typo in the same sentence — a missing space/dash in
  "(-30.8)smaller in magnitude" — worth a quick copyedit pass, not a content issue.
- **#12** (p.39, document-wide): all floats now standardized to `[t!]`. Both `models.tex`
  tables (`models.tex:22,102`, were `[htbp]`) and all 26 `results.tex` figures (were bare,
  no specifier) fixed; `preliminaries.tex:57` and `related_works.tex:137` already used
  `[t!]` from earlier comment fixes (#11, #7). Verified via full-chapter compile
  (`models`+`results`, 46 pages, zero errors, zero float warnings) that forcing top-only
  placement across `results.tex`'s unusually figure-dense content (26 figures in 260
  lines) doesn't trigger a float pileup or "too many unprocessed floats" error.
- **#10** (p.28, `dissertation.tex`): compiled-PDF check (previously flagged as
  unverifiable from source alone) found the concrete defect — `definition`'s default
  `trivlist` styling gives wrapped/`\\`-broken lines no hanging indent, so they snap
  flush to the page margin, visibly inconsistent with the `description` list used for
  the "Remaining Time" KPI example two paragraphs earlier, which does hang-indent.
  Fixed with `\AtBeginEnvironment{definition}{\hangindent=1.5em\hangafter=1}`
  (`dissertation.tex:6`, `etoolbox` already loaded by `Dissertate.cls`). Verified across
  all 9 `\begin{definition}` blocks in `preliminaries.tex` — every wrapped line now
  hangs consistently, including ones with nested `itemize` lists; no regressions, no new
  compile warnings. Vertical spacing (the comment's other suspected culprit) was
  checked and found already consistent — not changed.
- **#11** (p.29, `preliminaries.tex`): the raster `figures/ocel_example.png` is now a
  native LaTeX table (`preliminaries.tex:56-90`, `\label{tab:ocel_example}`, 17 rows
  reproducing two complete order traces from `order_management.sqlite`, with two data
  accuracy fixes over the original image along the way). Iterated twice more after the
  initial conversion to fix real page-fit problems (horizontal overflow from
  unconstrained numeric columns, then a second round dropping the `Customer` column
  entirely for a robust safety margin against font-metric differences) — all changes
  compiled and visually verified.
- **#3** (p.15, `introduction.tex`): the blank now reads "...predictive process
  monitoring on **object-centric processes**" (`introduction.tex:13`) — the exact
  process-level phrasing the remaining fix asked for, replacing the earlier
  "object-centric event logs."
- **#4** (p.15, `introduction.tex`): the same paragraph (`introduction.tex:13`) now ends
  "...is provided in the state-of-the-art review in Chapter 2," a forward pointer
  satisfying the "why these four components" ask. Minor residual: it's a literal
  "Chapter 2" rather than a `\ref{chp:relatedWorks}` cross-reference — good hygiene to
  fix eventually, not worth keeping the comment open over.
- **#7** (p.25, `related_works.tex`): checked against the verbatim annotation (extracted
  directly from `checked_thesis.pdf`'s PyMuPDF annotation layer, not a paraphrase):
  "This should be mentioned among those discussed in Section 2.3.2, because it's one of
  those. Then, this section should summarize the findings, which finally led to the
  choice. Ideally, you should have a table..." — three asks, all now met. `related_works.
  tex` has been restructured since the last check: `\subsubsection{Methodological
  template}` (`related_works.tex:131`) is no longer a separate, early, co-equal
  `\subsection` — it's now nested inside `\subsection{The GNN-explainability landscape}`
  (`related_works.tex:77`) as its last subsubsection, following the other four survey
  subsubsections and immediately preceding `tab:xai_comparison`
  (`related_works.tex:135-157`), satisfying "mentioned among those discussed... because
  it's one of those." That subsubsection opens with "Of the eight methods surveyed,
  Zhai et al.'s is the only one that has been applied to object-centric predictive
  process monitoring specifically..." (`related_works.tex:132`), satisfying "summarize
  the findings which finally led to the choice." Also fixed in the same pass: a stale
  cross-reference at `related_works.tex:126` ("As explained in the beginning of the
  section...") that no longer matched the restructured layout (Zhai et al.'s full
  treatment is now at the *end* of the section, not the beginning) — reworded to "As
  discussed further below."
- **#21** (p.57, `results.tex`): checked against the verbatim annotation: "Values
  0.24-0.39 are rather mediocre, this is not explained or motivated... why is there
  such a difference with the other case study where the numbers are certainly more
  satisfactory." Those 0.24-0.39 values were the stale, pre-fix Logistics numbers still
  sitting in the text (`results.tex:108`, "Logistics LOO 0.396 vs. GNNExplainer
  0.244") — the correct current values, already present in `results.txt:101` from this
  session's earlier fidelity-metrics refresh but never propagated to the `.tex`, are
  **0.975 vs. 0.757**. Corrected in place, plus one added sentence stating the
  reversal plainly: Logistics' characterization is now the *higher* of the two
  datasets, not the lower one — directly answering "why is there such a difference"
  (there effectively isn't one anymore, and what remains favors this dataset) without
  fabricating an unverified causal mechanism for a gap that no longer exists.
- **#22** (p.68, `results.tex`): checked against the verbatim annotation: "I wouldn't
  say that are shown to the stakeholders... I would tone down and just say that they
  are meant for the stakeholders. I don't get what you mean that the graphs show the
  computation of the fidelity metrics." Fixed the caption (`results.tex:250`,
  "...shown to the stakeholder" → "...meant for the stakeholder") and the confusing
  prose sentence (`results.tex:254`, "showcases the way fidelity metrics are computed"
  → names the three actual subgraphs shown: baseline, fidelity+, fidelity-, matching
  the figure's own subplot titles). Scoped to just this one occurrence (the only one
  inside §5.2.4, where the annotation is anchored) rather than the wider "throughout
  §5.2.4" scope the old proposed-fix note claimed — the other 3 occurrences of "shown
  to the stakeholder" in the chapter (`results.tex:133,219,230`) are in different
  subsections de Leoni didn't comment on.
- **#23** (p.68, `results.tex`): checked against the verbatim annotation: "this is a
  specific prefix. Why is it representative?... the characterization is again rather
  poor." Traced the actual generating script (`explanation_quality_showcase.py`,
  `ORDER_ID=1801`) against `counterfactual_showcase.py` (identical `ORDER_ID`,
  `DATABASE`, `KPI_LABEL`) — confirmed both use the same query graph. The honest
  answer is continuity with the immediately preceding Counterfactual Explanation
  worked example, not statistical representativeness; the text (`results.tex:254`) now
  says so directly, and connects the 0.565 score explicitly to the (corrected)
  aggregate range from #21 ("below the 0.815 aggregate LOO characterization reported
  for Order Management"), stating plainly that this trace is a below-typical case
  rather than glossing over it.

---

## Additional comments (`thesis_parts/deleonis_comment.txt`) — resolved (2)

A separate, plain-text note from de Leoni (not part of the 23 PyMuPDF-extracted PDF
annotations above), covering §5.2 "Explainability framework" in `results.tex`.

- **A. "No holistic 'helicopter view' on the explanations"**: "I don't see a proper
  attempt to aggregate the explanation of single instances into something more
  'holistic'... Section 5.2.1 reports aggregate results for the characterization
  comparison and the correlation between LOO and Shapley values, but this is about the
  quality of the explanations not about the helicopter view." Fixed by adding a new
  "Holistic aggregate view" subsubsection (`results.tex`, end of §5.2.1) with 4 new
  figures — one global, single-ranked-list LOO+Shapley bar chart per KPI (Figs.
  5.15-5.18), sourced from `explain_aggregate_shapley()` (the thesis's primary method,
  not the GNNExplainer-comparison method a similarly-named file elsewhere in the repo
  uses) via already-computed, previously-uncited assets found on disk — required no new
  model computation, only packaging (copied into `thesis_parts/latex_template/figures/`)
  and a label-collision cosmetic fix in `plot_aggregate_explanation_bars()`
  (`explainer.py:627`, regenerated all 4 from their cached CSVs to confirm). New prose
  explicitly contrasts these with the existing quality/fidelity figures already
  discussed, directly naming them the "helicopter view."
- **B. "order_id=1801 used repeatedly, no justification of representativity"**: "You
  repeatedly use order_id=1801 throughout the chapter, as if it's representative...
  leaving open the question if the instance was chosen primarily for its nice
  results." Confirmed accurate — 6 scripts hardcode the same order for narrative
  continuity. Fixed with a new upfront paragraph at the start of §5.2.1 (`results.tex`,
  before the first single-trace figure's prose) stating plainly: the trace is reused
  across the chapter for continuity, not representativeness; it was originally selected
  via a "slow trace" (bottom-quintile remaining-time) heuristic with no accuracy/fidelity
  criterion involved (traced to `loo_shapley_trace_showcase.py`); and its own
  explanation fidelity (0.565) is below the dataset average (0.815) — evidence against,
  not for, "chosen for its nice results." Forward-references the new holistic figures
  (fix A) as where the actual representative picture lives. The existing #23-fix
  sentence at `results.tex:287` was trimmed to a back-reference instead of restating.

Both verified via full end-to-end compile (`xelatex`→`bibtex`→`xelatex`×2, all
chapters, 85 pages): zero errors, zero undefined references, no new float-placement
failures from the 4 added figures (2 pre-existing, already-vetted "Float too large"
warnings unchanged, no new ones).

---

## Self-initiated audit: are all thesis graphs up to date? (2026-08-09) — resolved (6)

Not a de Leoni comment — the user asked directly whether every figure in the thesis
reflects the latest results, given how many independent model retrainings happened
this session. Ran a three-pronged investigation (figure-vs-generating-script diff,
canonical checkpoint-promotion audit, numeric cross-referencing of every specific
number in `models.tex`/`results.tex`) rather than spot-checking, since this exact
"fixed in a staging file but never propagated to the `.tex`" failure mode had already
been hit twice this session (#21, deleonis_comment.txt). Found and fixed 6 issues:

1. **11 stale figures**: `thesis_parts/latex_template/figures/` (what the PDF shows)
   had diverged from `thesis_parts/figures_tables/` (current script output) for
   `training_curves_order_management.png`, `training_curves_logistics.png`,
   `combined_baseline_comparison_table.png`, `mae_by_depth_summary.png`,
   `fidelity_comparison_summary.png`, `loo_vs_shapley_agreement.png`,
   `loo_shapley_trace_showcase.png`, `loo_shapley_edge_trace_showcase.png`,
   `single_subgraph.png`, `prefix_evolution.png`, `loo_shapley_aggregate_showcase.png`
   — all downstream of the Depart/LoadToVehicle Adam→AdamW collapse fix (promoted
   Aug 5 and Aug 7). Confirmed via MD5/pixel diff and re-copied from the already-
   correct `figures_tables/` source (no regeneration needed).
2. **`results.tex:117`**: Spearman's ρ for Logistics LOO-vs-Shapley agreement was
   `-0.66`, directly contradicting the figure it describes (`fig:loo_shapley_comp`,
   now showing ρ=0.83) — a self-contradiction, not just a stale number. Corrected to
   0.83 (independently recomputed as 0.825) and the surrounding interpretation
   rewritten from "significant negative correlation" to reflect the actual strong
   positive rank agreement, adding a concrete redundancy-correction example
   (`BringToLoadingBay`, LOO -17.3h vs. Shapley -31.3h pooled mean) to preserve the
   original methodological point (why Shapley is still needed) with accurate data.
3. **`results.tex:254`**: the counterfactual worked example's "10 hours" minimum
   threshold was fabricated — `explain_counterfactual(1801, n_results=3)` used no
   `min_gap_hours` argument, so the actual default of 0h applied. Corrected using
   wording already drafted (but never propagated) in `results.txt:213`.
4. **`models.tex` Table 3.2** (`tab:final_hyperparameters`): the Logistics column
   (layers=3, hidden=16, lr=0.01, Adam) was verbatim the collapse-inducing pre-fix
   configuration, and structurally couldn't represent Logistics correctly anyway
   since its two KPIs now have different promoted hyperparameters post-fix (unlike
   Order Management, where both KPIs share one configuration). Expanded to 3 columns
   (Order Management / Logistics-Depart / Logistics-LoadToVehicle) with current values
   from each KPI's `model_params.json`; updated two dependent prose sentences (the
   layer-depth search result, previously claiming a single "layer depth 3" winner,
   and the optimizer description) for consistency with the new table.
5. **`results.tex:80`**: the GBT-vs-baselines characterization ("second worst
   overall... reversal on LoadToVehicle") was internally contradictory and didn't
   match the current `combined_baseline_comparison.csv` (GBT is worse than both HGT
   and HomoGNN on LoadToVehicle, not better). Rewritten from the current data: GBT is
   worst on PayOrder, second-best on PackageDelivered, and mid-pack on both Logistics
   KPIs — genuinely inconsistent across KPIs rather than uniformly ranked.
6. **Two figure/caption mismatches found while re-copying** (not pre-existing bugs —
   a side effect of the refreshed data itself now covering more KPIs): `prefix_
   evolution.png` and `single_subgraph.png` both grew from a single-KPI 2-panel
   layout to a 4-KPI 8-panel and 4-panel layout respectively once regenerated against
   the fixed Depart/LoadToVehicle checkpoints, but their captions/prose still said
   "two different prefixes of the same process execution" / "one of the traces shown
   before" (singular). `prefix_evolution.png`'s new size also genuinely overflowed
   the page (visibly overlapping the page-number footer) — fixed with `width=0.85\
   textwidth`. Both captions and the surrounding prose rewritten to describe the
   actual 4-KPI content, with `prefix_evolution`'s prose citing a concrete example
   (`ConfirmOrder` flipping from positive to negative driver between prefix and
   last-event on PayOrder) instead of a now-meaningless hardcoded "prefix 4 and 12."

Also confirmed clean, no action needed: the 4 `aggregate_shapley_global_*` figures
added for deleonis_comment.txt (their source CSVs post-date both the Depart and
LoadToVehicle checkpoint promotions). Wrote `promote_aggregate_shapley_global_
figures.py` (repo root) as a permanent, reproducible generator for them, replacing
the throwaway temp script used originally — verified it reproduces the existing
files byte-for-byte.

Noted but out of scope (repo-hygiene, not thesis-content): LoadToVehicle's checkpoint
promotion is itself uncommitted in git, per `DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md`'s
own flag — worth committing separately, alongside Depart's (already in `000ddb0`).

Verified via full end-to-end compile (`xelatex`→`bibtex`→`xelatex`×2, all chapters,
87 pages): zero errors, zero undefined references, back to only the 2 pre-existing,
already-vetted "Float too large" warnings (no new ones after the width fix). All 6
fixes visually spot-checked against the rendered PDF.

---

## Cross-cutting notes

- **`.txt`/`.tex` divergence** (not a de Leoni comment, flagged for awareness): most
  chapters' `.tex` files have diverged from their `thesis_parts/*.txt` counterparts in
  both directions — some fixes (like all of #16-20 above) were applied only to the `.tex`
  build files; #21's fidelity numbers were the reverse case (fixed in `results.txt` but
  not `results.tex`) until this pass propagated them. `results_payorder_rewrite_draft.md`
  and `DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md` (both written against `results.txt`) are
  still stale relative to the real build source for the sections they cover. Reconciling
  the rest of this divergence is a separate task.
- **Bibliography** (`thesis_parts/latex_template/references.bib`, the file actually
  `\bibliography{}`-referenced by the build — a second, older, diverged copy exists at
  `thesis_parts/references.bib`): the 9 previously-vetted citation keys
  (`adams2022, huang2022, zhai2025, smit2024, stevens2022, castro2009, galantiDeleoni2024,
  galanti2023a, galanti2023b`) are confirmed present and correct. The newly load-bearing
  keys added by #6's and #13's fixes (`knopp2023a, knopp2023b, gherissi2025, weinzierl2020,
  wickramanayake2022, pasquadibisceglie2021, hsieh2021, mehdiyev2025`) are all genuinely
  `\cite{}`'d and spot-checked against matching papers in `files/papers_full/` — one
  year discrepancy noted (`weinzierl2020`'s matching PDF is filed as `weinzierl2021.pdf`,
  worth a one-line verification of the actual publication year). Two keys
  (`meyes2019`, `sindhgatta2020`) are present in the bib but cited nowhere in any
  chapter — plausibly relevant but missed (both are thematically on-topic: ablation
  studies, PPM explainability) rather than dead cruft; worth a look if the citation base
  is revisited, not urgent. 9 entries still carry mild, cosmetic `note=` fields (short tags
  like "LORELEY", "PGExplainer" — not the leaked internal-audit-trail commentary an earlier
  session's `pdf_review.md` worried about, which is confirmed already absent here);
  `IEEEtran` likely doesn't render `note` for `@article`/`@inproceedings` entries, making
  this probably moot — worth confirming at compile time rather than treating as a live
  visible issue.
- **`models.tex` is being edited independently of this document's comment-fix work**:
  the 2026-08-09 re-check found content there (e.g. the weight-decay/dead-projection bug
  fix now documented at `models.tex:124`) that wasn't present in the 2026-08-08 pass and
  isn't attributable to any of this session's edits — it shifted every anchor below the
  first table by ~11 lines. None of the numbered comments' *claims* were affected, only
  their line numbers (now corrected), but it's a reminder that anchors in this file are a
  snapshot, not a guarantee — worth a quick grep-for-content re-check rather than a
  trust-the-line-number one before citing them again.
- **No comments were found on the Conclusion chapter.**
