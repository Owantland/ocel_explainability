# Review: `OCEL_Explainability_Thesis.pdf` — open items

Round 2. Round 1 sampled the PDF (front matter, Introduction, complete Results
and Conclusion chapters, References, Acknowledgments) and framed the review as
a diff against the staging `thesis_parts/*.txt` files. This round reads the
document as a standalone, complete work — compiled elsewhere from more
complete chapter sources than what's staged locally — and closes the remaining
gaps: the rest of Related Works (§2.1.2-2.4: GNNs, the HOEG comparison
baseline, the full XAI/GNN-explainability landscape, the methodological-template
decision), all of Preliminaries (§3.1-3.4.4: the complete formal definitions —
event logs, KPI functions, process executions, prefixes, LOO/Shapley,
gradient/feature attribution, the four-part counterfactual dissimilarity
metric, fidelity/sparsity), and the start of Models (§4.1: both dataset
descriptions and Table 4.1's summary statistics). Combined with round 1's
reads, the PDF has now been read essentially cover to cover.

**Still worth flagging up front**: the local LaTeX project directory
`/Users/owantland/Documents/UPD/thesis/OCEL_Explainability_Thesis/` is *not*
the source this PDF was built from — its `chapters/*.tex` are still the
Dissertate template's stock placeholder text, its `references.bib` has one
entry, and its `frontmatter/personalize.tex` still has the template's joke
defaults. The real PDF (real title page, a real 27-entry bibliography, 75
populated pages) must be built elsewhere, most likely an Overleaf project
outside this repo. Anyone opening this repo should disregard that local
directory as a source of truth.

This round found one significant new factual error — only catchable by
cross-referencing the PDF's own numbers against the project's actual file
system — plus a typo, on top of re-confirming that round 1's three findings
are real properties of the document itself, not artifacts of comparing
against the staging files.

## 1. Table 4.1's Logistics trace count is wrong

Table 4.1 ("Summary of the datasets used in this thesis," page 25) reports
**2000** traces for *both* Order Management and Logistics. Checked directly
against the file system: `files/models/order_management/` has a `2000/`
production variant, confirming Order Management's 2000 is right. But
`files/models/logistics/` has **no `2000/` directory at all** — only `1000/`,
which is where every production Logistics checkpoint actually lives, including
the exact `TimeFrom_CustomerOrder_to_LoadToVehicle`/`_Depart` models behind
every Logistics figure and table in the Results chapter. The `cant` parameter
(dataset-size variant) used for every real Logistics result in this thesis is
1000, not 2000 — Table 4.1 almost certainly has a copy-paste error, reusing
Order Management's value for both rows. Worth correcting the Logistics row to
its real trace count.

## 2. Typo

Page 25 (Models chapter intro): "Finally, I explain the **the** explainability
framework..." — duplicated word.

## 3. Bibliography note-field leakage — FIXED

Three entries in `references.bib` — `castro2009`, `smit2024`, `gherissi2023` —
had internal research/audit-trail commentary embedded directly in their
BibTeX `note` field, rendering verbatim in the PDF's actual References section
(pages 57-58), visible to any reader or examiner (e.g. `smit2024`'s note read
"...Also audited directly against the authors' own released code
(OCPPM-master/) -- see thesis_parts/hoeg_codebase_audit.md.").

**Revalidated**: re-checked the full file for every `note` field, not just
these three. Found 12 total — the 3 with leaked audit commentary above, plus
9 harmless-looking short tags on other entries (`huang2022`: "LORELEY";
`li2023`: "HTGExplainer"; `luo2020`: "PGExplainer"; `stevens2022`: "KU Leuven";
`sundararajan2017`: "Integrated Gradients"; `wei2024`: "Paper 19, Kraków";
`yuan2022`: "Also available as an arXiv preprint"; `adams2023` and
`deleoni2016`: "No local copy available; read online"). Per the instruction
that there should be no notes at all, removed the `note` field from all 12
entries (not just the 3 problem ones), fixing each entry's trailing comma
accordingly. Verified afterward: `grep -n "note\s*=" thesis_parts/references.bib`
returns nothing, brace count is balanced (199 open / 199 close), and all 29
entries still parse as well-formed BibTeX (checked visually end to end).
`thesis_parts/references.bib` is very likely the real source file for the
PDF's bibliography — its entry count and content match the PDF's References
section closely.

## 4. Figure 5.9 contradicts its own supporting prose for the Logistics dataset (confirmed)

The text (pages 43-44) claims LOO and full Shapley Values show "a large degree
of agreement" between the two datasets' identified importance values. Figure
5.9's own annotation tells a different story: Spearman's ρ=+0.92 for Order
Management (consistent with the claim) but **ρ=−0.66 (p=1.8e-07, n=50) for
Logistics** — a statistically significant *negative* correlation, i.e. rank
disagreement, not agreement. This directly contradicts the "large degree of
agreement" claim for half of the validation. This is a real result from the
underlying data, not a typo — worth either narrowing the claim to Order
Management specifically, or investigating why Logistics' LOO/Shapley rankings
actually diverge before making a blanket "large degree of agreement" statement.

## 5. Conclusion chapter's four bugs (confirmed)

Checked the actual compiled Conclusion chapter (pages 55-56) word-for-word —
all four issues found in `conclusion_review.md` are present:
- "outperforming the heterogeneous model on half of the tested KPIs" — still
  3-of-4 by point estimate (only PackageDelivered favors HGT on all-prefix
  MAE), not half.
- "In order to further create trust in the stakeholder assessment metric are
  also provided" — singular/plural mismatch.
- "Current limitations for the explainability framework partain to..." —
  typo for "pertain."
- "...in order to validate its use as on predictive process monitoring [26]"
  — garbled phrase.

## No new issues found in Related Works §2.1.2-2.4, Preliminaries, or Models §4.1

The GNN-explainability landscape survey (HGExplainer, HTGExplainer,
GNNExplainer, PGExplainer, GNNInterpreter, NICE, LORELEY), the HOEG
comparison-baseline discussion, and the full formal preliminaries (event logs,
KPI functions, process executions, prefixes, LOO/Shapley, gradient
attribution, the four-part counterfactual dissimilarity metric,
fidelity/sparsity) are dense but internally consistent and read cleanly. The
Table 3.1 example OCEL data (customer/product names) matches the same
synthetic entities seen throughout this session's real explainer output — a
good consistency signal, not a separate finding. No broken cross-references
(`\ref` labels resolve correctly) or literal "??" markers turned up anywhere
across the full read.

## Scope note

The document has now been read essentially in full: title/front matter, Table
of Contents, Introduction, complete Related Works, complete Preliminaries,
Models (dataset descriptions, Table 4.1, hyperparameter/architecture/baseline
sections), complete Results (all subsections, all 26 figures), complete
Conclusion, full References, Acknowledgments.

## Overall assessment

The compiled PDF is a real, substantially complete, well-argued 75-page
thesis. Reading it end to end surfaced one genuinely new, well-evidenced
factual error (Table 4.1's Logistics trace count) that only a file-system
cross-check could catch, alongside a small duplicated-word typo. Of the three
issues carried over from round 1, the bibliography note-field leakage is now
fixed (all 12 `note` fields removed from `references.bib`, not just the 3
problem ones); Figure 5.9's statistical contradiction for Logistics and the
Conclusion chapter's four language/quantifier issues remain open. Everything
else read in this pass (Related Works, Preliminaries, Models §4.1) is sound.
