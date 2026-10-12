# Review: `conclusion.txt` — open items

First review round — no prior version of this file exists. Read `conclusion.txt`
in full and fact-checked every numeric/factual claim directly against the
underlying data (the four canonical `model_comparison_*.csv` files, one per KPI,
excluding two stale non-canonical files also present on disk —
`model_comparison_CustomerOrder.csv` and `model_comparison_TransportDocument.csv`
— neither of which is part of the current 4-KPI comparison), cross-checked
citation keys and terminology against `related_work.txt` and `references.bib`,
and proofread for typos/grammar, matching the verification depth of every prior
chapter's first review round.

Nine items found: one factual error, one confirmed-accurate claim worth noting,
four grammar/typo fixes, one cross-file consistency confirmation with a minor
omission, and two clarity notes.

## 1. Numeric error: "half of the tested KPIs" overstates how close all-prefix performance was

Line 9 claims the homogeneous model is "outperforming the heterogeneous model on
half of the tested KPIs" (all-prefix comparison). Checked directly against all 4
canonical `model_comparison_*.csv` files: HomoGNN actually beats HGT's all-prefix
MAE on **3 of 4** KPIs by point estimate (PayOrder: 157.46 vs. 167.75; Depart:
112.75 vs. 113.42; LoadToVehicle: 123.90 vs. 138.90) — only PackageDelivered favors
HGT (194.88 vs. 196.40). "Half" doesn't match this count either way.

Digging further with each KPI's confidence intervals: only LoadToVehicle shows a
statistically clear (non-overlapping CI) advantage for HomoGNN — HGT's CI
[136.61, 141.34] doesn't overlap HomoGNN's [122.10, 125.86]. PayOrder and Depart
are within noise (their CIs overlap substantially), matching `results.txt`'s own
careful framing ("cannot be claimed with certainty... overlapping estimate
ranges"). So "half" is wrong under both readings — 3/4 by point estimate, or 1/4
if only statistically-meaningful differences count — and should be corrected to
whichever framing the chapter intends.

## 2. Confirmed accurate: HGT wins all four KPIs on last-event MAE

Line 9's other claim — "the heterogeneous model obtained better accuracy on all
four KPI tests" for the last-event comparison — checks out: HGT has the lowest
last-event MAE in all 4 canonical KPI files (PayOrder, PackageDelivered, Depart,
LoadToVehicle). No issue here.

## 3. Grammar: duplicated article

Line 9: "meant to emulate **a the** processes of an Order Management and a
Logistics company" — conflicting articles left over from an edit. Should read
"the processes" or "a process."

## 4. Grammar: subject-verb number mismatch

Line 12: "In order to further create trust in the stakeholder **assessment metric
are** also provided" — should be "metrics" (plural), matching "Characterization
and sparsity **are** shown..." two sentences later in the same paragraph.

## 5. Typo

Line 18: "Current limitations for the explainability framework **partain** to..."
should be "pertain."

## 6. Grammar: garbled phrase

Line 21: "...in order to validate its use **as on** predictive process monitoring
\cite{li2023}." The "as on" doesn't parse — likely needs "validate its use **on**
predictive process monitoring" (drop the stray "as"), or "validate its use **as a
[method/explainer] for** predictive process monitoring" if a noun was meant to
follow "as."

## 7. Confirmed accurate: HGExplainer/HTGExplainer limitations and citation keys

Line 18's claim that HGExplainer lacks an open-source implementation and
HTGExplainer had "a failed port from DGL into Pytorch" matches `related_work.txt`
(lines 114-115) exactly, in the same order ("respectively" maps correctly to
HGExplainer→no-open-source, HTGExplainer→failed-port). The citation keys
`deleonieVolpato` and `li2023` (line 21) are both valid entries in
`references.bib` and used consistently everywhere else in the thesis
(`dataset.txt`, `models.txt`, `introduction.txt`, `related_work.txt`) — no
mismatched or stale keys.

## 8. Minor completeness/consistency note

Line 18's limitations sentence gives only one reason HGExplainer was ruled out
("no open-source implementation"), but `related_work.txt`:114 gives a second,
arguably more fundamental, reason: its scope is "specifically for
link-prediction/recommender-systems tasks, not general regression." Omitting this
isn't wrong for a conclusion-chapter summary, but it drops the stronger of the two
disqualifying reasons. Separately, neither HGExplainer nor HTGExplainer is
inline-cited in that sentence (`mika2023` / `li2023`), even though `li2023` IS
cited two sentences later when HTGExplainer reappears in the future-work
paragraph (line 21) — a small citation-style inconsistency within the same short
chapter.

## 9. Clarity notes (not errors)

- Line 9: "For each dataset two heterogeneous graph neural networks are trained
  and tested for predicting the value of a given KPI" could be misread as two
  networks per KPI rather than the intended one network per KPI, two KPIs per
  dataset. A small rewording (e.g. "one heterogeneous graph neural network per
  KPI, two KPIs per dataset") would remove the ambiguity.
- Line 12: "applied to all four regression models for testing" means the four
  KPI-specific HGT models, not the four baseline model *types*
  (Mean/GBT/HomoGNN/HGT) discussed earlier in the results chapter. Both readings
  are plausible out of context; worth a one-word clarification (e.g. "all four
  KPI-specific regression models").

Overall, the chapter's substantive claims are largely sound — the HGExplainer/
HTGExplainer limitations and citation keys check out cleanly, and the headline
last-event result (HGT wins all four KPIs) is accurate. The one real problem is
item 1's "half" quantifier, which understates how consistently the homogeneous
model matched or beat HGT on all-prefix metrics; the rest are typos, grammar, and
small precision/clarity tweaks rather than evidence of a deeper issue with the
chapter's argument.
