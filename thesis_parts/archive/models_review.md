# Review: `models.txt` — open items

Rechecked against the current text of `models.txt`. All 5 items from the previous review round are
resolved — two of them via a change to `dataset.txt`/`references.bib` rather than `models.txt`
itself, both confirmed valid:

- The broken `\cite{deleonieVolpato}` key is fixed — `references.bib`'s entry was renamed from
  `deleonivolpato` to `deleonieVolpato` (rather than changing the citation to match the old key).
  Confirmed no file still points at the old key (`grep -rn "deleonivolpato" thesis_parts/` — no
  hits); this also matches how `dataset.txt` and `related_work.txt` already cited this work, so it's
  the more consistent fix.
- The two bracket-style citations are now `\cite{huang2022}` and `\cite{zhai2025}`, both valid keys.
- The missing cross-references are fixed with real `\ref{}`s (`sec:dataset_perturb`,
  `sec:dataset_gradient`, `sec:dataset_cf`, `sec:dataset_metrics`) rather than hardcoded section
  numbers — `dataset.txt` was itself edited to add all four corresponding `\label{}` anchors at the
  relevant subsubsections, confirmed present and correctly targeted.
- GNNExplainer's comparison-only role now states its justification directly (bounded [0,1] mask vs.
  physically meaningful shift; no edge importance on a heterogeneous model), tied to the new
  cross-reference.
- The num_layers sweep now cites the actual values searched — but this fix introduced a small wording
  slip, see below.

Two small items remain.

## 1. Leftover phrasing glitch: "between among"

Line 42: "...also searched for an optimal number of layers greater than 2, specifically **between
among** the values 3, 4, and 5." Doubled/conflicting prepositions — reads like "between 3 and 5" was
edited into "among the values 3, 4, and 5" without removing "between." Since three discrete values
are listed, not a range, "among" alone is correct: "...specifically among the values 3, 4, and 5."

## 2. Minor: one remaining hardcoded section reference

Line 42 also has "given the problem discussed in **Section 3.3**" — still a plain-text section
number, unlike this round's other four fixes, which all replaced similar references with `\ref{}`.
Checked `dataset.txt`: the Logistics Dataset section (`\section{Logistics Dataset}`, line 346) has no
`\label{}` yet, so this one can't be converted the same way without first adding a label there — a
`dataset.txt` change, outside this file's own scope, but flagged here since it's now the one
inconsistent reference left in an otherwise fully `\ref{}`-based file.

No other factual or technical drift found this round — every other claim checked in the previous
review (train/test split, optimizer/loss, HomoGNN architecture, sweep grid, viability-doc summaries,
`explainer.py`-backed stakeholder outputs) is unchanged and still accurate.
