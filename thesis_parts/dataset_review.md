# Review: `dataset.txt` — open items

Rechecked against the current (LaTeX) text of `dataset.txt`. All 6 items from the last round are
resolved and dropped. `related_work.txt` and `thesis_structure.txt` are both unchanged since their
own last checks, so the one carried-forward lower-priority item needed no re-verification beyond
confirming its status is unchanged. One new issue turned up in a fresh full read.

## 1. Notation inconsistency in the Shapley Value definition's feature-set notation

`Definition[Permutation-Sampling Shapley Value]` defines `$X=\{x_j,v_j \in p\}$` — a comma where
the parallel `Definition[Leave-One-Out Perturbation]` three lines above it correctly uses `\mid`:
`$X=\{x_j \mid v_j \in p\}$`. As written, the Shapley version reads as an ambiguous set containing
two unrelated things (`x_j` and the boolean-looking `v_j \in p`) rather than proper set-builder
notation. Fix to match: `\{x_j \mid v_j \in p\}`.

## 2. Lower priority — hardcoded cross-chapter section references

Unchanged from the last round: several places still hardcode cross-references to `related_work.txt`
as plain text ("Section 2.1.2," "Section 2.2," "Section 2.2.2" ×2, "Section 2.2.5," "Section
2.2.6") instead of `\ref{}`. `related_work.txt` is unchanged since the last check (still no
subsection-level `\label{}` anchors, so these can't yet be converted), and its current subsection
ordering still keeps every one of these references correct by coincidence. Not currently wrong;
carried forward at the same lower priority as a standing fragility risk.
