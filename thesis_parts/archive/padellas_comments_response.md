# Response to `padellas_comments.txt`

**Status (2026-10-12): applied.** Both snippets below were added to `results.tex`/`conclusion.tex`
as drafted, with the two open choices resolved as: heading **"Scope of the predictive results"**
(the softer, context-setting option), and the conclusion sentence inserted at the **start** of
the existing limitations paragraph. Recompiled end-to-end with zero new errors/warnings.

## The comment

> I feel that the explanations part is okay, but it is built on a framework that's producing
> not-so-effective predictions (MAE high with respect to the process avg durations and not so
> different from other techniques)
>
> The only way to try to mitigate this problem is to report it. You can add a "Limitation"
> chapter or a Section at the end of the prediction quality-related part in which you state that
> "This thesis tries to deliver an explanation framework, I however acknowledge that the provided
> prediction are not qualitatively better if compared to the other state-of-the-art methods (cf.
> Section~\ref{the_section_with_results_compared_with_other_approaches})." It is important to
> highlight that the main scope of this thesis is to provide explanations and not only
> predictions, so for the aim of this thesis is enough that prediction are working, and not that
> they outperform the state of the art.

**One-line summary:** the explainability framework is fine, but it's built on predictions that
aren't clearly better than existing techniques — the fix isn't to improve the predictions, it's
to say so explicitly, framed against the thesis's actual scope (explanations, not SOTA accuracy).

This is a framing/acknowledgment gap, not a data gap: `results.tex` §5.1.3 already reports the
comparison neutrally (e.g. "it cannot be claimed with certainty that the heterogeneous
representation is head-and-shoulders above the others, echoing the results obtained in previous
works," line 81) and `conclusion.tex` already has a "Current limitations" paragraph — but that
paragraph is scoped entirely to the explainability framework (limited XAI options for
heterogeneous models, Shapley's compute cost, temporal-split sensitivity) and says nothing about
predictive quality relative to the state of the art. What's missing is the explicit statement
Padellas is asking for, in both of the places a reader would naturally look for it.

## Proposed fix — Results chapter

Add a new subsection at the end of §5.1 "GNN experiments," directly after §5.1.3's baseline
comparison and before §5.2 "Explainability framework" begins (`results.tex`, between the current
lines 95 and 97) — this is the literal location Padellas suggested ("a Section at the end of the
prediction quality-related part"). It also adds a `\label` to the existing baseline-comparison
subsection heading so this can be a proper cross-reference instead of a hardcoded section number
(matching this thesis's existing `\label{sec:...}`/`\ref{...}` convention, e.g.
`\label{sec:models_baselines}` in `models.tex`).

**Change to the existing heading** (`results.tex:69`):
```latex
\subsection{Model comparison against baselines}
\label{sec:results_baselines}
```

**New subsection** (insert after line 95, before `\section{Explainability framework}` at line 97):
```latex
\subsection{Scope of the predictive results}
    The previous sections presented a full accuracy comparison between the heterogeneous
    predictive model and several baselines, and it is important to state plainly what should be
    concluded from those results. As shown in Section~\ref{sec:results_baselines}, the
    heterogeneous model is not shown to be qualitatively better than the other techniques it was
    compared against, and on several KPIs performs comparably to, or on the full prefix set no
    better than, considerably simpler baselines.

    This is an accepted outcome rather than a shortcoming specific to this thesis's approach: the
    thesis's central contribution is the explainability framework built on top of these
    predictive models, not an improvement to predictive accuracy itself. For that framework's
    evaluation to be meaningful it is sufficient that the underlying predictive model produces
    working, non-trivial predictions; outperforming the state of the art was never a goal of this
    work. The predictive results reported above should therefore be read as context for
    interpreting the explanations that follow, rather than as a limitation of the explainability
    framework itself.
```

*(Heading name "Scope of the predictive results" can be swapped for something closer to
Padellas's own "Limitation" wording, e.g. "Limitations of the predictive models" — flagged as an
open choice below.)*

## Proposed fix — Conclusion chapter

Fold one additional sentence into the existing "Current limitations" paragraph
(`conclusion.tex:18`), so a reader who only checks the conclusion's limitations paragraph — the
more conventional place an examiner looks — doesn't miss this. Proposed insertion point: at the
start of the paragraph, before the existing "Current limitations for the explainability
framework pertain to..." sentence, so the predictive-quality limitation isn't presented as
subordinate to the explainability-specific ones that follow:

```latex
It is also worth reiterating that the underlying predictive models are not shown to outperform
existing state-of-the-art approaches (Section~\ref{sec:results_baselines}); this is an accepted
trade-off given the thesis's explanatory rather than predictive aim, and does not undermine the
evaluation of the explainability framework itself. Current limitations for the explainability
framework pertain to the limited amount of explainability options available for use with
heterogeneous models. [...]
```

## Rationale for placement

- Padellas's comment offers an either/or ("a 'Limitation' chapter or a Section at the end of the
  prediction quality-related part") — doing both isn't over-engineering, it's covering the two
  places a reader would plausibly look: right where the comparison numbers are (Results) and in
  the thesis's dedicated limitations paragraph (Conclusion). The Conclusion sentence is a single
  cross-referencing line, not a duplicate essay — it points back to the Results subsection rather
  than restating it.
- Neither addition contradicts or duplicates existing text. `results.tex` §5.1.3 already reports
  the underlying numbers and even echoes the same conclusion informally (line 81's "cannot be
  claimed... head-and-shoulders above the others"); the new subsection makes that conclusion's
  implication explicit and ties it back to the thesis's stated scope, which nothing currently
  does. `conclusion.tex`'s existing limitations paragraph is left otherwise untouched.
- Both snippets are written in this thesis's established impersonal/passive voice rather than
  Padellas's first-person suggested phrasing ("I however acknowledge..."), consistent with the
  voice decision made earlier for `models.tex`/`preliminaries.tex`/`related_works.tex`.
- New label `sec:results_baselines` checked against the whole `latex_template/` tree — no
  collision.

## Open choices before this is applied

1. **Subsection heading wording** — "Scope of the predictive results" (softer, frames it as
   context-setting) vs. "Limitations of the predictive models" (matches Padellas's own
   "Limitation" language and `conclusion.tex`'s existing "Current limitations" heading more
   directly). No strong reason to prefer one; matching Padellas's own word choice may read as
   more directly responsive to the comment.
2. **Conclusion insertion point** — proposed at the *start* of the existing limitations
   paragraph (so it isn't read as subordinate to the explainability-specific items). An
   alternative is appending it at the *end* of that paragraph instead, after the existing
   train/val temporal-split sensitivity item.
3. Nothing else in this response is contingent on these two choices — the underlying content
   and both locations were not otherwise in question.
