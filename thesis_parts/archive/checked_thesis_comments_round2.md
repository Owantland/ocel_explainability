# Review: supervisor comments on `checked_thesis.pdf` (Round 2)

`thesis_parts/checked_thesis.pdf` was re-annotated by Prof. de Leoni since the round
tracked in `checked_thesis_comments.md` (23 comments + 2 extra items, all resolved as
of 2026-08-09). The file on disk changed shape between rounds — 20MB→7.6MB, 75→87
pages, `pdfinfo` Producer now `iOS Version 17.7.7 ... Quartz PDFContext`, `ModDate`
2026-08-12 13:16 CEST (Round 1's file: `Microsoft: Print To PDF`, `ModDate` 2026-07-30)
— because this round was marked up in an iOS app that flattens ink directly into page
content rather than writing interactive PDF annotation objects.

**Extraction-method note**: PyMuPDF's `page.annots()`, which found all 23 Round-1
comments cleanly, returns **zero** results on this file — not a bug, the file
genuinely contains no annotation objects (confirmed via a raw byte-level grep for
`/Highlight`, `/FreeText`, `/Popup`, `/Ink`, `/StrikeOut`, `/Squiggly`, `/Note`: zero
matches anywhere). Recovered instead via `page.get_drawings()`, filtered on the
markup's distinctive red stroke color `(0.984, 0.192, 0.271)`, which locates every
marked page cheaply (17 of 87, zero false negatives cross-checked against stroke-only
paths too); each flagged page was then rendered at high DPI via `page.get_pixmap()`
and read visually (OCR isn't reliable on cursive ink).

**Verification pass (2026-08-13)**: `abstract.tex`, `introduction.tex`,
`related_works.tex`, `preliminaries.tex`, and `models.tex` were all edited since this
file was first written. Rather than assume "the file changed" means "the comment was
addressed," three parallel passes read the current `.tex` content directly against
every R1–R17 item below, line by line, with a handful independently spot-checked a
second time. Result: **15 of 17 resolved**, 4 sub-marks still open across 3 items —
plus, importantly, **the edit pass itself introduced 12 new correctness issues**,
logged in their own section below so they aren't silently absorbed into "resolved."
(R14/R15b were initially misjudged as permanently unrecoverable, and R16 was
misjudged as an unaddressed content question — both corrected the same day per the
user's direct clarifications; see Resolved below. All 12 new issues were also fixed
the same day.)

---

## Resolved (15)

- **R6** (p.3, `introduction.tex:19`): "Chapter 3 introduces the theoretical
  background." — the redundant "on the topics introduced in Chapter 2" clause is gone.
- **R7** (p.6, `related_works.tex:20`): the redundant second "(OCELs)" expansion is
  gone; only the bare acronym remains, relying on the definition already given at
  `related_works.tex:6`.
- **R8** (p.7, `related_works.tex:39`): "...proposed by Adams et al.\cite{adams2022}
  can then be used..." — the citation is now attached directly to that mention.
- **R9** (p.16, `related_works.tex`, structural): the entire "2.5 Summary and proposed
  framework" section is gone from the file (confirmed via direct grep — zero matches
  for that heading anywhere). It never had its own `\label{}`, so no dangling
  cross-reference was created by the deletion; Round 1's residual "Chapter 2"
  plain-text mentions in `introduction.tex` (lines 13, 19) point at the whole
  related-works chapter, not specifically §2.5, so they're unaffected.
- **R10** (p.17, `preliminaries.tex`): (a) `:9` — heading now reads "Case-centric event
  logs and predictive process monitoring." (b) `:20-21` — "a time mapping **function**
  $\pi_{time}$" / "an activity mapping **function** $\pi_{act}$," "mapping" → "mapping
  function" in both places.
- **R11** (p.18, `preliminaries.tex`): (a) `:26` — "...key performance indicator
  **(KPI)**. Therefore, it is necessary to provide a definition of the KPI function."
  (b) `:35` — "**This** KPI definition assumes it to be computed a posteriori..." (was
  "My").
- **R12** (p.22, `preliminaries.tex:165`): heading now reads "Predictive process
  monitoring on object-centric processes" (was "Predictive analytics...").
- **R13** (p.31, `models.tex`): all four flagged headings renamed —
  `\section{Use Cases}` (`:8`), `\subsection{Order Management}` (`:9`),
  `\subsection{Logistics}` (`:38`), `\subsection{Event log processing}` (`:52`),
  `\subsection{Train/test split of the event logs}` (`:73`). The technical OCEL
  vocabulary elsewhere (`\label{sec:dataset_OCEL}`, body-prose "the datasets") was
  correctly left untouched, matching the comment's `models.tex`-only scope.
- **R14 / R15** (p.34-35, `models.tex`): the illegible margin word originally
  catalogued separately as R14, and R15's clipped "MARGIN" note, are the same ask as
  R15's main note ("IF THIS PROCEDURE HAS BEEN DONE SOMEWHERE, REPORT AND CITE IT," on
  the temporal train/test-split methodology) — not separate, unrecoverable content, per
  the user's direct clarification. `models.tex:74`: "...following the procedures used
  by de Leoni \& Volpato\cite{deleonieVolpato}" — the citation addresses all three
  sub-marks together.
- **R17** (p.37, `models.tex:137`): "...uses a Graph Convolutional Netowrk (GCN)
  architecture..." — GCN is now expanded at first use (see New Issues below for the
  "Netowrk" typo this introduced).
- **R16** (p.36, `models.tex`, Table 4.2 — corrected scoping): the mark was originally
  misread as a word-fragment ("EXCEED[S...]") questioning why LoadToVehicle's
  hyperparameters diverge from Depart's. The user clarified the actual annotation
  reads "MARGIN EXCEEDED" — a pure typesetting defect, the table's natural width
  exceeded the page's text-body width (≈14.9cm), not a content question. **Fixed**
  (`models.tex:108`): the three long, non-wrapping column headers ("Order
  Management," "Logistics (Depart)," "Logistics (LoadToVehicle)") were the actual
  cause — right-aligned columns were forced to their header's full width even though
  the data in them was short (`AdamW`, `100`, etc.). Wrapped each into a two-line
  `\shortstack{}` header instead of shrinking font size or rotating to landscape.
  Verified via an isolated `\includeonly{chapters/models}` xelatex compile: zero
  "Overfull \hbox" warnings anywhere in the chapter (previously the table had no
  width-fitting technique at all), and the rendered page shows the table sitting
  fully inside the margin, matching every other table in the document.
- **R4** (p.1, `introduction.tex`), five of eight sub-marks: (a) `:6` "case
  identifier" (was "process instantiation"); (b) `:6` "are **often** generated"; (c)
  `:6` "timestamped" dropped; (d) `:6` "...interacts with other instances of the same
  or different processes \cite{galanti2023a}" — citation added; (f/g) `:9` "The type
  of process instances..." replacing "complex intricacy," and "leveraged deep learning
  models" replacing "the power of," paired with the analytics→process-monitoring
  rename (confirmed thesis-wide in this file: zero remaining standalone "analytics"
  matches).
- **R5** (p.2, `introduction.tex:13`), two of four sub-marks: (c) "However, **this
  thesis** shows a gap..." (was "my research"); (d) "In order to address this issue
  **this thesis proposes** an explainability framework..." (was "I propose") —
  resolved together, no dangling subject left behind.
- **R2** (p.v, `abstract.tex`), two of six marks: (b) `:2` "...two different
  **object-centric event logs**" (was "OCEL 2.0 datasets"); (c) `:6` all three flagged
  wordy spans gone ("explanation," "subgraphs within the universe of," "to a process").

**Settled by the edit itself, not just resolved**: R3's open "CASE CENTRIC?" question
— `preliminaries.tex:9` and `related_works.tex:19` (§2.1.1, now "...vs. case-centric
processes") were both renamed to "case-centric" terminology, so the ambiguous reading
has evidently been decided. No longer an open question.

---

## Still open (4)

- **R2a** (p.v, `abstract.tex:2`): unchanged — "...stakeholders are hesitant to trust
  models they do not properly understand" still pairs with the earlier
  transparency/lack-of-trust clause in the same sentence, the redundancy the
  "REPETITION" mark flagged was never trimmed. *(Note: R4h, the illegible ~5-letter
  word originally catalogued under `introduction.tex`, was mis-scoped — that sentence
  only exists here in `abstract.tex`, not in `introduction.tex`. Folding it into this
  item: still unresolved, still needs the user's own re-read of PDF p.1/introduction
  opening for the illegible word before acting further.)*
- **R4e** (p.1, `introduction.tex:7`): unchanged — "...single-case-ID logs that force
  an artificial **single-object view** \cite{galanti2023a}" — the "object" →
  "single-type view" swap was never applied.
- **R4i** (p.1, `introduction.tex:9`): unchanged — "...can successfully be applied to
  OCELs they require the object-centric event data..." — still no comma before "they
  require."
- **R5a/R5b** (p.2, `introduction.tex:13`): both citations still missing — no
  `\cite{}` near "...Shapley Value analysis..." / "...Natural Language Processing..."
  (likely `lundberg2017`), and none near "...traditional (non-object-centric)
  processes" (likely `vanderaalst2012` or `deleoni2016`).

---

## New issues found during verification (12) — all fixed 2026-08-13

Same spirit as `checked_thesis_comments.md`'s self-initiated audits: found while
verifying Round 2, not something de Leoni flagged, but real. All 12 addressed in a
follow-up pass the same day they were found.

1. **`abstract.tex:2`, subject–verb error** — **Fixed**: "Object-centric processes
   **are** a paradigm which represents..." (was "is").
2. **`abstract.tex:2`, terminology regression** — **Fixed**: "**Predictive process
   monitoring** on object-centric processes" (was "Predictive monitoring," which had
   dropped "process" and gone inconsistent with the thesis-wide rename).
3. **`abstract.tex:6`, botched caret resolution** — **Fixed**: reverted the literal
   "possible stakeholders" insertion; now reads "...presented to stakeholders in the
   form of graphs and tables, which aim to make the information as approachable as
   possible to the end user" — no more duplicate "possible."
4. **`introduction.tex:9`, subject–verb disagreement** — **Fixed**: "...associations
   **poses** a significant challenge" (was "pose").
5. **`preliminaries.tex:10`, broken sentence** — **Fixed**: "The following definitions
   **are** based on the formalizations provided by Galanti et al. \cite{galanti2023a}."
6. **`preliminaries.tex:167`, wrong-direction pronoun** — **Fixed**: "...since it
   **avoids** problems such as deficiency, convergence, and divergence" (dropped "me,"
   consistent with the surrounding impersonal voice — see #12's resolution below).
7. **`preliminaries.tex:167`, dangling pronoun** — **Fixed**: "By maintaining the
   original OCEL structure intact, **the explainability layer can be provided** with a
   more complete view of the information driving the predictions, offering the end
   user a more holistic understanding of how it's being processed." (restructured out
   of the antecedent-less "it," also consistent with the impersonal voice).
8. **`preliminaries.tex:6-7`, unpropagated rename** — **Fixed**: both roadmap mentions
   now say "predictive process monitoring," matching §3.3's actual heading.
9. **`models.tex:74`, doubled-word typo** — **Fixed**: "following the procedures used
   by de Leoni \& Volpato\cite{deleonieVolpato}" (dropped the duplicate "the").
10. **`models.tex:137`, spelling typo** — **Fixed**: "Graph Convolutional **Network**
    (GCN)" (was "Netowrk").
11. **Figure-filename regression** — **Fixed**: `figures/loadtovehcile_ablation.png`
    renamed back to the correct `loadtovehicle_ablation.png` (content unchanged, `git
    status` shows zero diff against `HEAD` after the rename — confirms it's the same
    image, just mis-spelled in the working tree), and `results.tex:34`'s
    `\includegraphics` updated to match. Matches the generating script's canonical
    output name again, so a future rerun won't silently re-break the reference.
12. **Unscoped chapter-wide voice reversal** — **Fixed, thesis-wide, 2026-08-14**: asked
    the user directly rather than guessing; decision was to **keep** the
    passive/impersonal voice in `models.tex`, `preliminaries.tex`, and
    `related_works.tex` as the new intentional style. Items #6 and #7 above were
    patched to read cleanly in that voice rather than reverted to first person. At the
    time this was logged, `introduction.tex`/`results.tex`/`conclusion.tex` still used
    "I/my" in places, so the thesis was left with two different authorial voices
    across its chapters as a deliberate, accepted trade-off. **That has since been
    superseded**: `introduction.tex` and `conclusion.tex` were independently converted
    to impersonal voice outside this tracking file's own edits (both show as
    externally modified in this session), and a full-thesis voice audit
    (2026-08-14) found the resulting state to be almost entirely consistent already —
    every chapter now uses impersonal passive voice and/or "this thesis"/"this work"
    as an active grammatical subject, with genuinely zero remaining first-person
    pronouns except one editorial "we" at `results.tex:83` ("This changes if **we**
    compare the models only on the last-event prefix..."), fixed in the same pass to
    "This changes **when comparing** the models only on the last-event prefix...". The
    residual inconsistency this item originally flagged no longer exists.

---

## Open questions (0, down from 4 — R9/R3 settled by the edit itself, voice scope settled 2026-08-13, R14/R15b and R16 clarified by the user as the same day)

None remaining. All four of the original round's open questions are now settled —
the last one, R16, turned out to be a margin-overflow typesetting note rather than a
content question about LoadToVehicle's hyperparameters, and is fixed (see Resolved
above).

---

## Cross-cutting notes

- **Recurring themes** (Round 2's original framing, still useful as a reference): the
  "predictive analytics"→"predictive process monitoring" and "Datasets"→"Use Cases"
  renames are done everywhere tracked except the one unpropagated spot (issue #8); the
  "My/my"→neutral-phrasing asks (R5, R11) are both done at their two authorized spots,
  but voice was changed far more broadly than that (issue #12); missing citations
  (R4d done, R5a/R5b/R8 — R8 done, R4d done, R5a/R5b still open); undefined/redundant
  acronyms (R7 done, R11 done, R17 done-with-typo).
- **Relationship to Round 1**: unaffected — none of Round 2's 17 pages overlap Round
  1's 23 comments, and nothing found during this verification pass touches Round 1's
  already-resolved items or reopens any of them.
- **Verification methodology**: three parallel read-only passes (one per
  abstract+introduction, related_works+preliminaries, models.tex), each running
  `git diff HEAD` against the relevant files and reading full current content, cross-
  checked against this file's original R1-R17 catalogue. A subset of citations (the
  §2.5 deletion, the two new typos, the abstract's subject-verb error, the figure
  rename) were independently re-confirmed via direct grep/read after the sub-passes
  reported, before finalizing this rewrite.
- **Verbatim quoting**: unlike Round 1 (machine-extracted annotation text), this
  round's original mark "quotes" are best-effort visual transcriptions of handwritten
  ink — treat as paraphrase-grade except where noted high-confidence. The "Resolved"/
  "Still open"/"New issues" sections above, by contrast, quote the current `.tex`
  source directly and are verbatim.
