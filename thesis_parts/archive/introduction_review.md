# Review: `introduction.txt` — open items

First review round — no prior version of this file exists. Read `introduction.txt`
in full (a short chapter: 5 content lines, each a dense single-line paragraph),
verified every citation key against `references.bib` and its usage elsewhere in
the thesis, checked cross-chapter consistency against `conclusion.txt` (which
describes the same four-component framework), and checked for formatting issues
alongside typos/grammar, matching the verification depth of every prior chapter's
first review round.

Seven items found: one structural/formatting bug (the main finding), one
incomplete stub, one grammar fix, two confirmed-accurate/consistent checks worth
recording, and two minor style/terminology notes.

## 1. Structural/formatting bug: missing paragraph breaks

`introduction.txt` doesn't use the blank-line paragraph-separation convention
that both already-reviewed chapters (`conclusion.txt`, `results.txt`) use
consistently. Instead, its five content lines (6-10) run together with no blank
lines anywhere: lines 6→7 and 9→10 have neither a blank line nor a `\\` between
them, while 7→8 and 8→9 end with a LaTeX `\\` (forced line break) instead.

In LaTeX, a bare newline with no following blank line is just whitespace — it
does not start a new paragraph. So as currently written, line 6 (PPM/OCEL
background) will run directly into line 7 (the GNN accuracy/explainability
tradeoff) with only a space between them, and line 9 (experimental validation
summary) will run directly into line 10 (the chapter roadmap) the same way. Lines
7→8 and 8→9 will at least get a forced line break, but still no paragraph
indent/spacing, since `\\` doesn't produce one. The five lines clearly intend to
be five distinct paragraphs (background → GNN tradeoff → framework overview →
experiment summary → roadmap), but as written they'll render as one long
run-on block with two mid-sentence line breaks in the middle. This is a real,
compile-affecting formatting problem, not a style nit, and it's inconsistent with
how the rest of the thesis is written — worth replacing every `\\`/bare-newline
boundary here with a proper blank line, matching `conclusion.txt`'s convention.

## 2. Incomplete stub

Line 10 ends mid-sentence: "The remainder of the document is organized as
follows. Chapter 2 --------" — an unfinished chapter-roadmap placeholder, not
yet written.

## 3. Grammar: singular/plural mismatch

Line 6: "While these graph-based **encoding** originally used homogeneous
graphs, more recent works have suggested..." — should be "encodings" (plural),
matching the surrounding "these... more recent works" construction.

## 4. Confirmed accurate: all citation keys valid and consistent

All 11 citation keys used in the chapter (`vanderaalst2012`, `adams2022` ×2,
`smit2024`, `deleonieVolpato`, `galantiDeleoni2024`, `limonroejurafsky2017`,
`castro2009`, `sundararajan2017`, `huang2022`, `stevens2022`) are valid entries in
`references.bib`, and each is used consistently with its meaning elsewhere in the
thesis. Specifically checked: `galantiDeleoni2024` (line 7's "GNNs underperform
LSTM/GBT" claim matches `related_work.txt`:41's identical framing exactly) and
`limonroejurafsky2017` (line 8's LOO citation matches `related_work.txt`:80's
identical framing). No broken or mismatched keys found.

## 5. Confirmed consistent across chapters: framework description

The "explainability framework composed of four elements" described here
(perturbation-based LOO+Shapley, feature/gradient-based, counterfactual, and a
set of fidelity metrics) matches `conclusion.txt`'s "four-component
explainability framework" description with no drift in how the framework is
characterized between the two chapters.

## 6. Minor style note

Line 9: "hopefully as more research is done on explainability frameworks for
heterogeneous graphs this limitation may soon be superseded" uses an informal
register ("hopefully") uncharacteristic of the rest of the thesis's prose. Worth
a lighter rephrase, e.g. "it is expected that further research on explainability
frameworks for heterogeneous graphs will address this limitation."

## 7. Very minor terminology note

Line 9's "two synthetic OCEL 2.0 **databases**" vs. `conclusion.txt`'s "two
different synthetic OCEL 2.0 **datasets**" — both describe the same two OCELs.
Harmless variation, not an error, but worth aligning the terminology between the
two chapters.

Overall, the chapter's content is sound and its citations check out cleanly
against the rest of the thesis — the one substantive problem is item 1's missing
paragraph breaks, which is a real rendering issue rather than a style
preference, and item 2's stub roadmap sentence is an acknowledged gap rather
than an error. The remaining items are a grammar fix and small
style/terminology polish.
