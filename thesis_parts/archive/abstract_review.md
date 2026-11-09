# Review: `abstract.txt` — open items

First review round — no prior version of this file exists. Read `abstract.txt`
in full (3 paragraphs, properly blank-line-separated — no structural issue like
`introduction.txt`'s missing paragraph breaks), cross-checked its claims against
the established framing used elsewhere in the thesis (`dataset.txt`'s formal
counterfactual definition, `conclusion.txt`/`introduction.txt`'s framework and
experiment-scale descriptions, both already reviewed), and proofread for
grammar/wording/consistency. The abstract has no citations at all, so there were
no `\cite` keys to verify this round.

Seven items found: three grammar/wording/punctuation fixes, one minor
cross-chapter consistency note, and three confirmed-accurate/consistent checks
worth recording.

## 1. Grammar: awkward double-preposition construction

Paragraph 1: "...has led to these methodologies **to** adopt deep learning
methods..." isn't idiomatic — "has led to X to adopt Y" doesn't parse cleanly.
Should read "has led these methodologies to adopt deep learning methods" (drop
the "to" before "these"), or "has led to the adoption of deep learning methods
by these methodologies" if the original clause structure is preferred.

## 2. Word choice: "whereupon" doesn't fit

Paragraph 1: "...represent process executions as sequences of events
**whereupon** each event may be related to different objects of multiple
types..." — "whereupon" means "immediately after which" (temporal), which
doesn't fit this relational, non-temporal description. "wherein" ("in which")
is very likely the intended word.

## 3. Punctuation: stray comma

Paragraph 2: "combines both factual**,** and counterfactual explanation
methods" — the comma in this two-item "both X and Y" construction shouldn't be
there. Should read "both factual and counterfactual explanation methods."

## 4. Minor consistency: missing hyphen

Paragraph 2: "a **four component** explainability framework" is unhyphenated,
while `conclusion.txt` consistently writes "a **four-component** explainability
framework." A one-word fix for consistency across chapters.

## 5. Confirmed accurate, cross-checked: counterfactual "minimum changes" framing

Paragraph 3's "the minimum number of changes to a process that may lead to a
different prediction" matches the thesis's own established framing for the
counterfactual method exactly — `dataset.txt`:259 states the goal is "to
identify the minimum change that an input graph would require to change its
prediction," realized via the least-dissimilar candidate. Not a
mischaracterization; the abstract's "number of changes" is a shade more literal
than `dataset.txt`'s own "minimum change" phrasing, but consistent with intent,
not a new or unsupported claim.

## 6. Confirmed consistent across chapters: experiment scale

Paragraph 3's "four different regression models, trained on information from
two different OCEL 2.0 datasets" matches `conclusion.txt`/`introduction.txt`'s
identical framing (4 total models, 2 per dataset) with no drift.

## 7. Confirmed accurate: multi-candidate counterfactual framing

Paragraph 3's "identify similar subgraphs within the universe of process
executions" (plural "subgraphs") is consistent with the actual counterfactual
method's multi-candidate behavior — confirmed during the `results.txt`/
`conclusion.txt` reviews that the stakeholder is shown a ranked table of
multiple close candidates, not just the single best one. `models.txt`:166
separately describes selecting one final candidate for the detailed
side-by-side comparison, but this is not a contradiction — just two valid
granularities of the same feature (the search considers many candidates; one
is highlighted in detail).

Overall, the abstract accurately represents the thesis's scope and results —
every substantive claim checked out against the established framing in
`dataset.txt`, `conclusion.txt`, and `introduction.txt` with no drift or
overstatement. The open items are grammar, word choice, and punctuation fixes
rather than evidence of any factual problem.
