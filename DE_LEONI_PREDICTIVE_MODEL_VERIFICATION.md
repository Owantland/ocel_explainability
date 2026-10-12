## Addendum (2026-10-12): #19b IS resolved — this document was checking the stale `results.txt`

This entire document was written and verified against `results.txt` (the plain-text draft),
not `results.tex` (the real, compiled build source). The `.txt`/`.tex` divergence flagged in
`checked_thesis_comments.md`'s cross-cutting notes means the two had drifted apart: checking
the actual `thesis_parts/latex_template/chapters/results.tex` directly shows **all of #16-20,
including #19b's scoped PackageDelivered rewrite below, are already present almost verbatim**
(results.tex:51-53 for #16/#18, :64-65 for #17, :83 for #19b, :84 for #20, :89 for #19a's
caption fix). No further `.tex` edit was needed for any of these — this document's "NOT
resolved" verdict for #19b below is stale as of this addendum; the fix it describes already
shipped, just not via this document's own tracked path. Recompiled end-to-end (xelatex→bibtex→
xelatex×2) with zero new errors/warnings to confirm. The still-genuinely-open item from this
document is the separate PackageDelivered train/test-split reproducibility discrepancy noted
under #19, which was not investigated further here.

# Verification: do the current predictive-model changes resolve de Leoni's comments #16-20?

Read-only verification pass (plus one full graph-cache rebuild for order_management/
PackageDelivered, immediately reverted) checking whether the recent predictive-model
changes — AdamW re-promotion of `CustomerOrder_to_Depart` (committed, `000ddb0`) and
`CustomerOrder_to_LoadToVehicle` (uncommitted), plus the new k-dim GNN baseline — are
sufficient to resolve de Leoni's five Results-chapter comments
(`thesis_parts/checked_thesis_comments.md`, p.53-56). **`results.txt` is not edited by
this pass** — this document reports verdicts and ready-to-paste text only.

Headline: two of the five comments (#16, #18) are already fully resolvable with the
existing draft. One (#17) needed its cited numbers refreshed against the new AdamW
checkpoint — done, story holds. One (#20) is an independent wording fix. **One (#19)
turned out to be *not* resolved, and is worse than previously assumed**: regenerating
the last-event significance test for `PackageDelivered` (never checked before) shows
`results.txt:78`'s "the heterogeneous model performs significantly better than all the
other baselines" is false for 2 of the 4 KPIs, not just under-cited.

---

## #16 — PayOrder "overfitting" framing (`results.txt:51`)

**Anchor**: paragraph after `Fig. \ref{fig:learning_curves_om}` (`\label{fig:learning_curves_om}`).

**Verdict: resolved, ready to apply.** PayOrder is untouched by the AdamW change
(`weight_decay=0.0` in `files/models/order_management/2000/Hetero/model_params.json` —
confirmed a structural no-op). `PAYORDER_TRAIN_VAL_GAP_INVESTIGATION.md`'s numbers
(train/val/test MAE 388.0h/353.2h/421.2h; split std 219.6h/135.8h/221.6h) stand
unchanged.

**Text**: `thesis_parts/results_payorder_rewrite_draft.md` §1 "NEW" block, unmodified.

---

## #17 — Logistics/Depart curve "not stabilized" (`results.txt:60`, Fig 5.5)

**Anchor**: paragraph after `Fig. \ref{fig:learning_curves_log}` (`\label{fig:learning_curves_log}`).

**Verdict: resolved, numbers refreshed.** The draft's Depart numbers
(434.8h/435.5h/441.4h) were measured on the checkpoint that predates the AdamW
re-promotion (lr=0.005, seed=0, MAE_last=6.124h, commit `000ddb0`). Re-ran the exact
same eval-mode methodology (`diagnose_train_val_gap.py`'s pattern, via a temporary,
verified-reverted `config.yml`/graph-cache swap) against the **current** checkpoint:

| split | old (pre-AdamW) checkpoint | new (AdamW) checkpoint |
|---|---|---|
| train | 434.8h | 432.1h |
| val | 435.5h | 435.1h |
| test | 441.4h | 439.9h |

Same story: all three splits within ~8h of each other, no real generalization gap —
the draft's qualitative explanation (divergence only in the patience-10 epochs past
the saved checkpoint) still holds. Only the specific hours need swapping in.

As a bonus (never checked before — LoadToVehicle wasn't in scope of the original
investigation, but was also silently re-promoted to AdamW): LoadToVehicle shows the
same pattern — train 415.3h / val 412.0h / test 421.5h, no real gap either. Not
anchored to any of de Leoni's comments specifically (Fig 5.5 = Depart only), but
confirms the same story generalizes if the topic ever comes up for the second KPI.

**Text**: `results_payorder_rewrite_draft.md` §2 "NEW" block, with
"434.8h/435.5h/441.4h" replaced by "432.1h/435.1h/439.9h".

---

## #18 — "didn't try enough" (`results.txt` ~75, "Overall" sentence)

**Anchor**: the "Overall, both graphs show..." sentence closing the training-curves
discussion (currently `results.txt:62`).

**Verdict: resolved, ready to apply.** Confirmed by direct read: the draft's §3
"Overall" paragraph cites no checkpoint-specific numbers for either dataset (just the
qualitative conclusions), so it's unaffected by #17's number refresh above. Test 2's
finding (z-normalization scope and epoch budget both tested directly on PayOrder and
ruled out as the mechanism) is untouched by AdamW.

**Text**: `results_payorder_rewrite_draft.md` §3 "NEW" block, unmodified. (§4's optional
`conclusion.txt` limitations addition is independent and can be included or dropped.)

---

## #19 — "HGT wins" not statistically supported + "Training MAE" caption (`results.txt:78,83`)

**Anchor**: the last-event comparison paragraph (`results.txt:78`) and the depth-summary
figure's caption (`results.txt:83`, `\label{fig:depth_summary}`).

### Caption (`:83`)

**Verdict: resolved.** Source-confirmed: `mae_by_depth_summary.py` / `baselines.py`
compute the depth-binned MAE from `test_df` (`test_graphs_sg.pt`), never the training
set. The caption "Training MAE for each regression model across prefix depth" should
read **"Test MAE for each regression model across prefix depth."**

### Significance claim (`:78`)

**Verdict: NOT resolved — the claim is false for 2 of the 4 KPIs, not just uncited.**

`explainer.py`'s `compare_to_baselines()` already runs a paired Wilcoxon+t-test on the
last-event subset. All four KPIs' significance files now exist (Depart/LoadToVehicle
were already current; PayOrder's generic file was already current; **PackageDelivered's
had never been isolated under its own filename before this pass** — same
generic-filename collision risk already found and fixed for logistics
`model_comparison_summary.py`, not previously applied here). Regenerated it via a
temporary, verified-reverted `config.yml` + graph-cache swap (order_management has no
pre-built PackageDelivered graph-cache backup like logistics does, so this required a
full `graphs,compare` pipeline rebuild rather than a quick file copy) and cross-checked
all four:

| KPI | vs. HomoGNN | vs. k-dim GNN | vs. Mean | vs. GBT | HGT "wins" all 4? |
|---|---|---|---|---|---|
| Depart | p=7.4e-07 ✅ better | p=1.1e-21 ✅ better | p=2.2e-24 ✅ better | p=1.7e-15 ✅ better | **Yes, 4/4** |
| LoadToVehicle | p=8.4e-22 ✅ better | p=6.4e-11 ✅ better | p=1.0e-24 ✅ better | p=7.7e-09 ✅ better | **Yes, 4/4** |
| PayOrder | p=0.016 ✅ better | p=0.314 ⚪ tied (not sig.) | p=6.3e-09 ✅ better | p=4.1e-07 ✅ better | **No, 3/4** — ties k-dim GNN |
| PackageDelivered | p=0.478 ⚪ tied (not sig.) | p=3.0e-07 ❌ **significantly worse** | p=0.020 ❌ **significantly worse** | p=0.0055 ❌ **significantly worse** | **No, 0/4** — loses to 3, ties 1 |

(✅/❌ direction taken from each CSV's `mean_diff` sign: negative = HGT more accurate.)

PackageDelivered's own numbers: HGT MAE_last=201.66h vs. k-dim GNN=155.25h,
Mean=158.37h, GBT=166.62h, HomoGNN=202.06h (n=156 last-event test cases). HGT is the
**worst-performing model of the five** on this KPI's last-event metric.

**Caveat on PackageDelivered's reproducibility**: a second sanity check against the
pre-existing `model_comparison_PackageDelivered.csv` (generated Aug 1) shows
substantially different absolute numbers for the same HGT checkpoint (HGT
MAE_last=158.44h there vs. 201.66h in this run's regeneration, though the *ranking* —
HGT losing to k-dim GNN — holds in both). This points to `TrainTestBuilder`'s temporal
split not being fully deterministic across `graphs`-stage rebuilds for this KPI
(156 vs. an unrecorded n in the Aug 1 run) — worth a closer look separately, but not
blocking here since the qualitative conclusion (HGT does not win PackageDelivered's
last-event comparison) is stable across both runs.

**What this means for `results.txt:78`**: the blanket sentence "the heterogeneous
model performs significantly better than all the other baselines" needs to be scoped,
not just cited. A factually accurate replacement:

> This changes if we compare the models only on the last-event prefix, though not
> uniformly across all four KPIs. For "Time from CustomerOrder to Depart" and "Time
> from CustomerOrder to LoadToVehicle," the heterogeneous model is significantly more
> accurate than every baseline (Wilcoxon p < 0.001 in all eight comparisons). For
> "Time from Orders to PayOrder" it is significantly better than the homogeneous GNN,
> the mean predictor, and GBT, but statistically indistinguishable from the k-dim GNN
> baseline (p=0.31). For "Time from Orders to PackageDelivered," the heterogeneous
> model does *not* win: it is statistically tied with the homogeneous GNN (p=0.48) and
> significantly *less* accurate than the mean predictor, GBT, and the k-dim GNN
> baseline. [...continue into the existing LoadToVehicle-R²-caveat sentence, retargeted
> per #20 below.]

This is a substantive rewrite, not a citation add-on — flagging for explicit review
before it goes in, since it changes the chapter's headline claim for one KPI out of
four.

---

## #20 — "terrible" R² wording (`results.txt:78`, Fig 5.6)

**Anchor**: same paragraph as #19, the "terrible $R^2$" sentence.

**Verdict: resolved, independent of everything above.** Reconfirmed on the freshly
regenerated, post-AdamW CSV: `model_comparison_LoadToVehicle.csv`'s Mean-predictor
`R2_last = -29796.9` — the extreme value de Leoni's comment cites (Fig. 5.6's
"-29796") — is a property of the Mean predictor (not a neural net, so untouched by
AdamW/k-dim changes) against a near-constant, heavy-tailed last-event target.

**Nuance worth fixing at the same time**: the current sentence's subject is
ambiguous — "the model does show... which has a terrible $R^2$" reads as describing
**HGT's own** R², but HGT's LoadToVehicle R2_last is -30.8 (moderately negative, "tens"
of hours off a tiny-scale target, as the explanation describes), not the -29796 figure
de Leoni is pointing at (that's the Mean predictor's). The rewrite should make clear
*whose* R² is -29796 (Mean predictor's) versus HGT's own (-30.8), rather than
implying one number.

**Suggested text** (drop "terrible," fix the whose-R² ambiguity, keep the
scale-explanation which is still valid):

> The one caveat is, again, "Time from CustomerOrder to LoadToVehicle": its R² values
> are strongly negative for every model, most extremely for the mean predictor
> (R²=-29796). This is a consequence of the last-event target's true scale being tiny
> (mean 2.3 hours, standard deviation 1.8 hours — an order of magnitude smaller than
> the other Logistics KPI's 27h mean / 7h std). Since R² is calculated against this
> tiny variance, even the heterogeneous model's own comparatively modest last-event
> errors (a few hours) produce a negative R² (-30.8), and the baselines' larger errors
> produce far more extreme values. MAE remains the more informative metric here;
> despite the negative R², the model still shows a significantly lower MAE at the last
> event than the other baselines [caveat: true for Depart/LoadToVehicle, not uniformly
> — see #19's rewrite above].

---

## Adjacent items flagged, not de Leoni comments

- **LoadToVehicle's AdamW re-promotion is uncommitted** (`files/models/logistics/1000/Hetero/model_params.json`
  and `files/config.yml` both show as modified in `git status`). Worth committing
  before or alongside any of the above thesis-text edits, so the checkpoint numbers
  being cited are captured in git history — same as Depart's promotion was in `000ddb0`.
- Neither AdamW, the k-dim GNN baseline, nor either re-promotion is mentioned in
  `results.txt`/`conclusion.txt` prose yet. Out of scope for de Leoni's comments
  specifically, but a separate documentation gap worth closing at some point — the
  k-dim GNN numbers are already flowing through the cited figures/significance tests
  above even though the prose never names the baseline.
- `order_management`'s `PackageDelivered` split reproducibility (noted under #19) is
  worth a dedicated look independent of this thesis-comment thread.

## Verdict summary

| # | Comment | Status |
|---|---|---|
| 16 | PayOrder "overfitting" backwards | ✅ Resolved, apply as-drafted |
| 17 | Depart curve "not stabilized" | ✅ Resolved, numbers refreshed |
| 18 | "Didn't try enough" | ✅ Resolved, apply as-drafted |
| 19a | "Training MAE" caption | ✅ Resolved, one-word fix |
| 19b | "HGT wins" statistical support | ❌ **Not resolved** — claim is false for PackageDelivered, needs a scoped rewrite |
| 20 | "Terrible" R² wording | ✅ Resolved, wording + whose-R² fix |
