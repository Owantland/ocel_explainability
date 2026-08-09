# Draft rewrite: PayOrder/Depart "overfitting" paragraphs (results.txt)

Addresses de Leoni's comments #16, #17, #18, #19, and #20 in `checked_thesis_comments.md`. #16-18's
investigation is complete and evidence-backed (`PAYORDER_TRAIN_VAL_GAP_INVESTIGATION.md`); #19/#20's
is complete and evidence-backed (`DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md`). This file is just
the prose rewrite, held here for review before it goes into `results.txt`/`conclusion.txt`.

Not yet applied anywhere. Once approved, the "NEW" blocks below replace the corresponding
"CURRENT" blocks in `results.txt`'s `\subsection{Model comparison versus homogeneous GNN}`
(§1-§3, currently lines ~51, ~60, ~62) and `\subsection{Model comparison against baselines}`
(§5-§6, currently lines ~78, ~83, ~88 — §5 and §6 together form one continuous replacement of
line ~78's paragraph, split across two sections only because they address two different
comments; all line numbers will drift once earlier edits land, match by content instead), and
the conclusion.txt addition (§4) is a new sentence appended to the existing limitations
paragraph.

---

## 1. Order Management / PayOrder paragraph (comment #16 + #18)

**CURRENT** (`results.txt`, the paragraph right after `Fig. \ref{fig:learning_curves_om}`):

> Fig. \ref{fig:learning_curves_om} shows the learning curves for the best performing regression
> model on the Order Management dataset, which is associated with the prediction of the "Elapsed
> time from Orders to PayOrder" KPI. As can be seen in the curves, even after performing a
> hyperparameter sweep the model cannot be improved below a 0.45 MAE. It's notable that even if
> the model is able to reduce the training loss each epoch, it's not able to properly generalize
> enough to improve on its validation score (showing clear signs of overfitting), resulting in the
> training being halted after the early-stop trigger is activated. It's worth noting that even
> though the homogeneous implementation is allowed to train for longer it shows the same overall
> pattern as the heterogeneous model, with a decrease in training error not equating to a decrease
> in validation error, instead actually increasing after finding its local minima at epoch 4 until
> it's also cut off by the early-stop trigger.

**NEW:**

> Fig. \ref{fig:learning_curves_om} shows the learning curves for the best performing regression
> model on the Order Management dataset, which is associated with the prediction of the "Elapsed
> time from Orders to PayOrder" KPI. As can be seen in the curves, even after performing a
> hyperparameter sweep the model cannot be improved below a 0.45 MAE. Notably, training loss stays
> above validation loss for the entire run rather than the reverse, which at first glance suggests
> overfitting; however, re-evaluating the fully-trained checkpoint directly on each split (rather
> than reading the pattern off the epoch-by-epoch curve) shows train MAE (388.0 hours) is
> genuinely higher than validation MAE (353.2 hours), with test MAE the worst of the three (421.2
> hours). Comparing each split's target distribution directly, independent of any trained model,
> explains this: the validation window's remaining-time values are intrinsically less variable
> than the training window's (standard deviation of 135.8 hours versus 219.6 hours), so even a
> constant mean-predictor already scores better on validation (105.7 hours MAE) than on training
> (158.1 hours MAE) for the same reason. Test's higher error similarly reflects genuine temporal
> drift toward longer remaining times later in the dataset's timeline, not a generalization
> failure. Two candidate explanations for the pattern were checked directly and ruled out: the gap
> is not caused by the z-normalization statistics being fit only on the training split, since the
> denormalized MAE metric already corrects for this uniformly across splits, and it is not caused
> by too few training epochs, since the gap traces to an intrinsic property of each split's data
> rather than an undertrained model. Training was still halted once the early-stop trigger
> activated after validation MAE stopped improving. The homogeneous implementation, which is
> allowed to train for longer, shows a similar overall pattern, with a decrease in training error
> not producing a decrease in validation error, instead increasing again after finding its local
> minimum at epoch 4 until it's also cut off by the early-stop trigger. Its smoother-looking curve
> does not translate into a clear performance advantage over the heterogeneous model for this KPI:
> HomoGNN's all-prefix MAE is slightly lower (157.5 hours versus 167.1 hours), but its last-event
> MAE remains higher than the heterogeneous model's (103.8 hours versus 98.0 hours) — see the full
> baseline comparison later in this chapter for the complete picture across both metrics.

**Numbers used, source:**
- Train/val/test MAE (388.0h/353.2h/421.2h) and split std (219.6h/135.8h/221.6h) and
  mean-predictor MAE (158.1h/105.7h/163.2h): `PAYORDER_TRAIN_VAL_GAP_INVESTIGATION.md`, Test 1 + 2.
- All-prefix/last-event MAE (157.5h/167.1h, 103.8h/98.0h): `combined_baseline_comparison.csv`
  (regenerated this session, matches the current `combined_baseline_comparison_table.png`).

**Status check (2026-08-08)**: re-verified against `DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md` —
PayOrder/PackageDelivered's checkpoints are confirmed untouched by the AdamW change
(`weight_decay=0.0`, a structural no-op), so none of this section's numbers need updating.

**Answers de Leoni's direct question** ("are you saying homogeneous graphs are better here?")
honestly rather than with a flat yes/no: HomoGNN's train/val curve looks more conventional and
its all-prefix MAE is marginally better, but HGT still wins on last-event MAE — both facts stated
explicitly instead of left implicit in the graph.

---

## 2. Logistics / Depart paragraph (comment #17)

**CURRENT:**

> Fig. \ref{fig:learning_curves_log} shows the learning curves for the best performing regression
> model on the Logistics dataset, which is associated with the prediction of the "Elapsed time
> from CustomerOrder to Depart". As can be seen on the graph, for both the homogeneous and
> heterogeneous models validation loss remains high and increases over time even as training loss
> is reduced. For this dataset the two models track each other closely, unlike what's shown for
> the order management dataset.

**NEW:**

> Fig. \ref{fig:learning_curves_log} shows the learning curves for the best performing regression
> model on the Logistics dataset, which is associated with the prediction of the "Elapsed time
> from CustomerOrder to Depart". Both the homogeneous and heterogeneous models show a genuine
> overfitting-like divergence toward the end of the plotted run, with validation loss rising as
> training loss keeps dropping, and neither curve has visibly stabilized by the final epoch shown.
> This divergence, however, occurs entirely after the epoch that early stopping actually selected:
> Logistics uses a patience of 10 epochs, so the plotted curve necessarily extends 10 epochs
> beyond the checkpoint that was saved and is cited elsewhere in this thesis, and it is in those
> extra, discarded epochs that training loss continues falling while validation loss rises.
> Reloading the actually-deployed checkpoint and re-evaluating it directly confirms it does not
> carry this gap: train, validation and test MAE are all within about 8 hours of each other
> (432.1h/435.1h/439.9h). For this dataset the two models track each other closely throughout,
> unlike what's shown for the order management dataset.

**Numbers used, source:** `DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md`'s #17 re-check (train
432.1h / val 435.1h / test 439.9h) and its interpretation of the patience-10 discarded-epochs
mechanism, originally established in `PAYORDER_TRAIN_VAL_GAP_INVESTIGATION.md`, Test 1.

**Updated 2026-08-08**: the original numbers here (434.8h/435.5h/441.4h) were measured on the
Depart checkpoint that predates the AdamW re-promotion (commit `000ddb0`, MAE_last=6.124h). Re-ran
the identical eval-mode methodology against the current checkpoint (via a temporary,
verified-reverted `config.yml`/graph-cache swap) — the story is unchanged, only the specific hours
moved slightly, now updated above.

**Directly answers comment #17**: yes, the curve does show a real divergence — but only in epochs
past the one actually deployed, not a property of the cited model.

---

## 3. "Overall" summary sentence (comment #18, closing the thread)

**CURRENT:**

> Overall, both graphs show the difficulty the chosen model architectures have on adapting to the
> provided datasets, resulting in overfitting even when applying the best possible
> hyperparameters.

**NEW:**

> Overall, neither graph reflects classic overfitting once the pattern behind it is accounted for:
> Order Management's apparent gap is a property of its temporal validation split rather than a
> generalization failure, and Logistics' apparent divergence occurs only in training epochs beyond
> the one actually deployed. What both datasets do share is a training process that plateaus
> early, with the best available hyperparameters still leaving both model architectures at a
> relatively high absolute MAE floor for their respective KPIs.

**Status check (2026-08-08)**: this paragraph cites no checkpoint-specific numbers for either
dataset — only the qualitative conclusions — so it's unaffected by §2's number refresh above.
Test 2's z-normalization/epoch-budget findings are PayOrder-specific and untouched by AdamW.

---

## 4. Optional: conclusion.txt limitations addition (comment #18's framing)

De Leoni's comment #18 suggests noting, in the limitations section, that this per-split variance
sensitivity is itself a property of small temporal splits worth flagging for future work. Proposed
as a new sentence appended to the existing limitations paragraph (`conclusion.txt` line 18, after
"...limits its possible applications as process executions grow in size and complexity."):

> A further limitation observed during evaluation concerns the sensitivity of small temporal
> train/validation/test splits to whichever particular slice of a process's timeline each split
> happens to cover: for one KPI (Order Management's "Orders to PayOrder"), the validation split's
> target values were found to be intrinsically less variable than the training split's, which on
> its own is enough to produce a training-MAE-above-validation-MAE pattern that could otherwise be
> mistaken for overfitting. Future work using a validation scheme less sensitive to which
> particular window of time it draws from -- for example rolling-origin or k-fold temporal
> validation rather than a single fixed split -- could mitigate this.

Not yet placed in `conclusion.txt`. Flagged as optional in the original plan; include only if
useful, since the main methodological correction already lives in `results.txt`.

---

## 5. Last-event significance claim + depth-summary caption (comment #19)

Addresses de Leoni's comment #19 in `checked_thesis_comments.md` (statistical support for
"HGT wins" is not demonstrated; "Training MAE" caption is likely mislabeled). Root-caused in
`DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md`: the caption is confirmed mislabeled (source reads
`test_df`, never train), and the significance claim itself is **false for 2 of the 4 KPIs**, not
merely uncited — PackageDelivered's own last-event significance test had never been generated
under its own filename before that verification pass (same generic-filename collision already
found and fixed for logistics, not previously applied to order_management).

**CURRENT** (`results.txt:83`, the depth-summary figure's caption):

> \caption{Training MAE for each regression model across prefix depth.}

**NEW:**

> \caption{Test MAE for each regression model across prefix depth.}

---

**CURRENT** (`results.txt:78`, first three sentences of the last-event paragraph):

> This changes if we compare the models only on the last-event prefix. The ambivalence present
> in the past analysis regarding the graph-based models and their accuracy disappears. When
> provided with all possible information about a process execution the heterogeneous model
> performs significantly better than all the other baselines. The one caveat is, again, "Time
> from CustomerOrder to LoadToVehicle" KPI which has a terrible $R^2$. [...]

**NEW** (only these two sentences change; the R² sentence that follows is comment #20's separate
fix, given its own section below in §6 — kept here only with its transition word adjusted, since
"the one caveat" is no longer accurate once PayOrder/PackageDelivered's caveats are stated):

> This changes if we compare the models only on the last-event prefix, though not as uniformly as
> it may first appear. For two of the four KPIs — "Time from CustomerOrder to Depart" and "Time
> from CustomerOrder to LoadToVehicle" — the heterogeneous model is significantly more accurate
> than every baseline (Wilcoxon signed-rank p < 0.001 against each of the three baselines and the
> k-dim GNN comparison introduced in Section \ref{sec:models_baselines}). For "Time from Orders to
> PayOrder" it is significantly better than the homogeneous GNN, the mean predictor, and GBT, but
> statistically indistinguishable from the k-dim GNN baseline (p=0.31). For "Time from Orders to
> PackageDelivered," the heterogeneous model does not win at all: it is statistically tied with the
> homogeneous GNN (p=0.48) and significantly *less* accurate than the mean predictor, GBT, and the
> k-dim GNN baseline. One further caveat applies on top of this, for "Time from CustomerOrder to
> LoadToVehicle" KPI, which has a strongly negative $R^2$. [...]

---

**CURRENT** (`results.txt:88`, closing sentence after the depth-summary figure):

> This result gets flipped when comparing the models exclusively on last-event prefixes indicating
> that aggregate metrics, which are dominated by non-last-event prefixes, and last-event-specific
> metrics can move independently, sometimes in opposite directions.

**NEW:**

> This result reverses for two of the four KPIs when comparing the models exclusively on
> last-event prefixes (see the statistical breakdown above), indicating that aggregate metrics,
> which are dominated by non-last-event prefixes, and last-event-specific metrics can move
> independently, sometimes in opposite directions — though, as that breakdown shows, this
> reversal is itself KPI-dependent rather than universal.

**Numbers used, source:** all four KPIs' paired Wilcoxon/t-test p-values and MAE_last values,
from `files/explainer_outputs/logistics/validation_1000/baseline_significance_{Depart,LoadToVehicle}.csv`
and `files/explainer_outputs/order_management/validation_2000/baseline_significance.csv`
(PayOrder, resting-default filename) and `baseline_significance_PackageDelivered.csv`
(regenerated via a temporary, verified-reverted graph-cache/config.yml swap — see
`DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md` for the full per-KPI breakdown and caveats,
including a noted split-reproducibility question for PackageDelivered that doesn't change the
qualitative conclusion).

**Continues directly into §6 below**: the R² sentence following the "NEW" `:78` block above is
comment #20's fix — §6 picks up exactly where this one's "[...]" leaves off.

---

## 6. "Terrible" R² wording (comment #20)

Addresses de Leoni's comment #20 (extreme negative R² values shouldn't be called "terrible";
questions whether to report them at all). Root-caused in `DE_LEONI_PREDICTIVE_MODEL_VERIFICATION.md`:
the extreme value de Leoni cites (Fig. 5.6's "-29796") belongs to the **mean predictor**, not
the heterogeneous model — a property of a near-constant baseline against a heavy-tailed
last-event target, untouched by the AdamW/k-dim-GNN changes. The current text's subject is
ambiguous and reads as if HGT's own R² is the "terrible" one, when HGT's own LoadToVehicle
R2_last is -30.8 — moderately negative for the reason the paragraph already explains, not the
-29796 figure. This section replaces §5's tail directly; assemble by concatenating §5's `:78`
"NEW" block with this one's "NEW" block (the "[...]" in each marks the join).

**CURRENT** (`results.txt:78`, remainder of the last-event paragraph, following the sentences
already replaced in §5):

> The one caveat is, again, "Time from CustomerOrder to LoadToVehicle" KPI which has a terrible
> $R^2$. This is due to the last-event target's true scale being tiny, with a mean of 2.3 hours and
> a standard deviation of only 1.8 hours (This is an order of magnitude smaller the other Logistics
> KPI which has a mean of 27 hours and a standard deviation of 7 hours). Given that normal errors
> for this model are in the tens-of-hours range the calculated $R^2$ against such a small
> denominator produces an enormous negative value. Nevertheless, the model does show a
> significantly lower MAE at the last event when compared to the other baselines, which falls in
> line with the other three models.

**NEW:**

> One further caveat applies on top of the breakdown above, for "Time from CustomerOrder to
> LoadToVehicle" KPI, which has a strongly negative $R^2$ for every model, most extremely for the
> mean predictor (R²=-29796). This is a consequence of the last-event target's true scale being
> tiny, with a mean of 2.3 hours and a standard deviation of only 1.8 hours (an order of magnitude
> smaller than the other Logistics KPI, which has a mean of 27 hours and a standard deviation of 7
> hours). Since R² is calculated against this tiny variance, even the heterogeneous model's own
> comparatively modest last-event error produces a negative R² (-30.8) — smaller in magnitude than
> the baselines' because its MAE is itself the lowest of the five, but still negative for the same
> structural reason. MAE remains the more informative metric for this KPI: as shown above, the
> heterogeneous model's last-event MAE is significantly lower than every baseline's here, unlike
> the mixed picture for PayOrder and PackageDelivered.

**Numbers used, source:** `model_comparison_LoadToVehicle.csv` (Mean predictor R2_last=-29796.9,
HGT R2_last=-30.8), reconfirmed on the freshly regenerated, post-AdamW CSV — the value is
unaffected by the AdamW change (Mean predictor isn't a neural net). Target-scale figures (2.3h/1.8h
mean/std vs. 27h/7h) unchanged from the original text, reconfirmed accurate.

**Directly answers comment #20**: drops "terrible," clarifies whose R² is -29796 (mean predictor's,
not HGT's), and keeps the target-scale explanation since it's still accurate — de Leoni's implicit
question of "should this even be reported" is answered by pointing back to MAE as the metric that
actually supports the model comparison here.

---

## Not touched by this draft

- `conclusion.txt` line 9's "providing a statistically-meaningful difference in only 1 of the 4
  chosen KPIs" already matches the corrected figure the earlier catch-up briefing flagged as
  needing a fix (previously assumed to still say "half of the tested KPIs") — appears already
  resolved separately from this draft; not re-verified against the underlying significance tests
  in this pass, since it's outside today's PayOrder-specific scope.
- Comments #21/#23 — separate, not addressed here.
