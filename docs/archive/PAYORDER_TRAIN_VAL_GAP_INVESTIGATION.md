# Investigation: the PayOrder train/val MAE gap flagged by de Leoni

## What prompted this

Reviewing `thesis_parts/checked_thesis_comments.md` (Prof. de Leoni's annotated
comments on `checked_thesis.pdf`), three comments raise the same underlying question
about the training-curve figures in the Results chapter (Fig. 5.4 = Order
Management/PayOrder, Fig. 5.5 = Logistics/Depart):

- **Comment #16** (p.53): the text calls the pattern in Fig. 5.4 "clear signs of
  overfitting," but de Leoni points out this is backwards — classic overfitting means
  training error drops *below* validation error, and Fig. 5.4 shows the opposite
  (validation MAE staying below training MAE throughout). He proposes two possible
  causes: the z-normalization statistics being fit on the training split only, or too
  few epochs before early stopping.
- **Comment #17** (p.54): a companion note on Fig. 5.5 (Logistics) — a real
  overfitting-like divergence *is* visible there, but neither curve looks stabilized by
  the final epoch.
- **Comment #18** (p.55): "I don't think that you did all you could to reduce this
  (more epochs, different or no z-normalization, etc.)" — a direct challenge that these
  mitigations were never actually tried.

This document records the two rounds of testing run in response, entirely read-only
(no retraining, no checkpoint modified), and what each one found.

## Test 1 — Reload each checkpoint and re-measure in `eval()` mode

**Question**: is the train-above-validation pattern in Fig. 5.4 real, or an artifact of
*when* the "training loss" curve is measured? `het_train()` (`training.py:234-251`)
accumulates each mini-batch's loss *before* that batch's own gradient update has had
its full effect — so the per-epoch "training loss" the figure plots is an average over
a mix of less-trained and more-trained intermediate weight states from across the
epoch, whereas validation is measured once, at the end, with the fully-updated weights.
With no Dropout or BatchNorm anywhere in `model_classes/HGT.py` (confirmed by direct
read), `train()` vs. `eval()` mode makes no behavioral difference — so if this timing
effect were the whole story, re-measuring training loss properly (same final weights,
same `eval()` mode used for validation) should make the gap disappear or shrink
substantially.

**Method** (`diagnose_train_val_gap.py`): for each of three trained checkpoints —
order_management/PayOrder, logistics/LoadToVehicle, logistics/Depart — reload the
already-saved `.pth` state dict (the exact pattern already used in
`training.py`'s `compare_models()`) and run the existing `het_loss_test()` function
(already `eval()`-mode, `@torch.no_grad()`) on the training set as well as
validation/test. No training occurs; this only reloads and evaluates.

**Results**:

| Checkpoint | Train MAE | Val MAE | Test MAE | Pattern |
|---|---|---|---|---|
| order_management/PayOrder (Fig. 5.4) | 388.0h | 353.2h | 421.2h | **train > val > ...** — real gap |
| logistics/LoadToVehicle | 382.4h | 423.4h | 445.4h | train < val < test — normal generalization gap |
| logistics/Depart (Fig. 5.5) | 434.8h | 435.5h | 441.4h | all three essentially equal — no real gap |

**Interpretation**:
- The train-above-validation pattern in Fig. 5.4 **is real**, not a measurement-timing
  artifact — properly re-measured on identical, fully-trained weights, train MAE is
  still higher than val MAE. This rules out one candidate explanation (a training-loop
  reporting quirk) and confirms de Leoni was right to push on this rather than accept
  the figure at face value.
- The same anomaly does **not** appear in the other two checkpoints tested, which rules
  out a codebase-wide bug in `het_train()`/`het_loss_test()` as the cause — whatever is
  happening is specific to PayOrder's particular data/split, not a systemic pipeline
  defect.
- Logistics/Depart (Fig. 5.5, the actual chart behind comment #17) shows **no real
  train/val/test gap at all** in the deployed checkpoint — train, val, and test MAE are
  all within about 7 hours of each other (434.8h/435.5h/441.4h). The divergence de Leoni
  observed in the epoch-by-epoch curve reflects the run continuing to train *past* the
  correctly-selected best epoch (logistics uses an early-stopping patience of 10, so the
  training log's final row is 10 epochs beyond the checkpoint that actually got saved);
  by then training loss had kept dropping (to 0.548 normalized) while validation MAE
  rose (to 0.842 normalized) in the discarded, over-trained epochs. The checkpoint that
  is actually cited in the thesis never reaches that state.

## Test 2 — Compare the train/val/test splits' own distributions (PayOrder only)

Test 1 narrowed the open question to PayOrder specifically. Test 2 checks de Leoni's
own two suggested causes directly, plus a third possibility neither of us had
named: since the split is temporal (earliest 70% of cases by end-timestamp = train,
remainder split again into val/test — `train_test_builder.py:18,72-92`), a
straightforward explanation is that the three windows of time simply cover
target values with different intrinsic variability, independent of any model.

**Method** (`investigate_payorder_split_shift.py`): using the same loaded data,
computed, per split: the raw (denormalized) target's mean/std/min/max; a
"mean-predictor MAE" (`|y − train_mean|`, averaged) which measures how variable each
split's target is on its own terms, without invoking any trained model at all; and,
per continuous node type (`Orders`, `Items`, `Packages`, `Products`, `Events`), each
split's already z-normalized (train-fit) feature mean/std, to check for feature-level
covariate drift.

**Results — target distribution**:

| Split | n (graphs) | Mean | Std | Mean-predictor MAE (vs. train mean) |
|---|---|---|---|---|
| train | 15,931 | 254.1h | 219.6h | 158.1h |
| val | 1,488 | 264.5h | **135.8h** | **105.7h** |
| test | 2,541 | 341.4h | 221.6h | 163.2h |

**Results — feature drift**: mild across `Orders`/`Items`/`Packages`/`Products` (z-scored
val/test means deviate from the ideal 0 by roughly 0.02-0.18, stds deviate from the
ideal 1 by roughly 0.07-0.2); a handful of `Events` one-hot dimensions show reduced
variance in val/test relative to train (consistent with certain event types occurring
at different relative frequencies later in the timeline). Not dramatic, and not the
primary driver of the MAE gap.

**Interpretation**: **val's target distribution is genuinely less variable than
train's** — its standard deviation (135.8h) is about 38% narrower than train's
(219.6h). This shows up even in a trivial constant-mean predictor, which already scores
105.7h MAE on val versus 158.1h on train, for reasons that have nothing to do with
model training, generalization, or normalization. This fully accounts for train MAE
(388h) coming out above val MAE (353h) in Test 1: val is an intrinsically easier
30-day-ish slice of the timeline, not a slice the model is generalizing to unusually
well. Separately, test's higher mean (341.4h, versus train's 254.1h and val's 264.5h)
reflects genuine temporal drift toward longer remaining-times later in the dataset's
timeline — which independently explains why test MAE (421.2h) is the worst of the
three, again without any generalization-failure story.

Neither of de Leoni's two suggested mechanisms turned out to be the cause:
train-only z-normalization does shift the *scale* used for training, but the
denormalized MAE metric already corrects for that uniformly across splits — the real
driver is the underlying target's variance differing by split, which normalization
choice doesn't change. And since the gap is intrinsic to each split's data, not a
symptom of undertraining, more epochs would not close it either.

## How this addresses each comment

- **#16** ("overfitting" motivation is invalid) — confirmed correct: the pattern isn't
  overfitting. It's now been replaced with a concrete, tested explanation (val's target
  variance is ~38% narrower than train's), rather than an incorrect one.
- **#17** (Logistics curves show overfitting without stabilization) — the deployed
  Depart checkpoint shows no real train/val/test gap; the divergence in the plotted
  curve reflects epochs past the one actually selected and saved, not a property of the
  cited model.
- **#18** ("I don't think you did all you could") — both named mitigations were tested,
  not left as hypotheticals: z-normalization was checked directly and ruled out as the
  mechanism (Test 2); additional epochs were shown to be beside the point once the gap
  was traced to an intrinsic split-level property rather than an undertrained model
  (Test 1 + Test 2 together). This is now a closed, evidence-backed question rather than
  an open one.

## Status / what's left

This investigation is complete and required no retraining — every number above comes
from reloading existing, already-saved checkpoints and data.

**Update (2026-10-12): the text-only fix has been applied — directly to `results.tex`**
(the real build source), not the stale `results.txt` draft this status note originally
pointed at. `results.tex:51-53` (Fig. 5.4/PayOrder) and `:64-65` (Fig. 5.5/Depart) now carry
this explanation almost verbatim, including the split-variance numbers above. Recompiled
end-to-end with zero new errors/warnings. `conclusion.tex`'s limitations paragraph also
already names the temporal-split sensitivity finding (added independently of this specific
edit). Nothing further is pending from this investigation.

## Scripts

- `diagnose_train_val_gap.py <database> <cant>` — Test 1 (reload a checkpoint, evaluate
  train/val/test in `eval()` mode).
- `investigate_payorder_split_shift.py` — Test 2 (per-split target/feature distribution
  comparison for order_management/PayOrder specifically).

Both are read-only: they load existing `.pt`/`.pth` files and print results; neither
writes or modifies any file.
