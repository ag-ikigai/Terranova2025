# M7 Weight Tuning (R1 ranking)

**Purpose.** M7.R1 ranks the investor offers in the Input Pack sheet
`Investor_500k_Offer_Grid` and writes `outputs/m7_r1_scores.parquet`.
The final choice is frozen to `outputs/m7_selected_offer.json`.

## Where the weights live

File: `src/terra_nova/modules/m7_optimizer/runner.py`

Look for:

```python
WEIGHTS = {
    "cap_score": 0.35,         # lower cap is better
    "discount_score": 0.20,    # higher discount is better
    "revshare_score": 0.15,    # lower pre-refi revshare is better
    "irr_floor_score": 0.15,   # lower IRR floor is better
    "exit_mult_score": 0.15    # lower exit multiple is better
}
