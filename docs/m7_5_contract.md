# Module 7.5 (A) — Junior Financing Wiring (Contract v0.1)

**Artifact:** `outputs/m7_5_junior_financing.parquet`

**Required columns & roles**

| Column                      | Role / Description                                     |
|----------------------------|--------------------------------------------------------|
| `Month_Index`              | Time index (int)                                       |
| `Option`                   | Code of selected option (e.g., `A_SAFE`)               |
| `Instrument`               | Human label (e.g., `SAFE`, `Convertible Note`)         |
| `FX_USD_to_NAD`            | FX used for conversion                                 |
| `Junior_Equity_In_NAD_000` | Cash **inflow** from junior instrument, NAD '000       |

**Notes**
- This is 7.5(A) schedule only. 7.5(B) will map this into PPE/Cash/Equity in the BS.
