== M5 – Cash Flow Statement (robust) ==
[OK] Shape: (60, 4)
[OK] Columns: ['Month_Index', 'CFO_NAD_000', 'CFI_NAD_000', 'CFF_NAD_000']
[OK] Opening cash (policy): M0:Line_Item match -> 9,250,000.00 (NAD '000)
[OK] Non‑zero flags: CFO=True, CFI=True, CFF=False

Sources:
- m2_pl: {'pl_path': 'outputs\\m2_pl_schedule.parquet', 'npat_col': 'NPAT_NAD_000', 'da_col': 'Depreciation_NAD_000'}
- m2_wc: {'wc_path': 'outputs\\m2_working_capital_schedule.parquet', 'nwc_cf_col': 'Cash_Flow_from_NWC_Change_NAD_000'}
- m1_capex: {'cfi_col': 'CAPEX_Outflow_NAD_000', 'sign_flipped': False, 'path': 'outputs\\m1_capex_schedule.parquet'}
- m3_revolver: {'rev_path': 'outputs\\m3_revolver_schedule.parquet', 'month_col': 'Month_Index', 'draw_col': 'Drawdown', 'repay_col': 'Repayment', 'fee_col': None, 'interest_col_present': True, 'fees_included_in_cff': False}
- m0_opening_cash: {'source': 'M0:Line_Item match', 'path': 'outputs\\m0_opening_bs.parquet', 'policy_default_zero': False}