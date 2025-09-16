== M5 – Cash Flow Statement (robust) ==
[OK] Shape: (60, 9)
[OK] Columns: ['Month_Index', 'Net_Profit_After_Tax_NAD_000', 'Depreciation_NAD_000', 'WC_Cash_Flow_NAD_000', 'Tax_Paid_NAD_000', 'Interest_Paid_NAD_000', 'Cash_Flow_from_Operations_NAD_000', 'CFI_NAD_000', 'CFF_NAD_000']
[OK] Opening cash (policy): M0:Line_Item match -> 9,250,000.00 (NAD '000)
[OK] Non‑zero flags: CFO=True, CFI=True, CFF=True

Sources:
- m2_pl: {'pl_path': 'C:\\TerraNova\\outputs\\m2_pl_schedule.parquet', 'npat_col': 'NPAT_NAD_000', 'da_col': 'Depreciation_NAD_000'}
- m2_wc: {'wc_path': 'C:\\TerraNova\\outputs\\m2_working_capital_schedule.parquet', 'nwc_cf_col': 'Cash_Flow_from_NWC_Change_NAD_000'}
- m1_capex: {'cfi_col': 'Monthly_CAPEX_NAD_000', 'sign_flipped': False, 'path': 'C:\\TerraNova\\outputs\\m1_capex_schedule.parquet'}
- m3_revolver: {'rev_path': 'C:\\TerraNova\\outputs\\m3_revolver_schedule.parquet', 'source': 'C:\\TerraNova\\outputs\\m3_revolver_schedule.parquet', 'month_col': 'Month_Index', 'draw_col': 'Revolver_Draw_NAD_000', 'repay_col': 'Revolver_Repayment_NAD_000', 'fee_col': None, 'interest_col': 'Revolver_Interest_Expense_NAD_000', 'fees_included_in_cff': False}
- m4_tax: {'tax_path': 'C:\\TerraNova\\outputs\\m4_tax_schedule.parquet', 'tax_paid_col': 'Tax_Paid_NAD_000'}
- m0_opening_cash: {'source': 'M0:Line_Item match', 'path': 'C:\\TerraNova\\outputs\\m0_opening_bs.parquet', 'policy_default_zero': False}