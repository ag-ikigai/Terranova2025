# No-Downstream-Change Verification (M0)

**Scope**: Only the three M0 artifacts that downstream modules actually consume:
- outputs/m0_inputs/FX_Path.parquet  (Month_Index, NAD_per_USD)
- outputs/m0_calendar.parquet        (Date, Year, Month, Month_Index)
- outputs/m0_opening_bs.parquet      (Line_Item, Value_NAD, Notes)

**Why only these three?**  
M1 and M2 read InputPack v10 directly via `load_and_validate_input_pack(...)`; they do not consume the other m0_inputs/*.parquet files. M8.B1 consumes the FX lane; M7.5B reads the opening cash seed; calendar helpers are used where Month_Index mapping is required. (See engine + data-contract + dependency map.) 

**Process**
1. Run `tools/ci_m0_validate_only.ps1` → read-only equality checks vs backup (schema + numbers).
2. Run `tools/diff_m0_vs_backup.ps1` → show hash/size for the three files.
3. If any difference is flagged, run `tools/restore_m0_from_backup.ps1` to restore only those three files.

**Acceptance Gates**
- FX: Month_Index and NAD_per_USD equal to backup.
- Calendar: same dates and Month_Index; Year/Month equal.
- Opening BS: same Line_Item and Value_NAD.
- No other module files are modified.
