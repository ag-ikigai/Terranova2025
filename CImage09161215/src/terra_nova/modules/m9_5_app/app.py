# src/terra_nova/modules/m9_5_app/app.py
from __future__ import annotations
import json
from pathlib import Path
import base64

import pandas as pd
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt

# -----------------------
# Config / paths
# -----------------------
st.set_page_config(page_title="Terra Nova — M9", layout="wide")

def _load_manifest(outputs: Path) -> dict:
    p = outputs / "m9_manifest.json"
    if not p.exists():
        st.warning(f"Manifest not found → {p}. Run M9.0 first.")
        return {"datasets": {}, "base_currency": "NAD"}
    return json.loads(p.read_text(encoding="utf-8"))

def _read_parquet(outputs: Path, name: str) -> pd.DataFrame | None:
    p = outputs / name
    if not p.exists(): return None
    try:
        return pd.read_parquet(p)
    except Exception as e:
        st.warning(f"Could not read {name}: {e}")
        return None

def _read_json(outputs: Path, name: str) -> dict | None:
    p = outputs / name
    if not p.exists(): return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        st.warning(f"Could not read {name}: {e}")
        return None

def _logo_base64(logo_path: Path) -> str | None:
    try:
        b = logo_path.read_bytes()
        return base64.b64encode(b).decode("ascii")
    except Exception:
        return None

# -----------------------
# UI
# -----------------------
def main():
    # CLI arg: --outputs path ; or default
    import argparse, sys
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--outputs", default="./outputs")
    parser.add_argument("--base_currency", default="NAD")
    args, _ = parser.parse_known_args()

    outputs = Path(args.outputs)
    st.sidebar.write(f"**Outputs:** `{outputs}`")
    mani = _load_manifest(outputs)
    base_ccy = mani.get("base_currency", args.base_currency)

    # Branding
    logo_p = Path(mani.get("assets", {}).get("logo_path", ""))
    if not logo_p.is_absolute():
        # relative to repo root if possible, else to this file
        repo_root = Path(__file__).resolve().parents[3]
        logo_p = (repo_root / logo_p) if (repo_root / logo_p).exists() else Path(__file__).resolve().parent / "assets" / "logo.jpg"
    b64 = _logo_base64(logo_p)
    if b64:
        st.markdown(f"<img src='data:image/jpg;base64,{b64}' style='height:72px;margin-bottom:8px;'>", unsafe_allow_html=True)
    st.title("Terra Nova — Module 9 (Presenter)")

    # Load datasets we may use across tabs
    pl   = _read_parquet(outputs, "m7_5b_profit_and_loss.parquet")
    bs   = _read_parquet(outputs, "m7_5b_balance_sheet.parquet")
    cf   = _read_parquet(outputs, "m7_5b_cash_flow.parquet")
    ifrs = _read_parquet(outputs, "m8b_ifrs_statements.parquet")

    promo_m = _read_parquet(outputs, "m8b2_promoter_scorecard_monthly.parquet")
    promo_y = _read_parquet(outputs, "m8b2_promoter_scorecard_yearly.parquet")
    inv_sel = _read_parquet(outputs, "m8b_investor_metrics_selected.parquet")
    lend_m  = _read_parquet(outputs, "m8b4_lender_metrics_monthly.parquet")
    lend_y  = _read_parquet(outputs, "m8b4_lender_metrics_yearly.parquet")

    bench_vals = _read_parquet(outputs, "m8b_benchmarks.values.parquet")
    bench_cat  = _read_json(outputs, "m8b_benchmarks.catalog.json")

    tabs = st.tabs(["Overview", "Promoters", "Investors", "Lenders", "IFRS", "Benchmarks", "Downloads"])

    # ---------------- Overview
    with tabs[0]:
        st.subheader("Highlights")
        col1, col2, col3 = st.columns(3)
        try:
            # simple heuristics for highlights
            yr = (pl["Calendar_Year"].max() if "Calendar_Year" in pl.columns else None) if pl is not None else None
            rev_col = next((c for c in (pl.columns if pl is not None else []) if c.lower().startswith("revenue")), None)
            ebitda_col = "EBITDA_NAD_000" if (pl is not None and "EBITDA_NAD_000" in pl.columns) else None
            if pl is not None and rev_col:
                col1.metric("Latest Monthly Revenue (NAD '000)", f"{pl[rev_col].iloc[-1]:,.0f}")
            if pl is not None and ebitda_col:
                col2.metric("Latest Monthly EBITDA (NAD '000)", f"{pl[ebitda_col].iloc[-1]:,.0f}")
            if cf is not None and "Closing_Cash_NAD_000" in cf.columns:
                col3.metric("Closing Cash (NAD '000)", f"{cf['Closing_Cash_NAD_000'].iloc[-1]:,.0f}")
        except Exception:
            st.info("Highlights are shown when source columns are present.")

        st.markdown("---")
        if pl is not None and "EBITDA_NAD_000" in pl.columns and "Month_Index" in pl.columns:
            st.write("**EBITDA trend (monthly)**")
            fig, ax = plt.subplots()
            sns.lineplot(data=pl, x="Month_Index", y="EBITDA_NAD_000", ax=ax)
            ax.set_xlabel("Month")
            ax.set_ylabel("EBITDA (NAD '000)")
            st.pyplot(fig)
        else:
            st.info("EBITDA monthly trend will appear when PL has 'EBITDA_NAD_000' & 'Month_Index'.")

    # ---------------- Promoters
    with tabs[1]:
        st.subheader("Promoter Scorecard")
        if promo_y is not None:
            st.write("**Yearly averages/summaries**")
            st.dataframe(promo_y)
        else:
            st.info("Yearly promoter scorecard not found.")
        if promo_m is not None and "Month_Index" in promo_m.columns:
            st.write("**Selected monthly KPIs**")
            # plot a couple of common ones if present
            for metric in ["EBITDA_Margin", "Current_Ratio", "Operating_Expense_Ratio"]:
                if metric in promo_m.columns:
                    fig, ax = plt.subplots()
                    sns.lineplot(data=promo_m, x="Month_Index", y=metric, ax=ax)
                    ax.set_xlabel("Month")
                    ax.set_ylabel(metric)
                    st.pyplot(fig)
        else:
            st.info("Monthly promoter scorecard not found.")

    # ---------------- Investors
    with tabs[2]:
        st.subheader("Investor (selected instrument)")
        if inv_sel is not None:
            st.dataframe(inv_sel)
            # If IRR/MOIC columns present, show a bar
            irr_cols = [c for c in inv_sel.columns if "IRR" in c.upper()]
            moic_cols = [c for c in inv_sel.columns if "MOIC" in c.upper()]
            if irr_cols:
                st.write("**IRR by gate**")
                fig, ax = plt.subplots()
                sns.barplot(data=inv_sel[irr_cols], ax=ax)
                ax.set_ylabel("IRR")
                st.pyplot(fig)
            if moic_cols:
                st.write("**MOIC by gate**")
                fig, ax = plt.subplots()
                sns.barplot(data=inv_sel[moic_cols], ax=ax)
                ax.set_ylabel("MOIC (x)")
                st.pyplot(fig)
        else:
            st.info("Investor metrics not found.")

    # ---------------- Lenders
    with tabs[3]:
        st.subheader("Lender / Bankability")
        if lend_m is not None and "Month_Index" in lend_m.columns:
            st.write("**DSCR (monthly)**")
            dscr_col = next((c for c in lend_m.columns if c.upper().startswith("DSCR")), None)
            if dscr_col:
                fig, ax = plt.subplots()
                sns.lineplot(data=lend_m, x="Month_Index", y=dscr_col, ax=ax)
                ax.axhline(1.2, ls="--"); ax.axhline(1.5, ls="--")
                ax.set_xlabel("Month"); ax.set_ylabel("DSCR")
                st.pyplot(fig)
        else:
            st.info("Monthly lender metrics not found.")
        if lend_y is not None:
            st.write("**Yearly lender metrics**")
            st.dataframe(lend_y)

    # ---------------- IFRS
    with tabs[4]:
        st.subheader("IFRS Financial Statements (NAD)")
        if ifrs is None:
            st.info("IFRS statements not found. (We can still show M7.5B raw statements below.)")
        else:
            # tolerant detection of columns
            item = next((c for c in ["Line_Item","IFRS_Line_Item","Item","Account"] if c in ifrs.columns), None)
            stmt = next((c for c in ["Statement","IFRS_Statement","Financial_Statement","FS"] if c in ifrs.columns), None)
            idx  = next((c for c in ["Month_Index","Period_Index","Index"] if c in ifrs.columns), None)
            valN = next((c for c in ["Value_NAD_000","Amount_NAD_000"] if c in ifrs.columns), None)
            if item and stmt and idx and valN:
                for name in ["Profit", "PL", "Income", "Loss"]:
                    sub = ifrs[ifrs[stmt].str.lower().str.contains(name.lower())]
                    if not sub.empty: 
                        st.write("**Profit & Loss (NAD '000)**")
                        st.dataframe(sub)
                        break
                for name in ["Balance", "BS"]:
                    sub = ifrs[ifrs[stmt].str.lower().str.contains(name.lower())]
                    if not sub.empty:
                        st.write("**Balance Sheet (NAD '000)**")
                        st.dataframe(sub)
                        break
                for name in ["Cash", "CF"]:
                    sub = ifrs[ifrs[stmt].str.lower().str.contains(name.lower())]
                    if not sub.empty:
                        st.write("**Cash Flow (NAD '000)**")
                        st.dataframe(sub)
                        break
            else:
                st.info("IFRS file present but lacked standard columns; showing raw preview.")
                st.dataframe(ifrs.head(100))

        st.markdown("---")
        st.subheader("Raw M7.5B statements")
        if pl is not None: st.write("**P&L**"); st.dataframe(pl)
        if bs is not None: st.write("**Balance Sheet**"); st.dataframe(bs)
        if cf is not None: st.write("**Cash Flow**"); st.dataframe(cf)

    # ---------------- Benchmarks
    with tabs[5]:
        st.subheader("Benchmarks & Thresholds")
        if bench_cat:
            st.write("**Benchmark catalog (metadata)**")
            st.json(bench_cat)
        if bench_vals is not None:
            st.write("**Benchmark values (regional/context placeholders)**")
            st.dataframe(bench_vals)

    # ---------------- Downloads
    with tabs[6]:
        st.subheader("Downloads")
        pack = outputs / "m9_pack.xlsx"
        if pack.exists():
            st.download_button("Download Excel pack (m9_pack.xlsx)", pack.read_bytes(), file_name="m9_pack.xlsx")
        else:
            st.info("Excel pack not found. Run M9.0 first.")

        # also expose CSV pack if present
        csv_folder = outputs / "m9_pack_csv"
        if csv_folder.exists():
            for p in sorted(csv_folder.glob("*.csv")):
                st.download_button(f"CSV: {p.name}", p.read_bytes(), file_name=p.name)
        else:
            st.info("CSV pack folder not found. Run M9.0 with export_csv=True.")

if __name__ == "__main__":
    main()
