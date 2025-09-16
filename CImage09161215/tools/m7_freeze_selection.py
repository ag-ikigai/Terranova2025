#!/usr/bin/env python3
import argparse, json, sys, pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Freeze a chosen investor offer for wiring (M7.5).")
    ap.add_argument("outputs_dir", help="Path to outputs (contains m7_r1_scores.parquet)")
    ap.add_argument("--option", help="Option code to freeze (e.g., A_SAFE). If omitted, picks Rank==1.")
    ap.add_argument("--scores", default="m7_r1_scores.parquet", help="Scores file name inside outputs dir")
    ap.add_argument("--out-json", default="m7_selected_offer.json", help="Frozen selection JSON filename")
    args = ap.parse_args()

    out_dir = Path(args.outputs_dir)
    scores_pq = out_dir / args.scores
    if not scores_pq.exists():
        print(f"[ERR] Scores file not found: {scores_pq}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_parquet(scores_pq)
    if args.option:
        sel = df.loc[df["Option"]==args.option]
        if sel.empty:
            print(f"[ERR] Option '{args.option}' not found in scores.", file=sys.stderr)
            sys.exit(3)
        row = sel.sort_values("Rank", ascending=True).iloc[0].to_dict()
    else:
        row = df.sort_values("Rank", ascending=True).iloc[0].to_dict()

    keep = {
        "Option": row.get("Option"),
        "Instrument": row.get("Instrument"),
        "Rank": int(row.get("Rank")),
        "Total_Score_0_100": float(row.get("Total_Score_0_100")),
        "Ticket_USD": float(row.get("Ticket_USD", 0) or 0),
        "Valuation_Cap_NAD": None if pd.isna(row.get("Valuation_Cap_NAD")) else float(row.get("Valuation_Cap_NAD")),
        "Discount_pct": None if pd.isna(row.get("Discount_pct")) else float(row.get("Discount_pct")),
        "RevShare_preRefi_pct": None if pd.isna(row.get("RevShare_preRefi_pct")) else float(row.get("RevShare_preRefi_pct")),
        "Min_IRR_Floor_pct": None if pd.isna(row.get("Min_IRR_Floor_pct")) else float(row.get("Min_IRR_Floor_pct")),
        "Exit_Refi_Multiple": None if pd.isna(row.get("Exit_Refi_Multiple")) else float(row.get("Exit_Refi_Multiple")),
    }

    out_json = out_dir / args.out_json
    out_json.write_text(json.dumps(keep, indent=2))
    print(f"[OK] Frozen selection -> {out_json}")
    print(json.dumps(keep, indent=2))

if __name__ == "__main__":
    main()
