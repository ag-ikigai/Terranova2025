# tools/validate_m8a.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("outputs", help="Path to outputs dir (e.g., .\\outputs)")
    ap.add_argument("--input-pack", help="Path to InputPack XLSX for 65ha check", default=None)
    ap.add_argument("--buffer", type=float, default=0.0, help="Min closing cash buffer in NAD '000")
    ap.add_argument("--strict", action="store_true", help="Strict mode")
    args = ap.parse_args()

    # Run the in-proc function to generate artifacts
    try:
        from terra_nova.modules.m8_verifier.runner import run_m8a
    except Exception as e:
        print(f"[M8.A][FAIL] Cannot import run_m8a: {e}")
        return 2

    run_m8a(args.outputs, "NAD",
            input_pack_path=args.input_pack,
            min_cash_buffer_nad_000=args.buffer,
            strict=bool(args.strict),
            write_reports=True)

    # Read summary JSON and exit accordingly
    rep = Path(args.outputs) / "m8a_super_verifier.json"
    if not rep.exists():
        print("[M8.A][FAIL] Missing m8a_super_verifier.json")
        return 2

    data = json.loads(rep.read_text(encoding="utf-8"))
    status = data.get("overall_status", "FAIL")
    print(f"[M8.A] Overall: {status}")
    if status == "FAIL":
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
