# tools/m3_run.py
#!/usr/bin/env python
import argparse, importlib, sys, json, traceback

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", dest="input", required=True)
    p.add_argument("--out", dest="out", required=True)
    args = p.parse_args()

    tried = []
    for mod_name, fn_name in [
        ("terra_nova.modules.m3_financing", "run_m3"),
        ("terra_nova.modules.m3_financing.runner", "run_m3"),
        ("terra_nova.modules.m3_financing.engine", "run"),
    ]:
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, fn_name, None)
            if fn is None:
                tried.append(f"{mod_name}.{fn_name} (missing)")
                continue
            res = fn(args.input, args.out)
            print(json.dumps({"status":"ok","entrypoint":f"{mod_name}.{fn_name}"}))
            return 0
        except Exception as e:
            tried.append(f"{mod_name}.{fn_name}: {type(e).__name__}: {e}")
            print(traceback.format_exc(), file=sys.stderr)

    print("No working run function for M3. Tried:\n" + "\n".join(tried), file=sys.stderr)
    return 2

if __name__ == "__main__":
    sys.exit(main())
