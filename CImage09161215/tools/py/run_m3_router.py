# tools/py/run_m3_router.py
import argparse, importlib, inspect, json, sys

def _patch_m1_alias():
    # Provide legacy alias expected by older M3 code, without changing M1 logic.
    try:
        m1 = importlib.import_module('terra_nova.modules.m1_operational_engines.engine')
        if not hasattr(m1, 'create_capex_and_depreciation_schedules'):
            def create_capex_and_depreciation_schedules(input_pack, outputs, currency='NAD'):
                cap = m1.build_capex_schedule(input_pack, outputs, currency)
                dep = m1.build_depreciation_schedule(input_pack, outputs, currency)
                return cap, dep
            setattr(m1, 'create_capex_and_depreciation_schedules', create_capex_and_depreciation_schedules)
    except Exception:
        # M1 import not needed for M3 or module path differs; do nothing.
        pass

def _resolve_run():
    candidates = [
        'terra_nova.modules.m3_financing.runner',
        'terra_nova.modules.m3_financing.engine',
        'terra_nova.modules.m3_financing',
    ]
    names = ['run_m3', 'run', 'main']
    for modname in candidates:
        try:
            m = importlib.import_module(modname)
        except Exception:
            continue
        for nm in names:
            fn = getattr(m, nm, None)
            if callable(fn):
                return fn, modname, nm
    return None, None, None

def _call_run(fn, input_xlsx, out_dir, currency):
    sig = inspect.signature(fn)
    params = sig.parameters

    # Try common kw patterns first (no logic change; we just adapt to the function signature)
    candidates = [
        {'input_xlsx': input_xlsx, 'out_dir': out_dir, 'currency': currency},
        {'input_xlsx': input_xlsx, 'out_dir': out_dir},
        {'input': input_xlsx, 'out': out_dir, 'currency': currency},
        {'input': input_xlsx, 'out': out_dir},
        {'input_pack': input_xlsx, 'outputs': out_dir, 'currency': currency},
        {'input_pack': input_xlsx, 'outputs': out_dir},
        {'input_path': input_xlsx, 'output_dir': out_dir, 'currency': currency},
        {'input_path': input_xlsx, 'output_dir': out_dir},
    ]
    for style in candidates:
        usable = {k: v for k, v in style.items() if k in params}
        try:
            return fn(**usable)
        except TypeError:
            pass

    # Finally try simple positionals
    for args in [(input_xlsx, out_dir, currency), (input_xlsx, out_dir), (out_dir,)]:
        try:
            return fn(*args)
        except TypeError:
            pass

    raise SystemExit("No compatible signature for run_m3")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--currency', default='NAD')
    args = ap.parse_args()

    _patch_m1_alias()
    fn, mod, nm = _resolve_run()
    if not fn:
        raise SystemExit("Could not find run function in m3_financing (tried runner/engine).")

    res = _call_run(fn, args.input, args.out, args.currency)
    print(json.dumps({"module": mod, "function": nm, "result": str(res)}))

if __name__ == '__main__':
    main()
