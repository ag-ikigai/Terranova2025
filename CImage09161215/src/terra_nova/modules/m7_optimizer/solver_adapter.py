from ortools.sat.python import cp_model

class M7Model:
    def __init__(self):
        self.model = cp_model.CpModel()
        self.vars = {}
    def bool_var(self, name):
        v = self.model.NewBoolVar(name); self.vars[name] = v; return v
    def int_var(self, lo, hi, name):
        v = self.model.NewIntVar(lo, hi, name); self.vars[name] = v; return v
    def add(self, ct): self.model.Add(ct)
    def maximize(self, expr): self.model.Maximize(expr)
    def solve(self, seconds=10):
        s = cp_model.CpSolver()
        s.parameters.max_time_in_seconds = float(seconds)
        status = s.Solve(self.model)
        return s, status, {k: (s.Value(v) if hasattr(v, 'Proto') else None) for k, v in self.vars.items()}
