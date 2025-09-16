from ortools.sat.python import cp_model

def main():
    model = cp_model.CpModel()
    # Pick exactly one option among 3
    xA = model.NewBoolVar("A")
    xB = model.NewBoolVar("B")
    xC = model.NewBoolVar("C")
    model.Add(xA + xB + xC == 1)

    # Example scoring (maximize promoter-friendly score):
    #   score = 10*A + 8*B + 6*C  (just a toy)
    model.Maximize(10 * xA + 8 * xB + 6 * xC)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 5.0
    status = solver.Solve(model)
    print("Status:", solver.StatusName(status))
    print({v.Name(): int(solver.Value(v)) for v in [xA, xB, xC]})

if __name__ == "__main__":
    main()
