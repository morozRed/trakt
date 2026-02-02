def run(ctx, input, output):
    frame = input.copy()
    frame["amount"] = frame["amount"] * 2
    return {"output": frame}


run.declared_inputs = ["input"]
run.declared_outputs = ["output"]
