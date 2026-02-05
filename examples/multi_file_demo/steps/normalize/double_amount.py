def run(ctx, records, normalized):
    frame = records.copy()
    frame["amount"] = frame["amount"] * 2
    return {"normalized": frame}


run.declared_inputs = ["records"]
run.declared_outputs = ["normalized"]
