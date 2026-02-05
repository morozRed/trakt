"""Normalize amount and currency fields for Glue smoke testing."""


def run(ctx, input):
    frame = input.copy()
    frame["amount"] = frame["amount"].astype(float).round(2)
    frame["currency"] = frame["currency"].str.upper()
    frame["trip_id"] = frame["trip_id"].astype(str)
    return {"output": frame}


run.declared_inputs = ["input"]
run.declared_outputs = ["output"]
