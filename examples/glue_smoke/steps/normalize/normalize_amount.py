"""Normalize amount and currency fields for Glue smoke testing."""

from trakt import step_contract


@step_contract(inputs=["input"], outputs=["output"])
def run(ctx, input):
    frame = input.copy()
    frame["amount"] = frame["amount"].astype(float).round(2)
    frame["currency"] = frame["currency"].str.upper()
    frame["trip_id"] = frame["trip_id"].astype(str)
    return {"output": frame}
