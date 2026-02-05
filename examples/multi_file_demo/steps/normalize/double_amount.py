from trakt import step_contract


@step_contract(inputs=["records"], outputs=["normalized"])
def run(ctx, records):
    frame = records.copy()
    frame["amount"] = frame["amount"] * 2
    return {"normalized": frame}
