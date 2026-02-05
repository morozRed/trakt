"""Built-in data quality gate step."""

from typing import Any

from trakt.core.policies import QualityGatePolicy, evaluate_quality_gates
from trakt.core.steps import step_contract


@step_contract(inputs=["input", "policy"], outputs=["output"])
def run(
    ctx: Any,
    input: Any,
    policy: QualityGatePolicy | dict[str, Any],
) -> dict[str, Any]:
    validated, metrics = evaluate_quality_gates(input, policy, ctx=ctx)
    return {
        "output": validated,
        "__metrics__": metrics,
    }
