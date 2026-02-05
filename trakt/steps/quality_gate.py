"""Built-in data quality gate step."""

from typing import Any

from trakt.core.policies import QualityGatePolicy, evaluate_quality_gates


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


run.declared_inputs = ["input", "policy"]
run.declared_outputs = ["output"]
