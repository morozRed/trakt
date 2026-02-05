"""Helpers for runtime overrides of pipeline bindings."""

from collections.abc import Iterable
from typing import Any

import yaml

from trakt.core.bindings import Const, const, is_const_binding
from trakt.core.pipeline import Pipeline
from trakt.core.steps import ResolvedStep


def parse_param_overrides(raw_overrides: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Parse CLI-style param overrides into a step -> param -> value mapping."""
    parsed: dict[str, dict[str, Any]] = {}
    for item in raw_overrides:
        if "=" not in item:
            raise ValueError(
                f"Invalid param override '{item}'. Expected STEP_ID.PARAM=VALUE."
            )
        key, raw_value = item.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not key or raw_value == "":
            raise ValueError(
                f"Invalid param override '{item}'. Expected STEP_ID.PARAM=VALUE."
            )
        if "." not in key:
            raise ValueError(
                f"Invalid param override '{item}'. Expected STEP_ID.PARAM=VALUE."
            )
        step_id, param = key.split(".", 1)
        step_id = step_id.strip()
        param = param.strip()
        if not step_id or not param:
            raise ValueError(
                f"Invalid param override '{item}'. Expected STEP_ID.PARAM=VALUE."
            )
        parsed.setdefault(step_id, {})[param] = yaml.safe_load(raw_value)
    return parsed


def apply_const_overrides(
    pipeline: Pipeline, overrides: dict[str, dict[str, Any]]
) -> None:
    """Apply constant overrides to a pipeline in-place."""
    if not overrides:
        return

    steps_by_id = {step.id: step for step in pipeline.steps}
    for step_id, params in overrides.items():
        step = steps_by_id.get(step_id)
        if step is None:
            raise ValueError(f"Unknown step id '{step_id}' in param overrides.")
        if not isinstance(step, ResolvedStep):
            raise ValueError(
                f"Step '{step_id}' does not support const overrides (only YAML steps are supported)."
            )
        for param, value in params.items():
            if param not in step.bindings:
                raise ValueError(
                    f"Step '{step_id}' has no binding named '{param}' to override."
                )
            binding = step.bindings[param]
            if not is_const_binding(binding):
                raise ValueError(
                    f"Step '{step_id}' binding '{param}' is not a const binding."
                )
            step.bindings[param] = _wrap_const_override(binding, value)


def _wrap_const_override(binding: Any, value: Any) -> Any:
    if isinstance(binding, Const):
        return const(value)
    return {"const": value}
