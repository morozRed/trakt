"""Step contract used by pipeline execution."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from trakt.core.context import Context

StepHandler = Callable[..., dict[str, Any]]


@dataclass(slots=True)
class Step(ABC):
    """Base class for executable pipeline steps."""

    id: str
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)

    @abstractmethod
    def run(self, ctx: Context, **kwargs: Any) -> dict[str, Any]:
        """Execute the step and return named outputs."""
        raise NotImplementedError


class StepBindingError(ValueError):
    """Raised when a step definition has invalid input/output bindings."""


@dataclass(slots=True)
class ResolvedStep(Step):
    """Concrete step resolved from YAML `uses` definitions."""

    uses: str = ""
    handler: StepHandler | None = None
    bindings: dict[str, Any] = field(default_factory=dict)
    declared_inputs: list[str] = field(default_factory=list)
    declared_outputs: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.handler is None:
            raise ValueError(f"Step '{self.id}' has no handler.")

    @classmethod
    def from_definition(
        cls,
        *,
        step_id: str,
        uses: str,
        handler: StepHandler,
        bindings: dict[str, Any] | None = None,
    ) -> "ResolvedStep":
        declared_inputs = list(getattr(handler, "declared_inputs", []))
        declared_outputs = list(getattr(handler, "declared_outputs", []))
        resolved_bindings = dict(bindings or {})

        step = cls(
            id=step_id,
            uses=uses,
            handler=handler,
            bindings=resolved_bindings,
            declared_inputs=declared_inputs,
            declared_outputs=declared_outputs,
        )
        step.validate_bindings()
        step.inputs = step._resolve_bound_inputs()
        step.outputs = step._resolve_bound_outputs()
        return step

    def validate_bindings(self) -> None:
        missing_inputs = [
            name for name in self.declared_inputs if name not in self.bindings
        ]
        missing_outputs = [
            name for name in self.declared_outputs if name not in self.bindings
        ]

        if missing_inputs or missing_outputs:
            details: list[str] = []
            if missing_inputs:
                details.append("missing input bindings=" + ", ".join(missing_inputs))
            if missing_outputs:
                details.append("missing output bindings=" + ", ".join(missing_outputs))
            raise StepBindingError(
                f"Step '{self.id}' binding error: " + "; ".join(details)
            )

    def run(self, ctx: Context, **kwargs: Any) -> dict[str, Any]:
        params = dict(self.bindings)
        params.update(kwargs)
        return self.handler(ctx, **params)

    def input_bindings(self) -> dict[str, Any]:
        if self.declared_inputs:
            return {name: self.bindings[name] for name in self.declared_inputs}

        bindings: dict[str, Any] = {}
        if "input" in self.bindings:
            bindings["input"] = self.bindings["input"]
        if "inputs" in self.bindings:
            bindings["inputs"] = self.bindings["inputs"]
        return bindings

    def output_bindings(self) -> dict[str, Any]:
        if self.declared_outputs:
            return {name: self.bindings[name] for name in self.declared_outputs}

        bindings: dict[str, Any] = {}
        if "output" in self.bindings:
            bindings["output"] = self.bindings["output"]
        if "outputs" in self.bindings:
            bindings["outputs"] = self.bindings["outputs"]
        return bindings

    def _resolve_bound_inputs(self) -> list[str]:
        if self.declared_inputs:
            return _resolve_named_bindings(
                bindings=self.bindings,
                keys=self.declared_inputs,
                step_id=self.id,
                binding_kind="input",
            )

        return _resolve_default_inputs(self.bindings, step_id=self.id)

    def _resolve_bound_outputs(self) -> list[str]:
        if self.declared_outputs:
            return _resolve_named_bindings(
                bindings=self.bindings,
                keys=self.declared_outputs,
                step_id=self.id,
                binding_kind="output",
            )

        return _resolve_default_outputs(self.bindings, step_id=self.id)


def _resolve_default_inputs(bindings: dict[str, Any], step_id: str) -> list[str]:
    names: list[str] = []
    if "input" in bindings:
        names.extend(_coerce_binding_values(bindings["input"], step_id, "input"))
    if "inputs" in bindings:
        names.extend(_coerce_binding_values(bindings["inputs"], step_id, "inputs"))
    return names


def _resolve_default_outputs(bindings: dict[str, Any], step_id: str) -> list[str]:
    names: list[str] = []
    if "output" in bindings:
        names.extend(_coerce_binding_values(bindings["output"], step_id, "output"))
    if "outputs" in bindings:
        names.extend(_coerce_binding_values(bindings["outputs"], step_id, "outputs"))
    return names


def _resolve_named_bindings(
    *,
    bindings: dict[str, Any],
    keys: list[str],
    step_id: str,
    binding_kind: str,
) -> list[str]:
    names: list[str] = []
    for key in keys:
        if key not in bindings:
            raise StepBindingError(
                f"Step '{step_id}' missing {binding_kind} binding '{key}'."
            )
        names.extend(_coerce_binding_values(bindings[key], step_id, key))
    return names


def _coerce_binding_values(value: Any, step_id: str, key: str) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        if not all(isinstance(item, str) for item in value):
            raise StepBindingError(
                f"Step '{step_id}' binding '{key}' must contain only strings."
            )
        return value
    if isinstance(value, dict):
        dict_values = list(value.values())
        if not all(isinstance(item, str) for item in dict_values):
            raise StepBindingError(
                f"Step '{step_id}' binding '{key}' must map to string values."
            )
        return dict_values

    raise StepBindingError(
        f"Step '{step_id}' binding '{key}' must be a string, list, or mapping."
    )
