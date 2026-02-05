"""Step contract used by pipeline execution."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from trakt.core.bindings import is_const_binding
from trakt.core.context import Context

StepHandler = Callable[..., dict[str, Any]]


@dataclass(slots=True)
class Step(ABC):
    """Base class for executable pipeline steps."""

    id: str
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    supports_batch: bool = True
    supports_stream: bool = False

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
            supports_batch=_coerce_capability(
                getattr(handler, "supports_batch", True),
                field_name="supports_batch",
            ),
            supports_stream=_coerce_capability(
                getattr(handler, "supports_stream", False),
                field_name="supports_stream",
            ),
        )
        step.validate_bindings()
        step.inputs = step._resolve_bound_inputs()
        step.outputs = step._resolve_bound_outputs()
        return step

    def validate_bindings(self) -> None:
        overlapping_names = sorted(set(self.declared_inputs) & set(self.declared_outputs))
        if overlapping_names:
            raise StepBindingError(
                f"Step '{self.id}' cannot declare the same name as input and output: "
                + ", ".join(overlapping_names)
            )

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

        for output_key, output_binding in self.output_bindings().items():
            _coerce_output_binding_values(output_binding, step_id=self.id, key=output_key)

        for input_key, input_binding in self.input_bindings().items():
            _collect_input_artifact_refs(input_binding, step_id=self.id, key=input_key)

    def run(self, ctx: Context, **kwargs: Any) -> dict[str, Any]:
        return self.handler(ctx, **kwargs)

    def input_bindings(self) -> dict[str, Any]:
        if self.declared_inputs:
            return {
                name: self.bindings[name]
                for name in self.declared_inputs
                if name in self.bindings
            }
        output_keys = self._output_binding_keys()
        return {key: value for key, value in self.bindings.items() if key not in output_keys}

    def output_bindings(self) -> dict[str, Any]:
        if self.declared_outputs:
            return {
                name: self.bindings[name]
                for name in self.declared_outputs
                if name in self.bindings
            }

        bindings: dict[str, Any] = {}
        if "output" in self.bindings:
            bindings["output"] = self.bindings["output"]
        if "outputs" in self.bindings:
            bindings["outputs"] = self.bindings["outputs"]
        return bindings

    def _resolve_bound_inputs(self) -> list[str]:
        names: list[str] = []
        for key, value in self.input_bindings().items():
            names.extend(_collect_input_artifact_refs(value, step_id=self.id, key=key))
        return list(dict.fromkeys(names))

    def _resolve_bound_outputs(self) -> list[str]:
        names: list[str] = []
        for key, value in self.output_bindings().items():
            names.extend(_coerce_output_binding_values(value, step_id=self.id, key=key))
        return list(dict.fromkeys(names))

    def _output_binding_keys(self) -> set[str]:
        if self.declared_outputs:
            return set(self.declared_outputs)

        keys: set[str] = set()
        if "output" in self.bindings:
            keys.add("output")
        if "outputs" in self.bindings:
            keys.add("outputs")
        return keys


def _coerce_output_binding_values(value: Any, *, step_id: str, key: str) -> list[str]:
    if is_const_binding(value):
        raise StepBindingError(
            f"Step '{step_id}' output binding '{key}' cannot use const literals."
        )
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
        f"Step '{step_id}' output binding '{key}' must be a string, list, or mapping."
    )


def _collect_input_artifact_refs(value: Any, *, step_id: str, key: str) -> list[str]:
    if is_const_binding(value):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (bool, int, float)) or value is None:
        return []
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            names.extend(_collect_input_artifact_refs(item, step_id=step_id, key=key))
        return names
    if isinstance(value, tuple):
        names: list[str] = []
        for item in value:
            names.extend(_collect_input_artifact_refs(item, step_id=step_id, key=key))
        return names
    if isinstance(value, Mapping):
        names: list[str] = []
        for item in value.values():
            names.extend(_collect_input_artifact_refs(item, step_id=step_id, key=key))
        return names

    raise StepBindingError(
        f"Step '{step_id}' input binding '{key}' has unsupported type "
        f"{type(value).__name__}; expected artifact refs, const literals, or primitive values."
    )


def _coerce_capability(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise ValueError(
        f"Step capability '{field_name}' must be a bool or boolean-like string."
    )
