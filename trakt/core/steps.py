"""Step contract used by pipeline execution."""

import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from difflib import get_close_matches
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


def step_contract(
    *,
    inputs: Sequence[str] | None = None,
    outputs: Sequence[str] | None = None,
    supports_batch: bool | str = True,
    supports_stream: bool | str = False,
) -> Callable[[StepHandler], StepHandler]:
    """Declare step metadata using a decorator instead of manual function attributes."""
    declared_inputs = _normalize_declared_names(inputs, field_name="inputs")
    declared_outputs = _normalize_declared_names(outputs, field_name="outputs")
    batch_capability = _coerce_capability(supports_batch, field_name="supports_batch")
    stream_capability = _coerce_capability(
        supports_stream,
        field_name="supports_stream",
    )

    def _decorate(handler: StepHandler) -> StepHandler:
        if declared_inputs:
            _validate_handler_signature(handler, declared_inputs)
        setattr(handler, "declared_inputs", list(declared_inputs))
        setattr(handler, "declared_outputs", list(declared_outputs))
        setattr(handler, "supports_batch", batch_capability)
        setattr(handler, "supports_stream", stream_capability)
        return handler

    return _decorate


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

        declared_binding_names = sorted(set(self.declared_inputs + self.declared_outputs))
        unknown_bindings: list[str] = []
        if declared_binding_names:
            declared_name_set = set(declared_binding_names)
            unknown_bindings = sorted(
                key for key in self.bindings if key not in declared_name_set
            )

        missing_inputs = [
            name for name in self.declared_inputs if name not in self.bindings
        ]
        missing_outputs = [
            name for name in self.declared_outputs if name not in self.bindings
        ]

        if missing_inputs or missing_outputs or unknown_bindings:
            details: list[str] = []
            if missing_inputs:
                details.append("missing input bindings=" + ", ".join(missing_inputs))
            if missing_outputs:
                details.append("missing output bindings=" + ", ".join(missing_outputs))
            if unknown_bindings:
                details.append(
                    "unknown bindings="
                    + ", ".join(
                        _format_binding_hint(name, candidates=declared_binding_names)
                        for name in unknown_bindings
                    )
                )
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


def _validate_handler_signature(
    handler: StepHandler, declared_inputs: list[str]
) -> None:
    """Check that declared inputs appear in the handler's function signature."""
    try:
        sig = inspect.signature(handler)
    except (ValueError, TypeError):
        return

    params = sig.parameters
    has_var_keyword = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    if has_var_keyword:
        return

    param_names = {
        name
        for name, p in params.items()
        if p.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    }
    # Remove the first parameter (ctx) from the check
    ordered = [
        name
        for name, p in params.items()
        if p.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    if ordered:
        param_names.discard(ordered[0])

    missing = sorted(name for name in declared_inputs if name not in param_names)
    if missing:
        raise StepBindingError(
            f"@step_contract declares inputs {declared_inputs} but handler "
            f"'{handler.__name__}' is missing parameters: {missing}. "
            "Either add them to the function signature or use **kwargs."
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


def _normalize_declared_names(
    names: Sequence[str] | None,
    *,
    field_name: str,
) -> list[str]:
    if names is None:
        return []
    if isinstance(names, (str, bytes)) or not isinstance(names, Sequence):
        raise TypeError(f"Step contract '{field_name}' must be a sequence of strings.")

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_name in names:
        if not isinstance(raw_name, str):
            raise TypeError(
                f"Step contract '{field_name}' must contain only strings."
            )
        name = raw_name.strip()
        if not name:
            raise ValueError(
                f"Step contract '{field_name}' cannot contain empty names."
            )
        if name in seen:
            raise ValueError(
                f"Step contract '{field_name}' cannot contain duplicate name '{name}'."
            )
        seen.add(name)
        normalized.append(name)
    return normalized


def _format_binding_hint(name: str, *, candidates: Sequence[str]) -> str:
    suggestion = _best_binding_match(name, candidates=candidates)
    if suggestion is None:
        return name
    return f"{name} (did you mean '{suggestion}'?)"


def _best_binding_match(name: str, *, candidates: Sequence[str]) -> str | None:
    matches = get_close_matches(name, list(candidates), n=1, cutoff=0.6)
    if not matches:
        return None
    return matches[0]
