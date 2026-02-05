"""Binding helpers for step inputs/config literals."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Const:
    """Explicit literal binding wrapper for YAML/DSL parity."""

    value: Any


def const(value: Any) -> Const:
    """Mark a binding value as a literal constant."""
    return Const(value=value)


def is_const_binding(value: Any) -> bool:
    """Return True when value is an explicit constant binding."""
    return isinstance(value, Const) or (
        isinstance(value, Mapping) and len(value) == 1 and "const" in value
    )


def get_const_binding_value(value: Any) -> Any:
    """Extract literal payload from Const or YAML `{\"const\": ...}` wrapper."""
    if isinstance(value, Const):
        return value.value
    if isinstance(value, Mapping) and len(value) == 1 and "const" in value:
        return value["const"]
    raise TypeError("Expected Const or mapping with a single 'const' key.")
