"""Step resolution registry."""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import import_module
from importlib import metadata
from typing import Any

StepFactory = Callable[..., Any]


@dataclass(slots=True)
class StepRegistry:
    """Maps aliases to step factories."""

    _aliases: dict[str, StepFactory] = field(default_factory=dict)

    def register(self, alias: str, factory: StepFactory) -> None:
        self._aliases[alias] = factory

    def resolve(self, alias: str) -> StepFactory:
        try:
            return self._aliases[alias]
        except KeyError as exc:
            raise KeyError(f"Unknown step alias: {alias}") from exc

    def resolve_uses(self, uses: str) -> StepFactory:
        """Resolve either a registered alias or a direct module path."""
        if uses in self._aliases:
            return self._aliases[uses]
        return _load_module_step(uses)

    def load_entry_points(self, group: str = "trakt.steps") -> None:
        """Register step aliases from Python entry points."""
        discovered = metadata.entry_points()
        grouped = _group_entry_points(discovered)
        for entry_point in grouped.get(group, []):
            self.register(entry_point.name, entry_point.load())

    @classmethod
    def from_entry_points(cls, group: str = "trakt.steps") -> "StepRegistry":
        registry = cls()
        registry.load_entry_points(group=group)
        return registry


def _load_module_step(module_path: str) -> StepFactory:
    module = import_module(module_path)
    handler = getattr(module, "run", None)
    if handler is None or not callable(handler):
        raise AttributeError(
            f"Module '{module_path}' must define a callable 'run(ctx, **kwargs)'."
        )
    return handler


def _group_entry_points(
    entry_points: metadata.EntryPoints | dict[str, list[metadata.EntryPoint]],
) -> dict[str, list[metadata.EntryPoint]]:
    if hasattr(entry_points, "select"):
        grouped: dict[str, list[metadata.EntryPoint]] = defaultdict(list)
        for entry_point in entry_points:  # type: ignore[assignment]
            grouped[entry_point.group].append(entry_point)
        return dict(grouped)

    grouped = {
        group: list(entries)
        for group, entries in entry_points.items()  # type: ignore[union-attr]
    }
    return grouped
