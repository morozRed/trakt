"""Shared compatibility utilities for entry point discovery."""

from collections import defaultdict
from importlib import metadata


def group_entry_points(
    entry_points: metadata.EntryPoints | dict[str, list[metadata.EntryPoint]],
) -> dict[str, list[metadata.EntryPoint]]:
    """Group entry points by their group attribute, handling both old and new APIs."""
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
