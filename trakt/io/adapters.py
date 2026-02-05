"""Artifact adapter registry and built-in adapter implementations."""

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from trakt.core.artifacts import Artifact, combine_artifact_frames
from trakt.io.csv_reader import read_csv
from trakt.io.csv_writer import write_csv


class ArtifactAdapter(ABC):
    """Runtime adapter for reading and writing a specific artifact kind."""

    file_extension = ""

    @abstractmethod
    def read_many(self, paths: list[Path], *, artifact: Artifact) -> Any:
        """Read one or more files and materialize an in-memory payload."""

    @abstractmethod
    def write(self, data: Any, uri: str, *, artifact_name: str | None = None) -> None:
        """Persist payload to the provided URI."""


class CsvArtifactAdapter(ArtifactAdapter):
    """Built-in CSV adapter used by the local runner."""

    file_extension = ".csv"

    def read_many(self, paths: list[Path], *, artifact: Artifact) -> Any:
        read_options = _csv_read_options(artifact)
        frames = [read_csv(str(path), **read_options) for path in paths]
        return (
            frames[0]
            if len(frames) == 1
            else combine_artifact_frames(frames, artifact.combine_strategy)
        )

    def write(self, data: Any, uri: str, *, artifact_name: str | None = None) -> None:
        write_csv(data, uri)


@dataclass(slots=True)
class ArtifactAdapterRegistry:
    """Map artifact kind names to runtime adapters."""

    _adapters: dict[str, ArtifactAdapter] = field(default_factory=dict)

    def register(self, kind: str, adapter: ArtifactAdapter) -> None:
        normalized_kind = _normalize_kind(kind)
        self._adapters[normalized_kind] = adapter

    def resolve(self, kind: str) -> ArtifactAdapter:
        normalized_kind = _normalize_kind(kind)
        try:
            return self._adapters[normalized_kind]
        except KeyError as exc:
            raise KeyError(f"Unknown artifact kind: {kind}") from exc

    def load_entry_points(self, group: str = "trakt.artifact_adapters") -> None:
        discovered = metadata.entry_points()
        grouped = _group_entry_points(discovered)
        for entry_point in grouped.get(group, []):
            loaded = entry_point.load()
            adapter = _coerce_adapter(loaded, kind=entry_point.name)
            self.register(entry_point.name, adapter)

    @classmethod
    def with_defaults(cls) -> "ArtifactAdapterRegistry":
        registry = cls()
        registry.register("csv", CsvArtifactAdapter())
        return registry

    @classmethod
    def from_entry_points(
        cls, group: str = "trakt.artifact_adapters"
    ) -> "ArtifactAdapterRegistry":
        registry = cls.with_defaults()
        registry.load_entry_points(group=group)
        return registry


def _csv_read_options(artifact: Artifact) -> dict[str, Any]:
    supported_keys = {
        "delimiter",
        "encoding",
        "header",
        "date_columns",
        "decimal",
    }
    return {
        key: value
        for key, value in artifact.metadata.items()
        if key in supported_keys and value is not None
    }


def _normalize_kind(kind: str) -> str:
    normalized = kind.strip().lower()
    if not normalized:
        raise ValueError("Artifact kind cannot be empty.")
    return normalized


def _coerce_adapter(loaded: Any, *, kind: str) -> ArtifactAdapter:
    if isinstance(loaded, ArtifactAdapter):
        return loaded

    if isinstance(loaded, type) and issubclass(loaded, ArtifactAdapter):
        return loaded()

    if callable(loaded):
        materialized = loaded()
        if isinstance(materialized, ArtifactAdapter):
            return materialized

    raise TypeError(
        f"Entry point for artifact kind '{kind}' must provide an ArtifactAdapter."
    )


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
