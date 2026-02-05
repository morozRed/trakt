"""Artifact adapter registry and built-in adapter implementations."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from collections import defaultdict
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from trakt.core.artifacts import (
    Artifact,
    combine_artifact_frames,
    validate_artifact_schema,
)
from trakt.io.csv_reader import read_csv
from trakt.io.csv_writer import write_csv


class ArtifactAdapter(ABC):
    """Runtime adapter for reading and writing a specific artifact kind."""

    file_extension = ""

    @abstractmethod
    def read_many(
        self,
        paths: list[Path],
        *,
        artifact: Artifact,
        execution_mode: str = "batch",
        chunk_size: int | None = None,
    ) -> Any:
        """Read one or more files and materialize an in-memory payload."""

    @abstractmethod
    def write(
        self,
        data: Any,
        uri: str,
        *,
        artifact_name: str | None = None,
        execution_mode: str = "batch",
        artifact: Artifact | None = None,
        **kwargs: Any,
    ) -> None:
        """Persist payload to the provided URI."""


class CsvArtifactAdapter(ArtifactAdapter):
    """Built-in CSV adapter used by the local runner."""

    file_extension = ".csv"

    def read_many(
        self,
        paths: list[Path],
        *,
        artifact: Artifact,
        execution_mode: str = "batch",
        chunk_size: int | None = None,
    ) -> Any:
        read_options = _csv_read_options(artifact)
        if execution_mode == "stream":
            if artifact.combine_strategy.value != "concat":
                raise ValueError(
                    "CSV stream mode currently supports combine_strategy='concat' only."
                )
            return _iter_csv_chunks(
                paths,
                read_options=read_options,
                chunk_size=chunk_size or 50_000,
                schema=artifact.schema,
                artifact_name=artifact.name,
            )

        frames = [read_csv(str(path), **read_options) for path in paths]
        if artifact.schema is not None:
            for path, frame in zip(paths, frames):
                validate_artifact_schema(
                    frame,
                    artifact.schema,
                    artifact_name=artifact.name,
                    source=str(path),
                )
        return (
            frames[0]
            if len(frames) == 1
            else combine_artifact_frames(frames, artifact.combine_strategy)
        )

    def write(
        self,
        data: Any,
        uri: str,
        *,
        artifact_name: str | None = None,
        execution_mode: str = "batch",
        artifact: Artifact | None = None,
        **kwargs: Any,
    ) -> None:
        write_options = _csv_write_options(artifact)
        if execution_mode == "stream":
            _write_csv_stream(data, uri, **write_options)
            return
        write_csv(data, uri, **write_options)


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
    metadata = dict(artifact.metadata or {})
    options = _coerce_options_mapping(
        metadata.get("read_options"),
        field_name=f"input '{artifact.name}' metadata.read_options",
    )
    supported_keys = {
        "delimiter",
        "encoding",
        "header",
        "date_columns",
        "decimal",
        "quotechar",
        "escapechar",
        "skipinitialspace",
        "dtype",
        "na_values",
        "keep_default_na",
        "delimiter_candidates",
    }
    for key in supported_keys:
        value = metadata.get(key)
        if value is not None:
            options[key] = value

    delimiter_mode = metadata.get("delimiter_mode")
    if (
        isinstance(delimiter_mode, str)
        and delimiter_mode.strip().lower() in {"auto", "sniff"}
        and "delimiter" not in options
    ):
        options["delimiter"] = "auto"
    return options


def _csv_write_options(artifact: Artifact | None) -> dict[str, Any]:
    if artifact is None:
        return {}

    metadata = dict(artifact.metadata or {})
    options = _coerce_options_mapping(
        metadata.get("write_options"),
        field_name=f"output '{artifact.name}' metadata.write_options",
    )
    supported_keys = {
        "delimiter",
        "encoding",
        "header",
        "index",
        "decimal",
        "quotechar",
        "escapechar",
        "lineterminator",
    }
    for key in supported_keys:
        value = metadata.get(key)
        if value is not None:
            options[key] = value
    return options


def _iter_csv_chunks(
    paths: list[Path],
    *,
    read_options: dict[str, Any],
    chunk_size: int,
    schema: Any | None = None,
    artifact_name: str | None = None,
) -> Iterator[Any]:
    if chunk_size <= 0:
        raise ValueError("Stream chunk_size must be a positive integer.")

    for path in paths:
        chunk_iter = read_csv(str(path), chunksize=chunk_size, **read_options)
        for chunk in chunk_iter:
            if schema is not None:
                validate_artifact_schema(
                    chunk,
                    schema,
                    artifact_name=artifact_name,
                    source=str(path),
                )
            yield chunk


def _write_csv_stream(data: Any, uri: str, **write_options: Any) -> None:
    if "mode" in write_options:
        raise ValueError(
            "CSV stream mode does not allow write_options.mode; Trakt controls chunk write mode."
        )
    if hasattr(data, "to_csv"):
        raise TypeError(
            "CSV stream writing expects an iterable of chunks, not a single DataFrame."
        )

    if not isinstance(data, Iterable) or isinstance(data, (str, bytes)):
        raise TypeError(
            "CSV stream writing expects an iterable of DataFrame-like chunks."
        )

    wrote_any_chunk = False
    initial_header = bool(write_options.pop("header", True))
    for chunk in data:
        chunk_options = dict(write_options)
        write_csv(
            chunk,
            uri,
            header=initial_header if not wrote_any_chunk else False,
            mode="w" if not wrote_any_chunk else "a",
            **chunk_options,
        )
        wrote_any_chunk = True

    if not wrote_any_chunk:
        # Persist an empty file so output contracts remain deterministic.
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_text("", encoding="utf-8")


def _coerce_options_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping when provided.")
    return dict(value)


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
