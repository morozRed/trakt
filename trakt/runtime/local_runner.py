"""Local runtime adapter."""

from glob import glob
from pathlib import Path
from collections.abc import Iterable
from typing import Any

from trakt.core.artifacts import Artifact, OutputDataset
from trakt.core.context import Context
from trakt.core.pipeline import Pipeline
from trakt.io.adapters import ArtifactAdapterRegistry
from trakt.runtime.runner_base import RunnerBase, _count_rows


class LocalRunner(RunnerBase):
    """Execute pipelines on the local machine."""

    def __init__(
        self,
        *,
        input_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        input_overrides: dict[str, str] | None = None,
        adapter_registry: ArtifactAdapterRegistry | None = None,
        output_kind: str = "csv",
        stream_chunk_size: int = 50_000,
    ) -> None:
        self.input_dir = Path(input_dir or ".")
        self.output_dir = Path(output_dir or "outputs")
        self.input_overrides = dict(input_overrides or {})
        self.adapter_registry = (
            adapter_registry or ArtifactAdapterRegistry.from_entry_points()
        )
        self.output_kind = output_kind
        self.stream_chunk_size = stream_chunk_size

    def load_inputs(
        self, pipeline: Pipeline, ctx: Context, **kwargs: Any
    ) -> dict[str, Any]:
        input_dir = Path(kwargs.get("input_dir", self.input_dir))
        overrides = dict(self.input_overrides)
        overrides.update(kwargs.get("input_overrides", {}))

        loaded: dict[str, Any] = {}
        input_stats: dict[str, dict[str, Any]] = {}
        for input_name, artifact in pipeline.inputs.items():
            adapter = self.adapter_registry.resolve(artifact.kind)
            source = overrides.get(input_name, artifact.uri)
            paths = _resolve_input_paths(
                source,
                base_dir=input_dir,
                expected_suffix=adapter.file_extension,
            )
            if not paths:
                raise FileNotFoundError(
                    f"No input files found for '{input_name}' using source '{source}'."
                )

            chunk_size = kwargs.get("stream_chunk_size", self.stream_chunk_size)
            loaded[input_name] = adapter.read_many(
                paths,
                artifact=artifact,
                execution_mode=pipeline.execution_mode,
                chunk_size=chunk_size,
            )
            input_stats[input_name] = {
                "source": source,
                "files_read": len(paths),
                "kind": artifact.kind,
            }
            ctx.emit_event(
                "input.loaded",
                input_name=input_name,
                file_count=len(paths),
                combine_strategy=artifact.combine_strategy.value,
                artifact_kind=artifact.kind,
                execution_mode=pipeline.execution_mode,
            )
        ctx.add_metadata("input_stats", input_stats)
        return loaded

    def write_outputs(
        self, pipeline: Pipeline, artifacts: dict[str, Any], ctx: Context, **kwargs: Any
    ) -> dict[str, Any]:
        output_dir = Path(kwargs.get("output_dir", self.output_dir))
        output_dir.mkdir(parents=True, exist_ok=True)
        default_output_kind = kwargs.get("output_kind", self.output_kind)

        persisted: dict[str, Any] = {}
        for output_name, output_binding in pipeline.outputs.items():
            output_spec = _coerce_output_dataset(output_name, output_binding)
            source_name = output_spec.source
            if source_name not in artifacts:
                raise KeyError(
                    f"Pipeline output '{output_name}' references unknown artifact '{source_name}'."
                )

            output_kind = output_spec.kind or default_output_kind
            output_adapter = self.adapter_registry.resolve(output_kind)
            output_suffix = output_adapter.file_extension or ""
            target_path = _resolve_output_target_path(
                output_name=output_name,
                output_uri=output_spec.uri,
                output_dir=output_dir,
                default_suffix=output_suffix,
            )
            data = artifacts[source_name]
            output_artifact = Artifact(
                name=output_name,
                kind=output_kind,
                uri=str(target_path),
                metadata=dict(output_spec.metadata),
            )
            output_adapter.write(
                data,
                str(target_path),
                artifact_name=output_name,
                execution_mode=pipeline.execution_mode,
                artifact=output_artifact,
            )
            persisted[output_name] = {
                "path": str(target_path),
                "rows": _count_rows(data),
                "kind": output_kind,
                "source": source_name,
            }
            ctx.emit_event(
                "output.written",
                output_name=output_name,
                source_name=source_name,
                path=str(target_path),
                artifact_kind=output_kind,
                execution_mode=pipeline.execution_mode,
            )
        return persisted


def _resolve_input_paths(
    source: str, base_dir: Path, expected_suffix: str | None = None
) -> list[Path]:
    raw_specs = _split_source_specs(source)
    resolved: list[Path] = []
    seen: set[Path] = set()

    for raw_spec in raw_specs:
        paths = _expand_one_spec(
            raw_spec,
            base_dir=base_dir,
            expected_suffix=expected_suffix,
        )
        for path in paths:
            normalized = path.resolve()
            if normalized in seen:
                continue
            seen.add(normalized)
            resolved.append(normalized)
    return resolved


def _split_source_specs(source: str) -> list[str]:
    parts = [part.strip() for part in str(source).split(",")]
    return [part for part in parts if part]


def _expand_one_spec(
    spec: str, *, base_dir: Path, expected_suffix: str | None
) -> list[Path]:
    path = Path(spec)
    candidate = path if path.is_absolute() else base_dir / path
    spec_str = str(candidate)

    if _has_glob_token(spec):
        matches = [Path(match) for match in glob(spec_str, recursive=True)]
        return _filter_supported_paths(matches, expected_suffix=expected_suffix)

    if candidate.is_dir():
        return _filter_supported_paths(candidate.iterdir(), expected_suffix=expected_suffix)

    if candidate.exists():
        if expected_suffix:
            normalized_suffix = expected_suffix.lower()
            if candidate.suffix.lower() != normalized_suffix:
                raise ValueError(
                    f"Input spec '{spec}' does not match expected file extension "
                    f"'{expected_suffix}'."
                )
        return [candidate]

    return []


def _has_glob_token(text: str) -> bool:
    return any(token in text for token in ("*", "?", "["))


def _filter_supported_paths(
    paths: Iterable[Path], *, expected_suffix: str | None
) -> list[Path]:
    if not expected_suffix:
        return sorted(path for path in paths if path.is_file())

    normalized_suffix = expected_suffix.lower()
    return sorted(
        path
        for path in paths
        if path.is_file() and path.suffix.lower() == normalized_suffix
    )


def _coerce_output_dataset(
    output_name: str, output_binding: OutputDataset | str
) -> OutputDataset:
    if isinstance(output_binding, OutputDataset):
        return output_binding
    if isinstance(output_binding, str):
        return OutputDataset(name=output_name, source=output_binding)
    raise TypeError(
        f"Pipeline output '{output_name}' must be a string or OutputDataset, got "
        f"{type(output_binding).__name__}."
    )


def _resolve_output_target_path(
    *,
    output_name: str,
    output_uri: str | None,
    output_dir: Path,
    default_suffix: str,
) -> Path:
    if output_uri:
        configured = Path(output_uri)
        return configured if configured.is_absolute() else output_dir / configured
    return output_dir / f"{output_name}{default_suffix}"
