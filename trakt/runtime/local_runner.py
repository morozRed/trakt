"""Local runtime adapter."""

from glob import glob
from pathlib import Path
from typing import Any

from trakt.core.artifacts import Artifact, combine_artifact_frames
from trakt.core.context import Context
from trakt.core.pipeline import Pipeline
from trakt.io.csv_reader import read_csv
from trakt.io.csv_writer import write_csv
from trakt.runtime.runner_base import RunnerBase


class LocalRunner(RunnerBase):
    """Execute pipelines on the local machine."""

    def __init__(
        self,
        *,
        input_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        input_overrides: dict[str, str] | None = None,
    ) -> None:
        self.input_dir = Path(input_dir or ".")
        self.output_dir = Path(output_dir or "outputs")
        self.input_overrides = dict(input_overrides or {})

    def load_inputs(
        self, pipeline: Pipeline, ctx: Context, **kwargs: Any
    ) -> dict[str, Any]:
        input_dir = Path(kwargs.get("input_dir", self.input_dir))
        overrides = dict(self.input_overrides)
        overrides.update(kwargs.get("input_overrides", {}))

        loaded: dict[str, Any] = {}
        input_stats: dict[str, dict[str, Any]] = {}
        for input_name, artifact in pipeline.inputs.items():
            source = overrides.get(input_name, artifact.uri)
            paths = _resolve_input_paths(source, base_dir=input_dir)
            if not paths:
                raise FileNotFoundError(
                    f"No input files found for '{input_name}' using source '{source}'."
                )

            read_options = _csv_read_options(artifact)
            frames = [read_csv(str(path), **read_options) for path in paths]
            loaded[input_name] = (
                frames[0]
                if len(frames) == 1
                else combine_artifact_frames(frames, artifact.combine_strategy)
            )
            input_stats[input_name] = {
                "source": source,
                "files_read": len(paths),
            }
            ctx.emit_event(
                "input.loaded",
                input_name=input_name,
                file_count=len(paths),
                combine_strategy=artifact.combine_strategy.value,
            )
        ctx.add_metadata("input_stats", input_stats)
        return loaded

    def write_outputs(
        self, pipeline: Pipeline, artifacts: dict[str, Any], ctx: Context, **kwargs: Any
    ) -> dict[str, Any]:
        output_dir = Path(kwargs.get("output_dir", self.output_dir))
        output_dir.mkdir(parents=True, exist_ok=True)

        persisted: dict[str, Any] = {}
        for output_name, source_name in pipeline.outputs.items():
            if source_name not in artifacts:
                raise KeyError(
                    f"Pipeline output '{output_name}' references unknown artifact '{source_name}'."
                )

            target_path = output_dir / f"{output_name}.csv"
            data = artifacts[source_name]
            write_csv(data, str(target_path))
            persisted[output_name] = {
                "path": str(target_path),
                "rows": _count_rows(data),
            }
            ctx.emit_event(
                "output.written",
                output_name=output_name,
                source_name=source_name,
                path=str(target_path),
            )
        return persisted


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


def _resolve_input_paths(source: str, base_dir: Path) -> list[Path]:
    raw_specs = _split_source_specs(source)
    resolved: list[Path] = []
    seen: set[Path] = set()

    for raw_spec in raw_specs:
        paths = _expand_one_spec(raw_spec, base_dir=base_dir)
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


def _expand_one_spec(spec: str, *, base_dir: Path) -> list[Path]:
    path = Path(spec)
    candidate = path if path.is_absolute() else base_dir / path
    spec_str = str(candidate)

    if _has_glob_token(spec):
        matches = [Path(match) for match in glob(spec_str, recursive=True)]
        return sorted(matches)

    if candidate.is_dir():
        return sorted(item for item in candidate.iterdir() if item.suffix.lower() == ".csv")

    if candidate.exists():
        return [candidate]

    return []


def _has_glob_token(text: str) -> bool:
    return any(token in text for token in ("*", "?", "["))


def _count_rows(payload: Any) -> int | None:
    try:
        return len(payload)
    except TypeError:
        return None
