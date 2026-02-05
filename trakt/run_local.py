"""CLI entrypoint for local pipeline execution."""

import argparse
import json
from pathlib import Path

from trakt.core.loader import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Trakt pipeline locally.")
    parser.add_argument("--pipeline", help="Pipeline name under pipelines/<name>/pipeline.yaml")
    parser.add_argument("--pipeline-file", help="Explicit path to pipeline YAML file")
    parser.add_argument("--input-dir", default=".", help="Base directory for local input files")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory for output artifacts and manifest.",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="Override a pipeline input artifact source.",
    )
    parser.add_argument("--run-id", default=None, help="Optional explicit run id")
    parser.add_argument(
        "--pipeline-version",
        default=None,
        help="Optional pipeline version metadata value",
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
        help="Optional explicit manifest output path (default: <output-dir>/manifest.json).",
    )
    parser.add_argument(
        "--stream-chunk-size",
        type=int,
        default=50_000,
        help="Chunk size for stream execution mode (CSV adapters only).",
    )
    parser.add_argument(
        "--otel-enabled",
        action="store_true",
        help="Enable OpenTelemetry spans for the pipeline run.",
    )
    parser.add_argument(
        "--otel-service-name",
        default="trakt",
        help="OpenTelemetry service.name attribute.",
    )
    parser.add_argument(
        "--otel-tracer-name",
        default="trakt.runner",
        help="OpenTelemetry tracer name.",
    )
    args = parser.parse_args()

    pipeline_file = _resolve_pipeline_file(args.pipeline, args.pipeline_file)
    overrides = _parse_input_overrides(args.input)

    pipeline = load_pipeline_from_yaml(pipeline_file)
    runner = LocalRunner(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        input_overrides=overrides,
    )
    result = runner.run(
        pipeline,
        run_id=args.run_id,
        pipeline_version=args.pipeline_version,
        manifest_path=args.manifest_path,
        stream_chunk_size=args.stream_chunk_size,
        otel_enabled=args.otel_enabled,
        otel_service_name=args.otel_service_name,
        otel_tracer_name=args.otel_tracer_name,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def _resolve_pipeline_file(
    pipeline_name: str | None, pipeline_file: str | None
) -> Path:
    if pipeline_file:
        return Path(pipeline_file)
    if pipeline_name:
        return Path("pipelines") / pipeline_name / "pipeline.yaml"
    raise ValueError("Provide either --pipeline or --pipeline-file.")


def _parse_input_overrides(raw_overrides: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in raw_overrides:
        if "=" not in item:
            raise ValueError(f"Invalid input override '{item}'. Expected NAME=PATH.")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ValueError(f"Invalid input override '{item}'. Expected NAME=PATH.")
        parsed[key] = value
    return parsed


if __name__ == "__main__":
    main()
