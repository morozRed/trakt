"""AWS Glue entrypoint for running Trakt pipelines."""

import argparse
import json
from typing import Any

from trakt.cli import parse_input_overrides, resolve_pipeline_file
from trakt.core.loader import load_pipeline_from_yaml
from trakt.core.overrides import apply_const_overrides, parse_param_overrides
from trakt.runtime.glue_runner import GlueRunner


def main(argv: list[str] | None = None) -> None:
    """Run a pipeline using Glue-compatible runtime arguments."""
    parser = argparse.ArgumentParser(description="Run a Trakt pipeline in AWS Glue.")
    parser.add_argument(
        "--pipeline",
        help="Pipeline name under pipelines/<name>/pipeline.yaml",
    )
    parser.add_argument("--pipeline-file", help="Explicit path to pipeline YAML file")
    parser.add_argument(
        "--client-id",
        required=True,
        help="Client identifier for runtime metadata and output partitioning.",
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Batch identifier for traceability and idempotency.",
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Base input location (local path or mounted S3 prefix).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output location for artifacts and manifest.",
    )
    parser.add_argument(
        "--job-name",
        default=None,
        help="Optional Glue job name override.",
    )
    parser.add_argument(
        "--pipeline-version",
        default=None,
        help="Optional pipeline version metadata value.",
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
        help="Optional explicit manifest path (default: <output-dir>/manifest.json).",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="Override a pipeline input artifact source.",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="STEP.PARAM=VALUE",
        help="Override a const binding (value parsed as YAML).",
    )
    parser.add_argument(
        "--lenient",
        action="store_true",
        help="Allow unknown keys in input/step/output definitions (default: strict).",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional explicit run identifier.",
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
        help="Enable OpenTelemetry spans for this run.",
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
    args = parser.parse_args(argv)

    pipeline_file = resolve_pipeline_file(args.pipeline, args.pipeline_file)
    overrides = parse_input_overrides(args.input)
    param_overrides = parse_param_overrides(args.param)
    pipeline = load_pipeline_from_yaml(
        pipeline_file,
        strict_unknown_keys=not args.lenient,
    )
    apply_const_overrides(pipeline, param_overrides)

    runner = GlueRunner(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        input_overrides=overrides,
        job_name=args.job_name,
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
        context_metadata={
            "client_id": args.client_id,
            "batch_id": args.batch_id,
        },
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
