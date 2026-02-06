"""Shared CLI utilities and unified command-line interface."""

import argparse
import json
import logging
import sys
import textwrap
from pathlib import Path
from typing import Any

logger = logging.getLogger("trakt.cli")


def resolve_pipeline_file(
    pipeline_name: str | None, pipeline_file: str | None
) -> Path:
    """Resolve a pipeline YAML path from either a name or explicit file path."""
    if pipeline_file:
        return Path(pipeline_file)
    if pipeline_name:
        return Path("pipelines") / pipeline_name / "pipeline.yaml"
    raise ValueError("Provide either --pipeline or --pipeline-file.")


def parse_input_overrides(raw_overrides: list[str]) -> dict[str, str]:
    """Parse NAME=PATH input override strings into a dictionary."""
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


def _resolve_pipeline_arg(args: argparse.Namespace) -> Path:
    """Resolve --pipeline from either name or file, with smart detection."""
    pipeline = getattr(args, "pipeline", None)
    pipeline_file = getattr(args, "pipeline_file", None)
    if pipeline_file:
        return Path(pipeline_file)
    if pipeline:
        if "/" in pipeline or pipeline.endswith(".yaml") or pipeline.endswith(".yml"):
            return Path(pipeline)
        return Path("pipelines") / pipeline / "pipeline.yaml"
    raise SystemExit("Error: provide --pipeline or --pipeline-file.")


def _configure_logging(args: argparse.Namespace) -> None:
    """Configure logging level based on CLI verbosity flags."""
    if getattr(args, "quiet", False):
        level = logging.WARNING
    elif getattr(args, "verbose", False):
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        stream=sys.stderr,
    )


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add verbosity flags shared across subcommands."""
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress informational output (warnings only).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show debug-level output.",
    )


def _add_pipeline_args(parser: argparse.ArgumentParser) -> None:
    """Add --pipeline and --pipeline-file arguments."""
    parser.add_argument(
        "--pipeline",
        help="Pipeline name (under pipelines/<name>/pipeline.yaml) or path.",
    )
    parser.add_argument(
        "--pipeline-file",
        help="Explicit path to pipeline YAML file.",
    )


def _cmd_run(args: argparse.Namespace) -> None:
    """Execute a pipeline locally."""
    from trakt.core.loader import PipelineLoadError, load_pipeline_from_yaml
    from trakt.core.overrides import apply_const_overrides, parse_param_overrides
    from trakt.core.pipeline import PipelineValidationError
    from trakt.runtime.local_runner import LocalRunner

    pipeline_file = _resolve_pipeline_arg(args)
    overrides = parse_input_overrides(args.input)
    param_overrides = parse_param_overrides(args.param)

    try:
        pipeline = load_pipeline_from_yaml(
            pipeline_file,
            strict_unknown_keys=not args.lenient,
        )
        apply_const_overrides(pipeline, param_overrides)
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
    except PipelineLoadError as exc:
        print(f"Error loading pipeline: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except PipelineValidationError as exc:
        print(f"Pipeline validation failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except FileNotFoundError as exc:
        print(f"File not found: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except KeyError as exc:
        print(f"Missing artifact: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        if args.verbose:
            raise
        print(
            f"Error: {type(exc).__name__}: {exc}\n"
            "Use --verbose for full traceback.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    step_count = len(result.get("steps", []))
    output_count = len(result.get("outputs", {}))
    print(
        f"Pipeline '{result.get('pipeline', '')}' completed successfully "
        f"({step_count} steps, {output_count} outputs)",
        file=sys.stderr,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def _cmd_validate(args: argparse.Namespace) -> None:
    """Validate pipeline YAML without executing."""
    from trakt.core.loader import PipelineLoadError, load_pipeline_from_yaml

    pipeline_file = _resolve_pipeline_arg(args)

    try:
        pipeline = load_pipeline_from_yaml(
            pipeline_file,
            strict_unknown_keys=not args.lenient,
        )
    except PipelineLoadError as exc:
        print(f"Validation failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except FileNotFoundError as exc:
        print(f"File not found: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        if args.verbose:
            raise
        print(f"Validation error: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)

    print(
        f"Pipeline '{pipeline.name}' is valid "
        f"({len(pipeline.inputs)} inputs, {len(pipeline.steps)} steps, "
        f"{len(pipeline.outputs)} outputs, mode={pipeline.execution_mode})",
    )


def _cmd_init(args: argparse.Namespace) -> None:
    """Scaffold a new pipeline project."""
    name = args.name
    base = Path(name)
    if base.exists():
        print(f"Error: directory '{name}' already exists.", file=sys.stderr)
        raise SystemExit(1)

    base.mkdir(parents=True)
    (base / "input").mkdir()
    (base / "steps").mkdir()
    (base / "steps" / "__init__.py").write_text("", encoding="utf-8")

    pipeline_yaml = textwrap.dedent(f"""\
        name: {name}

        inputs:
          source__records:
            uri: records/*.csv
            combine_strategy: concat

        steps:
          - id: transform
            uses: steps.transform
            with:
              input: source__records
              output: records_out

        outputs:
          datasets:
            - name: final
              from: records_out
    """)
    (base / "pipeline.yaml").write_text(pipeline_yaml, encoding="utf-8")

    transform_py = textwrap.dedent("""\
        from trakt import step_contract


        @step_contract(inputs=["input"], outputs=["output"])
        def run(ctx, input):
            frame = input.copy()
            # Add your transformation logic here
            return {"output": frame}
    """)
    (base / "steps" / "transform.py").write_text(transform_py, encoding="utf-8")

    print(f"Created pipeline scaffold in '{name}/'")
    print(f"  {name}/pipeline.yaml")
    print(f"  {name}/steps/transform.py")
    print(f"  {name}/input/")


def main(argv: list[str] | None = None) -> None:
    """Unified trakt CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="trakt",
        description="Trakt ETL framework CLI.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- trakt run ---
    run_parser = subparsers.add_parser("run", help="Run a pipeline locally.")
    _add_pipeline_args(run_parser)
    _add_common_args(run_parser)
    run_parser.add_argument(
        "--input-dir", default=".", help="Base directory for local input files.",
    )
    run_parser.add_argument(
        "--output-dir", default="outputs",
        help="Directory for output artifacts and manifest.",
    )
    run_parser.add_argument(
        "--input", action="append", default=[], metavar="NAME=PATH",
        help="Override a pipeline input artifact source.",
    )
    run_parser.add_argument(
        "--param", action="append", default=[], metavar="STEP.PARAM=VALUE",
        help="Override a const binding (value parsed as YAML).",
    )
    run_parser.add_argument(
        "--lenient", action="store_true",
        help="Allow unknown keys in definitions (default: strict).",
    )
    run_parser.add_argument("--run-id", default=None, help="Optional run id.")
    run_parser.add_argument(
        "--pipeline-version", default=None, help="Pipeline version metadata.",
    )
    run_parser.add_argument(
        "--manifest-path", default=None, help="Explicit manifest output path.",
    )
    run_parser.add_argument(
        "--stream-chunk-size", type=int, default=50_000,
        help="Chunk size for stream execution mode.",
    )
    run_parser.add_argument(
        "--otel-enabled", action="store_true",
        help="Enable OpenTelemetry spans.",
    )
    run_parser.add_argument(
        "--otel-service-name", default="trakt",
        help="OpenTelemetry service.name attribute.",
    )
    run_parser.add_argument(
        "--otel-tracer-name", default="trakt.runner",
        help="OpenTelemetry tracer name.",
    )
    run_parser.set_defaults(func=_cmd_run)

    # --- trakt validate ---
    validate_parser = subparsers.add_parser(
        "validate", help="Validate pipeline YAML without executing.",
    )
    _add_pipeline_args(validate_parser)
    _add_common_args(validate_parser)
    validate_parser.add_argument(
        "--lenient", action="store_true",
        help="Allow unknown keys in definitions (default: strict).",
    )
    validate_parser.set_defaults(func=_cmd_validate)

    # --- trakt init ---
    init_parser = subparsers.add_parser(
        "init", help="Scaffold a new pipeline project.",
    )
    init_parser.add_argument("name", help="Name of the pipeline to create.")
    _add_common_args(init_parser)
    init_parser.set_defaults(func=_cmd_init)

    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        raise SystemExit(1)

    _configure_logging(args)
    args.func(args)


if __name__ == "__main__":
    main()
