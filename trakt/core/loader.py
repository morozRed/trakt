"""Pipeline YAML loading and step resolution."""

from pathlib import Path
from typing import Any

import yaml

from trakt.core.artifacts import Artifact
from trakt.core.pipeline import Pipeline, PipelineValidationError
from trakt.core.registry import StepRegistry
from trakt.core.steps import ResolvedStep, StepBindingError


class PipelineLoadError(ValueError):
    """Raised when a pipeline YAML file cannot be parsed or resolved."""


def load_pipeline_from_yaml(
    pipeline_file: str | Path, registry: StepRegistry | None = None
) -> Pipeline:
    """Build and validate a pipeline from a YAML definition file."""
    path = Path(pipeline_file)
    payload = _read_yaml(path)
    if not isinstance(payload, dict):
        raise PipelineLoadError(
            f"Pipeline file '{path}' must contain a mapping at the root."
        )

    step_registry = registry or StepRegistry.from_entry_points()
    name = str(payload.get("name") or path.parent.name or path.stem)
    execution_mode = _parse_execution_mode(payload)
    inputs = _parse_inputs(payload.get("inputs", {}))
    steps = _parse_steps(payload.get("steps", []), registry=step_registry)
    outputs = _parse_outputs(payload.get("outputs", {}))

    pipeline = Pipeline(
        name=name,
        execution_mode=execution_mode,
        inputs=inputs,
        steps=steps,
        outputs=outputs,
    )
    try:
        pipeline.validate()
    except (PipelineValidationError, StepBindingError) as exc:
        raise PipelineLoadError(f"Pipeline '{name}' is invalid: {exc}") from exc
    return pipeline


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PipelineLoadError(f"Failed to read pipeline file '{path}': {exc}") from exc

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise PipelineLoadError(f"Invalid YAML in '{path}': {exc}") from exc

    return data or {}


def _parse_inputs(raw_inputs: Any) -> dict[str, Artifact]:
    if raw_inputs is None:
        return {}
    if not isinstance(raw_inputs, dict):
        raise PipelineLoadError("Pipeline 'inputs' must be a mapping.")

    parsed: dict[str, Artifact] = {}
    for name, definition in raw_inputs.items():
        parsed[name] = _parse_input_definition(name, definition)
    return parsed


def _parse_input_definition(name: str, definition: Any) -> Artifact:
    if definition is None:
        definition = {}

    if isinstance(definition, str):
        return Artifact(name=name, kind="csv", uri=definition)

    if not isinstance(definition, dict):
        raise PipelineLoadError(f"Input '{name}' must be a mapping or string.")

    known_keys = {"kind", "uri", "schema", "metadata", "combine_strategy"}
    raw_metadata = definition.get("metadata") or {}
    if not isinstance(raw_metadata, dict):
        raise PipelineLoadError(f"Input '{name}' field 'metadata' must be a mapping.")

    metadata = dict(raw_metadata)
    for key, value in definition.items():
        if key not in known_keys:
            metadata[key] = value

    return Artifact(
        name=name,
        kind=str(definition.get("kind", "csv")),
        uri=str(definition.get("uri", name)),
        schema=definition.get("schema"),
        metadata=metadata,
        combine_strategy=definition.get("combine_strategy", "concat"),
    )


def _parse_steps(raw_steps: Any, registry: StepRegistry) -> list[ResolvedStep]:
    if raw_steps is None:
        return []
    if not isinstance(raw_steps, list):
        raise PipelineLoadError("Pipeline 'steps' must be a list.")

    parsed: list[ResolvedStep] = []
    for index, definition in enumerate(raw_steps):
        if not isinstance(definition, dict):
            raise PipelineLoadError(f"Step #{index + 1} must be a mapping.")

        step_id = definition.get("id")
        uses = definition.get("uses")
        if not step_id or not isinstance(step_id, str):
            raise PipelineLoadError(f"Step #{index + 1} missing string 'id'.")
        if not uses or not isinstance(uses, str):
            raise PipelineLoadError(f"Step '{step_id}' missing string 'uses'.")

        bindings = definition.get("with", {})
        if bindings is None:
            bindings = {}
        if not isinstance(bindings, dict):
            raise PipelineLoadError(f"Step '{step_id}' field 'with' must be a mapping.")

        try:
            handler = registry.resolve_uses(uses)
            step = ResolvedStep.from_definition(
                step_id=step_id,
                uses=uses,
                handler=handler,
                bindings=bindings,
            )
        except (
            ImportError,
            AttributeError,
            KeyError,
            StepBindingError,
            ValueError,
        ) as exc:
            raise PipelineLoadError(
                f"Failed to resolve step '{step_id}' using '{uses}': {exc}"
            ) from exc

        parsed.append(step)
    return parsed


def _parse_outputs(raw_outputs: Any) -> dict[str, str]:
    if raw_outputs is None:
        return {}

    if isinstance(raw_outputs, dict) and "datasets" in raw_outputs:
        datasets = raw_outputs.get("datasets")
        if not isinstance(datasets, list):
            raise PipelineLoadError("Pipeline 'outputs.datasets' must be a list.")

        parsed: dict[str, str] = {}
        for dataset in datasets:
            if not isinstance(dataset, dict):
                raise PipelineLoadError("Each output dataset must be a mapping.")
            name = dataset.get("name")
            source = dataset.get("from")
            if not isinstance(name, str) or not isinstance(source, str):
                raise PipelineLoadError(
                    "Each output dataset must define string fields 'name' and 'from'."
                )
            parsed[name] = source
        return parsed

    if isinstance(raw_outputs, dict):
        parsed_outputs: dict[str, str] = {}
        for name, source in raw_outputs.items():
            if not isinstance(source, str):
                raise PipelineLoadError(
                    f"Output '{name}' must map to a string source artifact."
                )
            parsed_outputs[str(name)] = source
        return parsed_outputs

    raise PipelineLoadError("Pipeline 'outputs' must be a mapping.")


def _parse_execution_mode(payload: dict[str, Any]) -> str:
    raw_execution = payload.get("execution")
    top_level_mode = payload.get("execution_mode")

    if raw_execution is None:
        return _coerce_execution_mode(top_level_mode, field_name="execution_mode")

    if not isinstance(raw_execution, dict):
        raise PipelineLoadError("Pipeline 'execution' must be a mapping.")

    nested_mode = raw_execution.get("mode")
    if nested_mode is not None and top_level_mode is not None and nested_mode != top_level_mode:
        raise PipelineLoadError(
            "Pipeline defines conflicting execution modes in 'execution.mode' and 'execution_mode'."
        )

    return _coerce_execution_mode(
        nested_mode if nested_mode is not None else top_level_mode,
        field_name="execution.mode",
    )


def _coerce_execution_mode(value: Any, field_name: str) -> str:
    if value is None:
        return "batch"
    if not isinstance(value, str):
        raise PipelineLoadError(f"Pipeline '{field_name}' must be a string.")
    return value.strip().lower() or "batch"
