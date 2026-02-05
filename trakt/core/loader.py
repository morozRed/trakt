"""Pipeline YAML loading and step resolution."""

from pathlib import Path
from typing import Any

import yaml

from trakt.core.artifacts import Artifact, OutputDataset
from trakt.core.pipeline import Pipeline, PipelineValidationError
from trakt.core.registry import StepRegistry
from trakt.core.steps import ResolvedStep, StepBindingError


class PipelineLoadError(ValueError):
    """Raised when a pipeline YAML file cannot be parsed or resolved."""


def load_pipeline_from_yaml(
    pipeline_file: str | Path,
    registry: StepRegistry | None = None,
    *,
    strict_unknown_keys: bool = False,
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
    inputs = _parse_inputs(
        payload.get("inputs", {}),
        strict_unknown_keys=strict_unknown_keys,
    )
    steps = _parse_steps(
        payload.get("steps", []),
        registry=step_registry,
        strict_unknown_keys=strict_unknown_keys,
    )
    outputs = _parse_outputs(
        payload.get("outputs", {}),
        strict_unknown_keys=strict_unknown_keys,
    )

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


def _parse_inputs(
    raw_inputs: Any,
    *,
    strict_unknown_keys: bool,
) -> dict[str, Artifact]:
    if raw_inputs is None:
        return {}
    if not isinstance(raw_inputs, dict):
        raise PipelineLoadError("Pipeline 'inputs' must be a mapping.")

    parsed: dict[str, Artifact] = {}
    for name, definition in raw_inputs.items():
        parsed[name] = _parse_input_definition(
            name,
            definition,
            strict_unknown_keys=strict_unknown_keys,
        )
    return parsed


def _parse_input_definition(
    name: str,
    definition: Any,
    *,
    strict_unknown_keys: bool,
) -> Artifact:
    if definition is None:
        definition = {}

    if isinstance(definition, str):
        return Artifact(name=name, kind="csv", uri=definition)

    if not isinstance(definition, dict):
        raise PipelineLoadError(f"Input '{name}' must be a mapping or string.")

    known_keys = {"kind", "uri", "schema", "metadata", "combine_strategy"}
    unknown_keys = sorted(key for key in definition if key not in known_keys)
    if strict_unknown_keys and unknown_keys:
        raise PipelineLoadError(
            f"Input '{name}' has unknown fields: {', '.join(unknown_keys)}."
        )
    raw_metadata = definition.get("metadata") or {}
    if not isinstance(raw_metadata, dict):
        raise PipelineLoadError(f"Input '{name}' field 'metadata' must be a mapping.")

    metadata = dict(raw_metadata)
    for key, value in definition.items():
        if key in unknown_keys:
            metadata[key] = value

    return Artifact(
        name=name,
        kind=str(definition.get("kind", "csv")),
        uri=str(definition.get("uri", name)),
        schema=definition.get("schema"),
        metadata=metadata,
        combine_strategy=definition.get("combine_strategy", "concat"),
    )


def _parse_steps(
    raw_steps: Any,
    registry: StepRegistry,
    *,
    strict_unknown_keys: bool,
) -> list[ResolvedStep]:
    if raw_steps is None:
        return []
    if not isinstance(raw_steps, list):
        raise PipelineLoadError("Pipeline 'steps' must be a list.")

    parsed: list[ResolvedStep] = []
    for index, definition in enumerate(raw_steps):
        if not isinstance(definition, dict):
            raise PipelineLoadError(f"Step #{index + 1} must be a mapping.")

        if strict_unknown_keys:
            known_keys = {"id", "uses", "with"}
            unknown_keys = sorted(key for key in definition if key not in known_keys)
            if unknown_keys:
                raise PipelineLoadError(
                    f"Step #{index + 1} has unknown fields: {', '.join(unknown_keys)}."
                )

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


def _parse_outputs(
    raw_outputs: Any,
    *,
    strict_unknown_keys: bool,
) -> dict[str, OutputDataset]:
    if raw_outputs is None:
        return {}

    if isinstance(raw_outputs, dict) and "datasets" in raw_outputs:
        if strict_unknown_keys:
            unknown_keys = sorted(
                key for key in raw_outputs if key != "datasets"
            )
            if unknown_keys:
                raise PipelineLoadError(
                    "Pipeline 'outputs' has unknown fields when using "
                    f"'datasets': {', '.join(unknown_keys)}."
                )
        datasets = raw_outputs.get("datasets")
        if not isinstance(datasets, list):
            raise PipelineLoadError("Pipeline 'outputs.datasets' must be a list.")

        parsed: dict[str, OutputDataset] = {}
        for dataset in datasets:
            if not isinstance(dataset, dict):
                raise PipelineLoadError("Each output dataset must be a mapping.")
            name = dataset.get("name")
            source = dataset.get("from")
            if not isinstance(name, str) or not isinstance(source, str):
                raise PipelineLoadError(
                    "Each output dataset must define string fields 'name' and 'from'."
                )
            metadata = dataset.get("metadata") or {}
            if not isinstance(metadata, dict):
                raise PipelineLoadError(
                    f"Output dataset '{name}' field 'metadata' must be a mapping."
                )
            metadata = dict(metadata)
            known_keys = {"name", "from", "kind", "uri", "metadata"}
            unknown_keys = sorted(key for key in dataset if key not in known_keys)
            if strict_unknown_keys and unknown_keys:
                raise PipelineLoadError(
                    f"Output dataset '{name}' has unknown fields: "
                    + ", ".join(unknown_keys)
                    + "."
                )
            for key, value in dataset.items():
                if key in unknown_keys:
                    metadata[key] = value
            parsed[name] = OutputDataset(
                name=name,
                source=source,
                kind=_coerce_optional_string(dataset.get("kind"), "kind", output_name=name),
                uri=_coerce_optional_string(dataset.get("uri"), "uri", output_name=name),
                metadata=metadata,
            )
        return parsed

    if isinstance(raw_outputs, dict):
        parsed_outputs: dict[str, OutputDataset] = {}
        for name, source_definition in raw_outputs.items():
            output_name = str(name)
            if isinstance(source_definition, str):
                parsed_outputs[output_name] = OutputDataset(
                    name=output_name,
                    source=source_definition,
                )
                continue

            if not isinstance(source_definition, dict):
                raise PipelineLoadError(
                    f"Output '{output_name}' must map to a string source artifact or mapping."
                )

            source = source_definition.get("from")
            if not isinstance(source, str):
                raise PipelineLoadError(
                    f"Output '{output_name}' mapping must define string field 'from'."
                )
            metadata = source_definition.get("metadata") or {}
            if not isinstance(metadata, dict):
                raise PipelineLoadError(
                    f"Output '{output_name}' field 'metadata' must be a mapping."
                )
            metadata = dict(metadata)
            known_keys = {"from", "kind", "uri", "metadata"}
            unknown_keys = sorted(
                key for key in source_definition if key not in known_keys
            )
            if strict_unknown_keys and unknown_keys:
                raise PipelineLoadError(
                    f"Output '{output_name}' has unknown fields: "
                    + ", ".join(unknown_keys)
                    + "."
                )
            for key, value in source_definition.items():
                if key in unknown_keys:
                    metadata[key] = value
            parsed_outputs[output_name] = OutputDataset(
                name=output_name,
                source=source,
                kind=_coerce_optional_string(
                    source_definition.get("kind"),
                    "kind",
                    output_name=output_name,
                ),
                uri=_coerce_optional_string(
                    source_definition.get("uri"),
                    "uri",
                    output_name=output_name,
                ),
                metadata=metadata,
            )
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


def _coerce_optional_string(value: Any, field: str, *, output_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise PipelineLoadError(
            f"Output '{output_name}' field '{field}' must be a string when provided."
        )
    normalized = value.strip()
    return normalized or None
