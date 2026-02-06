"""Pipeline definition container."""

from dataclasses import dataclass, field

from trakt.core.artifacts import Artifact, CombineStrategy, OutputDataset
from trakt.core.bindings import is_const_binding
from trakt.core.steps import ResolvedStep, Step


class PipelineValidationError(ValueError):
    """Validation errors for pipeline wiring."""

    def __init__(
        self,
        *,
        invalid_execution_mode: str | None = None,
        incompatible_steps: list[tuple[str, str]] | None = None,
        incompatible_inputs: list[tuple[str, str]] | None = None,
        missing_inputs: list[tuple[str, str]] | None = None,
        unused_inputs: list[str] | None = None,
        output_collisions: list[tuple[str, str, str]] | None = None,
        unknown_output_bindings: list[tuple[str, str]] | None = None,
        suspected_literal_bindings: list[tuple[str, str, str]] | None = None,
    ) -> None:
        self.invalid_execution_mode = invalid_execution_mode
        self.incompatible_steps = incompatible_steps or []
        self.incompatible_inputs = incompatible_inputs or []
        self.missing_inputs = missing_inputs or []
        self.unused_inputs = unused_inputs or []
        self.output_collisions = output_collisions or []
        self.unknown_output_bindings = unknown_output_bindings or []
        self.suspected_literal_bindings = suspected_literal_bindings or []

        details: list[str] = []
        if self.invalid_execution_mode:
            details.append(f"invalid execution mode={self.invalid_execution_mode}")
        if self.incompatible_steps:
            details.append(
                "mode incompatible steps="
                + ", ".join(
                    f"{step_id}:{mode}"
                    for step_id, mode in self.incompatible_steps
                )
            )
        if self.incompatible_inputs:
            details.append(
                "mode incompatible inputs="
                + ", ".join(
                    f"{input_name}:{reason}"
                    for input_name, reason in self.incompatible_inputs
                )
            )
        if self.missing_inputs:
            details.append(
                "missing inputs="
                + ", ".join(
                    f"{step_id}:{input_name}"
                    for step_id, input_name in self.missing_inputs
                )
            )
        if self.unused_inputs:
            details.append("unused inputs=" + ", ".join(sorted(self.unused_inputs)))
        if self.output_collisions:
            details.append(
                "output collisions="
                + ", ".join(
                    f"{step_id}:{output_name} (already defined by {owner})"
                    for step_id, output_name, owner in self.output_collisions
                )
            )
        if self.unknown_output_bindings:
            details.append(
                "unknown output bindings="
                + ", ".join(
                    f"{target}<-{source}"
                    for target, source in self.unknown_output_bindings
                )
            )
        if self.suspected_literal_bindings:
            details.append(
                "suspected literal bindings (use {const: value})="
                + ", ".join(
                    f"{step_id}.{key}='{value}'"
                    for step_id, key, value in self.suspected_literal_bindings
                )
            )
        message = "; ".join(details) if details else "pipeline validation failed"
        super().__init__(message)


@dataclass(slots=True)
class Pipeline:
    """Ordered pipeline definition with input artifacts and step chain."""

    name: str
    execution_mode: str = "batch"
    inputs: dict[str, Artifact] = field(default_factory=dict)
    steps: list[Step] = field(default_factory=list)
    outputs: dict[str, OutputDataset | str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.execution_mode = str(self.execution_mode).strip().lower()

    def validate(self) -> None:
        """Validate input/output wiring across the full step chain."""
        mode = self.execution_mode

        invalid_execution_mode: str | None = None
        if mode not in {"batch", "stream"}:
            invalid_execution_mode = mode

        produced_by: dict[str, str] = {name: "pipeline input" for name in self.inputs}
        available_names = set(self.inputs)
        used_pipeline_inputs: set[str] = set()

        incompatible_steps: list[tuple[str, str]] = []
        incompatible_inputs: list[tuple[str, str]] = []
        missing_inputs: list[tuple[str, str]] = []
        output_collisions: list[tuple[str, str, str]] = []

        if mode == "stream":
            for input_name, artifact in self.inputs.items():
                if (
                    artifact.kind.strip().lower() == "csv"
                    and artifact.combine_strategy is not CombineStrategy.CONCAT
                ):
                    incompatible_inputs.append(
                        (
                            input_name,
                            f"combine_strategy={artifact.combine_strategy.value}",
                        )
                    )

        for step in self.steps:
            if mode == "batch" and not step.supports_batch:
                incompatible_steps.append((step.id, mode))
            if mode == "stream" and not step.supports_stream:
                incompatible_steps.append((step.id, mode))

            for input_name in step.inputs:
                if input_name not in available_names:
                    missing_inputs.append((step.id, input_name))
                elif input_name in self.inputs:
                    used_pipeline_inputs.add(input_name)

            for output_name in step.outputs:
                owner = produced_by.get(output_name)
                if owner is not None:
                    output_collisions.append((step.id, output_name, owner))
                    continue

                produced_by[output_name] = step.id
                available_names.add(output_name)

        unused_inputs = sorted(
            name
            for name in set(self.inputs) - used_pipeline_inputs
            if _is_required_input(self.inputs[name])
        )
        unknown_output_bindings: list[tuple[str, str]] = []
        for target, output_spec in self.outputs.items():
            source = _output_source(target, output_spec)
            if source not in available_names:
                unknown_output_bindings.append((target, source))

        suspected_literal_bindings = _detect_suspected_literals(
            self.steps, available_names
        )

        if (
            invalid_execution_mode
            or incompatible_steps
            or incompatible_inputs
            or missing_inputs
            or unused_inputs
            or output_collisions
            or unknown_output_bindings
            or suspected_literal_bindings
        ):
            raise PipelineValidationError(
                invalid_execution_mode=invalid_execution_mode,
                incompatible_steps=incompatible_steps,
                incompatible_inputs=incompatible_inputs,
                missing_inputs=missing_inputs,
                unused_inputs=unused_inputs,
                output_collisions=output_collisions,
                unknown_output_bindings=unknown_output_bindings,
                suspected_literal_bindings=suspected_literal_bindings,
            )


def _detect_suspected_literals(
    steps: list[Step], available_names: set[str]
) -> list[tuple[str, str, str]]:
    """Detect string bindings that look like literal values rather than artifact refs."""
    suspects: list[tuple[str, str, str]] = []
    for step in steps:
        if not isinstance(step, ResolvedStep):
            continue
        for key, value in step.input_bindings().items():
            if is_const_binding(value):
                continue
            if not isinstance(value, str):
                continue
            if value in available_names:
                continue
            suspects.append((step.id, key, value))
    return suspects


def _is_required_input(artifact: Artifact) -> bool:
    required_flag = artifact.metadata.get("required", True)
    if isinstance(required_flag, bool):
        return required_flag
    if isinstance(required_flag, str):
        return required_flag.lower() not in {"false", "0", "no"}
    return bool(required_flag)


def _output_source(name: str, output_spec: OutputDataset | str) -> str:
    if isinstance(output_spec, OutputDataset):
        return output_spec.source
    if isinstance(output_spec, str):
        return output_spec
    raise TypeError(
        f"Pipeline output '{name}' must be a string or OutputDataset, got "
        f"{type(output_spec).__name__}."
    )
