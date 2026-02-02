"""Pipeline definition container."""

from dataclasses import dataclass, field

from trakt.core.artifacts import Artifact
from trakt.core.steps import Step


class PipelineValidationError(ValueError):
    """Validation errors for pipeline wiring."""

    def __init__(
        self,
        *,
        missing_inputs: list[tuple[str, str]] | None = None,
        unused_inputs: list[str] | None = None,
        output_collisions: list[tuple[str, str, str]] | None = None,
        unknown_output_bindings: list[tuple[str, str]] | None = None,
    ) -> None:
        self.missing_inputs = missing_inputs or []
        self.unused_inputs = unused_inputs or []
        self.output_collisions = output_collisions or []
        self.unknown_output_bindings = unknown_output_bindings or []

        details: list[str] = []
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
        message = "; ".join(details) if details else "pipeline validation failed"
        super().__init__(message)


@dataclass(slots=True)
class Pipeline:
    """Ordered pipeline definition with input artifacts and step chain."""

    name: str
    inputs: dict[str, Artifact] = field(default_factory=dict)
    steps: list[Step] = field(default_factory=list)
    outputs: dict[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate input/output wiring across the full step chain."""
        produced_by: dict[str, str] = {name: "pipeline input" for name in self.inputs}
        available_names = set(self.inputs)
        used_pipeline_inputs: set[str] = set()

        missing_inputs: list[tuple[str, str]] = []
        output_collisions: list[tuple[str, str, str]] = []

        for step in self.steps:
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
        unknown_output_bindings = [
            (target, source)
            for target, source in self.outputs.items()
            if source not in available_names
        ]

        if (
            missing_inputs
            or unused_inputs
            or output_collisions
            or unknown_output_bindings
        ):
            raise PipelineValidationError(
                missing_inputs=missing_inputs,
                unused_inputs=unused_inputs,
                output_collisions=output_collisions,
                unknown_output_bindings=unknown_output_bindings,
            )


def _is_required_input(artifact: Artifact) -> bool:
    required_flag = artifact.metadata.get("required", True)
    if isinstance(required_flag, bool):
        return required_flag
    if isinstance(required_flag, str):
        return required_flag.lower() not in {"false", "0", "no"}
    return bool(required_flag)
