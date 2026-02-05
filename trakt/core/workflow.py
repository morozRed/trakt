"""Python DSL for creating and running pipelines."""

from dataclasses import dataclass, field
from typing import Any, Callable, Self

from trakt.core.artifacts import Artifact
from trakt.core.pipeline import Pipeline
from trakt.core.registry import StepRegistry
from trakt.core.steps import ResolvedStep, StepBindingError

StepHandler = Callable[..., dict[str, Any]]


@dataclass(slots=True)
class WorkflowArtifact:
    """Reusable artifact specification for Python-defined workflows."""

    name: str
    kind: str = "csv"
    uri: str | None = None
    schema: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    combine_strategy: str = "concat"

    def as_kind(self, kind: str) -> Self:
        self.kind = kind
        return self

    def at(self, uri: str) -> Self:
        self.uri = uri
        return self

    def combine(self, strategy: str) -> Self:
        self.combine_strategy = strategy
        return self

    def with_schema(self, schema: dict[str, Any] | None) -> Self:
        self.schema = schema
        return self

    def meta(self, **metadata: Any) -> Self:
        self.metadata.update(metadata)
        return self

    def to_artifact(self) -> Artifact:
        return Artifact(
            name=self.name,
            kind=self.kind,
            uri=self.uri or self.name,
            schema=self.schema,
            metadata=dict(self.metadata),
            combine_strategy=self.combine_strategy,
        )


@dataclass(slots=True)
class WorkflowStep:
    """Reusable step specification for Python-defined workflows."""

    step_id: str
    uses: str | None = None
    handler: StepHandler | None = None
    bindings: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (self.uses is None) == (self.handler is None):
            raise ValueError(
                f"Workflow step '{self.step_id}' must define exactly one of 'uses' or 'run'."
            )

    def bind(self, **bindings: Any) -> Self:
        """Set step bindings and return this step."""
        for key, value in bindings.items():
            self.bindings[key] = _normalize_binding_value(
                value,
                step_id=self.step_id,
                binding_key=key,
            )
        return self

    def bind_input(
        self, artifact_ref: str | WorkflowArtifact | Artifact, *, param: str = "input"
    ) -> Self:
        self.bindings[param] = _artifact_name(
            artifact_ref,
            step_id=self.step_id,
            binding_key=param,
        )
        return self

    def bind_inputs(
        self,
        *artifact_refs: str | WorkflowArtifact | Artifact,
        param: str = "inputs",
    ) -> Self:
        if not artifact_refs:
            raise ValueError(
                f"Workflow step '{self.step_id}' bind_inputs requires at least one input."
            )
        self.bindings[param] = [
            _artifact_name(artifact_ref, step_id=self.step_id, binding_key=param)
            for artifact_ref in artifact_refs
        ]
        return self

    def bind_output(
        self, artifact_ref: str | WorkflowArtifact | Artifact, *, param: str = "output"
    ) -> Self:
        self.bindings[param] = _artifact_name(
            artifact_ref,
            step_id=self.step_id,
            binding_key=param,
        )
        return self

    def bind_outputs(
        self,
        *artifact_refs: str | WorkflowArtifact | Artifact,
        param: str = "outputs",
    ) -> Self:
        if not artifact_refs:
            raise ValueError(
                f"Workflow step '{self.step_id}' bind_outputs requires at least one output."
            )
        self.bindings[param] = [
            _artifact_name(artifact_ref, step_id=self.step_id, binding_key=param)
            for artifact_ref in artifact_refs
        ]
        return self


@dataclass(slots=True)
class WorkflowBuilder:
    """Build a Pipeline using a Python DSL instead of YAML."""

    name: str
    execution_mode: str = "batch"
    registry: StepRegistry | None = None
    _inputs: dict[str, Artifact] = field(default_factory=dict)
    _steps: list[WorkflowStep] = field(default_factory=list)
    _outputs: dict[str, str] = field(default_factory=dict)

    def source(self, spec: WorkflowArtifact | Artifact) -> Self:
        """Register one input artifact."""
        resolved = _coerce_artifact(spec)
        self._inputs[resolved.name] = resolved
        return self

    def sources(self, specs: list[WorkflowArtifact | Artifact]) -> Self:
        """Register multiple input artifacts."""
        for spec in specs:
            self.source(spec)
        return self

    def input(
        self,
        name: str,
        *,
        uri: str | None = None,
        kind: str = "csv",
        schema: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        combine_strategy: str = "concat",
    ) -> Self:
        """Convenience helper for adding one input artifact."""
        return self.source(
            artifact(
                name,
                kind=kind,
                uri=uri,
                schema=schema,
                metadata=metadata,
                combine_strategy=combine_strategy,
            )
        )

    def step(self, spec: WorkflowStep) -> Self:
        """Append one pre-defined workflow step."""
        if not isinstance(spec, WorkflowStep):
            raise TypeError("WorkflowBuilder.step(...) expects a WorkflowStep instance.")
        self._steps.append(spec)
        return self

    def steps(self, specs: list[WorkflowStep]) -> Self:
        """Append multiple pre-defined workflow steps in order."""
        for spec in specs:
            self.step(spec)
        return self

    def output(self, name: str, *, from_: str) -> Self:
        self._outputs[name] = from_
        return self

    def build(self, *, registry: StepRegistry | None = None) -> Pipeline:
        active_registry = registry or self.registry
        resolved_steps: list[ResolvedStep] = []

        for spec in self._steps:
            if spec.handler is None and active_registry is None:
                active_registry = StepRegistry.from_entry_points()
            handler, uses = self._resolve_handler(spec, active_registry=active_registry)
            resolved_steps.append(
                ResolvedStep.from_definition(
                    step_id=spec.step_id,
                    uses=uses,
                    handler=handler,
                    bindings=spec.bindings,
                )
            )

        pipeline = Pipeline(
            name=self.name,
            execution_mode=self.execution_mode,
            inputs=dict(self._inputs),
            steps=resolved_steps,
            outputs=dict(self._outputs),
        )
        pipeline.validate()
        return pipeline

    def run(
        self,
        runner: Any,
        *,
        registry: StepRegistry | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        pipeline = self.build(registry=registry)
        return runner.run(pipeline, **kwargs)

    def _resolve_handler(
        self,
        spec: WorkflowStep,
        *,
        active_registry: StepRegistry | None,
    ) -> tuple[StepHandler, str]:
        if spec.handler is not None:
            return spec.handler, spec.uses or _callable_name(spec.handler)

        if spec.uses is None:
            raise ValueError(f"Workflow step '{spec.step_id}' has neither uses nor handler.")

        registry = active_registry or StepRegistry.from_entry_points()
        try:
            handler = registry.resolve_uses(spec.uses)
        except (ImportError, AttributeError, KeyError, StepBindingError, ValueError) as exc:
            raise ValueError(
                f"Failed to resolve workflow step '{spec.step_id}' using '{spec.uses}': {exc}"
            ) from exc
        return handler, spec.uses


def artifact(
    name: str,
    *,
    kind: str = "csv",
    uri: str | None = None,
    schema: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    combine_strategy: str = "concat",
) -> WorkflowArtifact:
    """Create a reusable workflow artifact specification."""
    return WorkflowArtifact(
        name=name,
        kind=kind,
        uri=uri,
        schema=schema,
        metadata=dict(metadata or {}),
        combine_strategy=combine_strategy,
    )


def step(
    step_id: str,
    *,
    uses: str | None = None,
    run: StepHandler | None = None,
) -> WorkflowStep:
    """Create a reusable workflow step specification."""
    return WorkflowStep(step_id=step_id, uses=uses, handler=run)


def workflow(
    name: str,
    *,
    execution_mode: str = "batch",
    registry: StepRegistry | None = None,
) -> WorkflowBuilder:
    """Convenience factory for the WorkflowBuilder API."""
    return WorkflowBuilder(name=name, execution_mode=execution_mode, registry=registry)


def _coerce_artifact(spec: WorkflowArtifact | Artifact) -> Artifact:
    if isinstance(spec, WorkflowArtifact):
        return spec.to_artifact()
    if isinstance(spec, Artifact):
        return spec
    raise TypeError("WorkflowBuilder.source(...) expects WorkflowArtifact or Artifact.")


def _artifact_name(
    ref: str | WorkflowArtifact | Artifact,
    *,
    step_id: str,
    binding_key: str,
) -> str:
    if isinstance(ref, str):
        return ref
    if isinstance(ref, WorkflowArtifact):
        return ref.name
    if isinstance(ref, Artifact):
        return ref.name
    raise TypeError(
        f"Workflow step '{step_id}' binding '{binding_key}' expects str/WorkflowArtifact/Artifact."
    )


def _normalize_binding_value(value: Any, *, step_id: str, binding_key: str) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (WorkflowArtifact, Artifact)):
        return _artifact_name(value, step_id=step_id, binding_key=binding_key)
    if isinstance(value, list):
        return [
            _normalize_binding_value(item, step_id=step_id, binding_key=binding_key)
            for item in value
        ]
    if isinstance(value, tuple):
        return [
            _normalize_binding_value(item, step_id=step_id, binding_key=binding_key)
            for item in value
        ]
    if isinstance(value, dict):
        return {
            key: _normalize_binding_value(item, step_id=step_id, binding_key=binding_key)
            for key, item in value.items()
        }
    return value


def _callable_name(handler: StepHandler) -> str:
    module_name = getattr(handler, "__module__", "__main__")
    qualified_name = getattr(handler, "__qualname__", getattr(handler, "__name__", "run"))
    return f"{module_name}.{qualified_name}"
