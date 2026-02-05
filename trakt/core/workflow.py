"""Python builder API for creating and running pipelines."""

from dataclasses import dataclass, field
from typing import Any, Callable, Self

from trakt.core.artifacts import Artifact
from trakt.core.pipeline import Pipeline
from trakt.core.registry import StepRegistry
from trakt.core.steps import ResolvedStep, StepBindingError

StepHandler = Callable[..., dict[str, Any]]


@dataclass(slots=True)
class WorkflowStep:
    """Declarative step definition used by WorkflowBuilder."""

    step_id: str
    uses: str | None = None
    handler: StepHandler | None = None
    bindings: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class WorkflowBuilder:
    """Build a Pipeline using a Python API instead of YAML."""

    name: str
    execution_mode: str = "batch"
    registry: StepRegistry | None = None
    _inputs: dict[str, Artifact] = field(default_factory=dict)
    _steps: list[WorkflowStep] = field(default_factory=list)
    _outputs: dict[str, str] = field(default_factory=dict)

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
        self._inputs[name] = Artifact(
            name=name,
            kind=kind,
            uri=uri or name,
            schema=schema,
            metadata=dict(metadata or {}),
            combine_strategy=combine_strategy,
        )
        return self

    def step(
        self,
        step_id: str,
        *,
        uses: str | None = None,
        run: StepHandler | None = None,
        with_: dict[str, Any] | None = None,
    ) -> Self:
        if (uses is None) == (run is None):
            raise ValueError(
                f"Workflow step '{step_id}' must define exactly one of 'uses' or 'run'."
            )
        if with_ is None:
            with_ = {}
        if not isinstance(with_, dict):
            raise TypeError(f"Workflow step '{step_id}' field 'with_' must be a mapping.")

        self._steps.append(
            WorkflowStep(
                step_id=step_id,
                uses=uses,
                handler=run,
                bindings=dict(with_),
            )
        )
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


def workflow(
    name: str,
    *,
    execution_mode: str = "batch",
    registry: StepRegistry | None = None,
) -> WorkflowBuilder:
    """Convenience factory for the WorkflowBuilder API."""
    return WorkflowBuilder(name=name, execution_mode=execution_mode, registry=registry)


def _callable_name(handler: StepHandler) -> str:
    module_name = getattr(handler, "__module__", "__main__")
    qualified_name = getattr(handler, "__qualname__", getattr(handler, "__name__", "run"))
    return f"{module_name}.{qualified_name}"
