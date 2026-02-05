"""Core domain types for Trakt."""

from trakt.core.artifacts import (
    Artifact,
    CombineStrategy,
    OutputDataset,
    combine_artifact_frames,
)
from trakt.core.bindings import Const, const
from trakt.core.context import Context
from trakt.core.loader import PipelineLoadError, load_pipeline_from_yaml
from trakt.core.policies import (
    DedupePolicy,
    JoinPolicy,
    QualityGatePolicy,
    RenamePolicy,
    apply_dedupe_policy,
    apply_join_policy,
    apply_rename_policy,
    evaluate_quality_gates,
)
from trakt.core.pipeline import Pipeline, PipelineValidationError
from trakt.core.registry import StepRegistry
from trakt.core.steps import ResolvedStep, Step, StepBindingError
from trakt.core.workflow import (
    WorkflowArtifact,
    WorkflowBuilder,
    WorkflowRef,
    WorkflowStep,
    artifact,
    ref,
    step,
    workflow,
)

__all__ = [
    "Artifact",
    "CombineStrategy",
    "OutputDataset",
    "Const",
    "Context",
    "DedupePolicy",
    "JoinPolicy",
    "WorkflowRef",
    "WorkflowArtifact",
    "Pipeline",
    "PipelineLoadError",
    "PipelineValidationError",
    "QualityGatePolicy",
    "RenamePolicy",
    "ResolvedStep",
    "Step",
    "StepBindingError",
    "StepRegistry",
    "WorkflowBuilder",
    "WorkflowStep",
    "artifact",
    "apply_dedupe_policy",
    "combine_artifact_frames",
    "apply_join_policy",
    "apply_rename_policy",
    "evaluate_quality_gates",
    "load_pipeline_from_yaml",
    "ref",
    "step",
    "workflow",
    "const",
]
