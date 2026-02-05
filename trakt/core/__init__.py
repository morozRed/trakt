"""Core domain types for Trakt."""

from trakt.core.artifacts import Artifact, CombineStrategy, combine_artifact_frames
from trakt.core.context import Context
from trakt.core.loader import PipelineLoadError, load_pipeline_from_yaml
from trakt.core.policies import (
    DedupePolicy,
    JoinPolicy,
    RenamePolicy,
    apply_dedupe_policy,
    apply_join_policy,
    apply_rename_policy,
)
from trakt.core.pipeline import Pipeline, PipelineValidationError
from trakt.core.registry import StepRegistry
from trakt.core.steps import ResolvedStep, Step, StepBindingError
from trakt.core.workflow import (
    WorkflowArtifact,
    WorkflowBuilder,
    WorkflowStep,
    artifact,
    step,
    workflow,
)

__all__ = [
    "Artifact",
    "CombineStrategy",
    "Context",
    "DedupePolicy",
    "JoinPolicy",
    "WorkflowArtifact",
    "Pipeline",
    "PipelineLoadError",
    "PipelineValidationError",
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
    "load_pipeline_from_yaml",
    "step",
    "workflow",
]
