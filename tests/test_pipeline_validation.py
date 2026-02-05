from dataclasses import dataclass

import pytest

from trakt.core.artifacts import Artifact, OutputDataset
from trakt.core.pipeline import Pipeline, PipelineValidationError
from trakt.core.steps import Step


@dataclass(slots=True)
class DummyStep(Step):
    def run(self, ctx, **kwargs):
        return {}


def test_pipeline_validation_passes_for_valid_wiring() -> None:
    pipeline = Pipeline(
        name="valid",
        inputs={},
        steps=[
            DummyStep(id="s1", outputs=["records"]),
            DummyStep(id="s2", inputs=["records"], outputs=["final"]),
        ],
        outputs={"dataset": "final"},
    )

    pipeline.validate()


def test_pipeline_validation_reports_all_errors() -> None:
    pipeline = Pipeline(
        name="invalid",
        inputs={},
        steps=[DummyStep(id="s1", inputs=["missing"], outputs=["dup"]), DummyStep(id="s2", outputs=["dup"])],
        outputs={"dataset": "unknown"},
    )

    with pytest.raises(PipelineValidationError) as exc_info:
        pipeline.validate()

    error = exc_info.value
    assert error.missing_inputs == [("s1", "missing")]
    assert error.output_collisions == [("s2", "dup", "s1")]
    assert error.unknown_output_bindings == [("dataset", "unknown")]


def test_pipeline_validation_rejects_invalid_execution_mode() -> None:
    pipeline = Pipeline(
        name="invalid_mode",
        execution_mode="realtime",
        inputs={},
        steps=[DummyStep(id="s1", outputs=["records"])],
        outputs={"dataset": "records"},
    )

    with pytest.raises(PipelineValidationError) as exc_info:
        pipeline.validate()

    assert exc_info.value.invalid_execution_mode == "realtime"


def test_pipeline_validation_rejects_incompatible_stream_steps() -> None:
    pipeline = Pipeline(
        name="stream_incompatible",
        execution_mode="stream",
        inputs={},
        steps=[DummyStep(id="s1", outputs=["records"])],
        outputs={"dataset": "records"},
    )

    with pytest.raises(PipelineValidationError) as exc_info:
        pipeline.validate()

    assert exc_info.value.incompatible_steps == [("s1", "stream")]


def test_pipeline_validation_accepts_stream_capable_step() -> None:
    pipeline = Pipeline(
        name="stream_ok",
        execution_mode="stream",
        inputs={},
        steps=[
            DummyStep(
                id="s1",
                outputs=["records"],
                supports_batch=False,
                supports_stream=True,
            )
        ],
        outputs={"dataset": "records"},
    )

    pipeline.validate()


def test_pipeline_validation_rejects_stream_non_concat_inputs() -> None:
    pipeline = Pipeline(
        name="stream_non_concat_inputs",
        execution_mode="stream",
        inputs={
            "source__records": Artifact(
                name="source__records",
                kind="csv",
                uri="records/*.csv",
                combine_strategy="union_by_name",
            )
        },
        steps=[
            DummyStep(
                id="s1",
                inputs=["source__records"],
                outputs=["records"],
                supports_batch=False,
                supports_stream=True,
            )
        ],
        outputs={"dataset": "records"},
    )
    with pytest.raises(PipelineValidationError) as exc_info:
        pipeline.validate()

    assert exc_info.value.incompatible_inputs == [
        ("source__records", "combine_strategy=union_by_name")
    ]


def test_pipeline_validation_supports_output_dataset_objects() -> None:
    pipeline = Pipeline(
        name="output_objects",
        inputs={},
        steps=[DummyStep(id="s1", outputs=["records"])],
        outputs={
            "dataset": OutputDataset(
                name="dataset",
                source="records",
                kind="csv",
                uri="custom/records.csv",
            )
        },
    )

    pipeline.validate()
