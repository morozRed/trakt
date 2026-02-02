from dataclasses import dataclass

import pytest

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
