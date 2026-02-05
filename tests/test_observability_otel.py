from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trakt.core.pipeline import Pipeline
from trakt.core.steps import Step
from trakt.runtime.runner_base import RunnerBase


class RecordingSpan:
    def __init__(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        self.name = name
        self.attributes = dict(attributes or {})
        self.events: list[tuple[str, dict[str, Any] | None]] = []

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        self.events.append((name, attributes))

    def end(self) -> None:
        return


class RecordingTracer:
    def __init__(self) -> None:
        self.spans: list[RecordingSpan] = []

    @contextmanager
    def start_as_current_span(
        self, name: str, attributes: dict[str, Any] | None = None
    ):
        span = RecordingSpan(name, attributes)
        self.spans.append(span)
        yield span


@dataclass(slots=True)
class WarningStep(Step):
    def run(self, ctx, **kwargs: Any) -> dict[str, Any]:
        ctx.emit_event("warning.missing_column", column="country")
        return {"records": [1, 2, 3]}


class InMemoryRunner(RunnerBase):
    def load_inputs(self, pipeline: Pipeline, ctx, **kwargs: Any) -> dict[str, Any]:
        return {}

    def write_outputs(
        self, pipeline: Pipeline, artifacts: dict[str, Any], ctx, **kwargs: Any
    ) -> dict[str, Any]:
        return {
            "final": {
                "path": "memory://final",
                "rows": len(artifacts["records"]),
            }
        }


def test_runner_creates_pipeline_and_step_spans(tmp_path) -> None:
    pipeline = Pipeline(
        name="otel_demo",
        steps=[WarningStep(id="warn_step", outputs=["records"])],
        outputs={"final": "records"},
    )
    tracer = RecordingTracer()
    runner = InMemoryRunner()

    result = runner.run(
        pipeline,
        run_id="otel-run",
        tracer=tracer,
        manifest_path=tmp_path / "manifest.json",
    )

    assert result["status"] == "success"
    assert [span.name for span in tracer.spans] == ["pipeline.run", "step.warn_step"]

    pipeline_span = tracer.spans[0]
    step_span = tracer.spans[1]
    assert pipeline_span.attributes["pipeline.name"] == "otel_demo"
    assert pipeline_span.attributes["status"] == "success"
    assert step_span.attributes["step.id"] == "warn_step"
    assert step_span.attributes["rows.out"] == 3
    assert step_span.events == [("warning.missing_column", {"column": "country"})]
    assert (tmp_path / "manifest.json").exists()
