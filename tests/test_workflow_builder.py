import pytest

from trakt.core.registry import StepRegistry
from trakt.core.workflow import WorkflowBuilder, step, workflow
from trakt.runtime.local_runner import LocalRunner


def test_workflow_builder_builds_pipeline_from_step_specs() -> None:
    def normalize(ctx, input, output):
        return {"output": input}

    normalize.declared_inputs = ["input"]
    normalize.declared_outputs = ["output"]
    normalize.supports_stream = True

    pipeline = (
        WorkflowBuilder(name="workflow_demo", execution_mode="stream")
        .input("source__records", uri="records.csv")
        .step(
            step("normalize", run=normalize).bind(
                input="source__records",
                output="records_norm",
            )
        )
        .output("final", from_="records_norm")
        .build()
    )

    assert pipeline.execution_mode == "stream"
    assert pipeline.steps[0].uses.endswith(".normalize")
    assert pipeline.steps[0].supports_stream is True


def test_workflow_builder_steps_method_appends_multiple_steps() -> None:
    def normalize(ctx, input, output):
        return {"output": input}

    def enrich(ctx, input, output):
        return {"output": input}

    normalize.declared_inputs = ["input"]
    normalize.declared_outputs = ["output"]
    enrich.declared_inputs = ["input"]
    enrich.declared_outputs = ["output"]

    pipeline = (
        workflow("workflow_steps")
        .input("source__records", uri="records.csv")
        .steps(
            [
                step("normalize", run=normalize).bind(
                    input="source__records",
                    output="records_norm",
                ),
                step("enrich", run=enrich).bind(
                    input="records_norm",
                    output="records_enriched",
                ),
            ]
        )
        .output("final", from_="records_enriched")
        .build()
    )

    assert [resolved.id for resolved in pipeline.steps] == ["normalize", "enrich"]


def test_workflow_builder_resolves_registry_alias() -> None:
    def run(ctx, source, target):
        return {"target": source}

    run.declared_inputs = ["source"]
    run.declared_outputs = ["target"]

    registry = StepRegistry()
    registry.register("normalize.alias", run)

    pipeline = (
        workflow("workflow_alias", registry=registry)
        .input("source__records", uri="records.csv")
        .step(
            step("normalize", uses="normalize.alias").bind(
                source="source__records",
                target="records_norm",
            )
        )
        .output("final", from_="records_norm")
        .build()
    )

    assert pipeline.steps[0].uses == "normalize.alias"
    assert pipeline.steps[0].inputs == ["source__records"]


def test_workflow_builder_rejects_non_step_argument() -> None:
    with pytest.raises(TypeError, match="expects a WorkflowStep"):
        workflow("invalid").step("not-a-step")  # type: ignore[arg-type]


def test_workflow_builder_run_executes_with_local_runner(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,5\n", encoding="utf-8")

    def double_amount(ctx, input, output):
        frame = input.copy()
        frame["amount"] = frame["amount"] * 2
        return {"output": frame}

    double_amount.declared_inputs = ["input"]
    double_amount.declared_outputs = ["output"]

    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = (
        workflow("workflow_run")
        .input("source__records", uri="records.csv")
        .steps(
            [
                step("double_amount", run=double_amount).bind(
                    input="source__records",
                    output="records_norm",
                )
            ]
        )
        .output("final", from_="records_norm")
        .run(runner, run_id="workflow-run")
    )

    assert result["status"] == "success"
    assert result["run_id"] == "workflow-run"
    assert (output_dir / "final.csv").exists()
    assert "1,10" in (output_dir / "final.csv").read_text(encoding="utf-8")
