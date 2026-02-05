from trakt.core.registry import StepRegistry
from trakt.core.workflow import WorkflowBuilder, workflow
from trakt.runtime.local_runner import LocalRunner


def test_workflow_builder_builds_pipeline_from_callable() -> None:
    def run(ctx, input, output):
        return {"output": input}

    run.declared_inputs = ["input"]
    run.declared_outputs = ["output"]
    run.supports_stream = True

    pipeline = (
        WorkflowBuilder(name="workflow_demo", execution_mode="stream")
        .input("source__records", uri="records.csv")
        .step(
            "normalize",
            run=run,
            with_={"input": "source__records", "output": "records_norm"},
        )
        .output("final", from_="records_norm")
        .build()
    )

    assert pipeline.execution_mode == "stream"
    assert pipeline.steps[0].uses.endswith(".run")
    assert pipeline.steps[0].supports_stream is True


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
            "normalize",
            uses="normalize.alias",
            with_={"source": "source__records", "target": "records_norm"},
        )
        .output("final", from_="records_norm")
        .build()
    )

    assert pipeline.steps[0].uses == "normalize.alias"
    assert pipeline.steps[0].inputs == ["source__records"]


def test_workflow_builder_run_executes_with_local_runner(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,5\n", encoding="utf-8")

    def run(ctx, input, output):
        frame = input.copy()
        frame["amount"] = frame["amount"] * 2
        return {"output": frame}

    run.declared_inputs = ["input"]
    run.declared_outputs = ["output"]

    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = (
        workflow("workflow_run")
        .input("source__records", uri="records.csv")
        .step(
            "double_amount",
            run=run,
            with_={"input": "source__records", "output": "records_norm"},
        )
        .output("final", from_="records_norm")
        .run(runner, run_id="workflow-run")
    )

    assert result["status"] == "success"
    assert result["run_id"] == "workflow-run"
    assert (output_dir / "final.csv").exists()
    assert "1,10" in (output_dir / "final.csv").read_text(encoding="utf-8")
