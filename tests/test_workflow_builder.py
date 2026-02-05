import pytest

from trakt.core.artifacts import Artifact
from trakt.core.bindings import get_const_binding_value, is_const_binding
from trakt.core.registry import StepRegistry
from trakt.core.workflow import artifact, const, ref, step, workflow
from trakt.runtime.local_runner import LocalRunner


def test_workflow_builder_builds_pipeline_from_step_specs() -> None:
    def normalize(ctx, input):
        return {"output": input}

    normalize.declared_inputs = ["input"]
    normalize.declared_outputs = ["output"]
    normalize.supports_stream = True

    pipeline = (
        workflow("workflow_demo", execution_mode="stream")
        .source(artifact("source__records").at("records.csv"))
        .step(
            step("normalize", run=normalize)
            .bind_input(artifact("source__records").at("records.csv"))
            .bind_output("records_norm")
        )
        .output("final", from_="records_norm")
        .build()
    )

    assert pipeline.execution_mode == "stream"
    assert pipeline.steps[0].uses.endswith(".normalize")
    assert pipeline.steps[0].supports_stream is True


def test_workflow_builder_steps_method_appends_multiple_steps() -> None:
    def normalize(ctx, input):
        return {"output": input}

    def enrich(ctx, input):
        return {"output": input}

    normalize.declared_inputs = ["input"]
    normalize.declared_outputs = ["output"]
    enrich.declared_inputs = ["input"]
    enrich.declared_outputs = ["output"]

    pipeline = (
        workflow("workflow_steps")
        .source(artifact("source__records").at("records.csv"))
        .steps(
            [
                step("normalize", run=normalize)
                .bind_input(artifact("source__records"))
                .bind_output("records_norm"),
                step("enrich", run=enrich)
                .bind_input(artifact("records_norm"))
                .bind_output("records_enriched"),
            ]
        )
        .output("final", from_="records_enriched")
        .build()
    )

    assert [resolved.id for resolved in pipeline.steps] == ["normalize", "enrich"]


def test_workflow_builder_resolves_registry_alias() -> None:
    def run(ctx, source):
        return {"target": source}

    run.declared_inputs = ["source"]
    run.declared_outputs = ["target"]

    registry = StepRegistry()
    registry.register("normalize.alias", run)

    pipeline = (
        workflow("workflow_alias", registry=registry)
        .source(artifact("source__records").at("records.csv"))
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


def test_workflow_builder_supports_multiple_workflow_inputs(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10\n2,20\n", encoding="utf-8")
    (input_dir / "countries.csv").write_text("id,country\n1,US\n2,DE\n", encoding="utf-8")

    def join_inputs(ctx, inputs):
        records, countries = inputs
        joined = records.merge(countries, on="id", how="left")
        return {"output": joined}

    join_inputs.declared_inputs = ["inputs"]
    join_inputs.declared_outputs = ["output"]

    input_1 = artifact("source__records").at("records.csv")
    input_2 = artifact("source__countries").at("countries.csv")

    result = (
        workflow("workflow_multi_input")
        .sources([input_1, input_2])
        .step(
            step("join_inputs", run=join_inputs)
            .bind_inputs(input_1, input_2)
            .bind_output("records_joined")
        )
        .output("final", from_="records_joined")
        .run(LocalRunner(input_dir=input_dir, output_dir=output_dir), run_id="multi-input")
    )

    assert result["status"] == "success"
    output_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "US" in output_text
    assert "DE" in output_text


def test_workflow_builder_accepts_core_artifact_objects() -> None:
    pipeline = (
        workflow("artifact_object")
        .source(Artifact(name="source__records", kind="csv", uri="records.csv"))
        .build()
    )
    assert "source__records" in pipeline.inputs


def test_workflow_step_bind_normalizes_artifact_values() -> None:
    input_1 = artifact("source__records").at("records.csv")
    output_1 = Artifact(name="records_norm", kind="csv", uri="records_norm.csv")

    spec = step("normalize", uses="normalize.alias").bind(
        input=input_1,
        output=output_1,
        inputs=[input_1, "source__fallback"],
    )

    assert spec.bindings["input"] == "source__records"
    assert spec.bindings["output"] == "records_norm"
    assert spec.bindings["inputs"] == ["source__records", "source__fallback"]


def test_workflow_step_helpers_separate_refs_from_params() -> None:
    spec = (
        step("normalize", uses="normalize.alias")
        .input(input=ref("source__records"), lookup=artifact("source__lookup"))
        .params(currency="usd", multiplier=2, options={"mode": "strict"})
        .output(output=ref("records_norm"))
    )

    assert spec.bindings["input"] == "source__records"
    assert spec.bindings["lookup"] == "source__lookup"
    assert spec.bindings["output"] == "records_norm"
    assert is_const_binding(spec.bindings["currency"]) is True
    assert get_const_binding_value(spec.bindings["currency"]) == "usd"
    assert get_const_binding_value(spec.bindings["multiplier"]) == 2
    assert get_const_binding_value(spec.bindings["options"]) == {"mode": "strict"}


def test_workflow_step_in_helper_rejects_literal_numbers() -> None:
    with pytest.raises(TypeError, match="Use .params"):
        step("normalize", uses="normalize.alias").in_(input=123)  # type: ignore[arg-type]


def test_workflow_step_input_output_aliases_match_existing_helpers() -> None:
    source = artifact("source__records")
    spec = (
        step("normalize", uses="normalize.alias")
        .input(input=source)
        .params(currency="usd")
        .output(output=ref("records_norm"))
    )

    assert spec.bindings["input"] == "source__records"
    assert get_const_binding_value(spec.bindings["currency"]) == "usd"
    assert spec.bindings["output"] == "records_norm"


def test_ref_helper_accepts_artifact_values() -> None:
    source = artifact("source__records").at("records.csv")
    assert ref("records_norm").name == "records_norm"
    assert ref(source).name == "source__records"
    assert ref(Artifact(name="records_checked", kind="csv", uri="records_checked.csv")).name == "records_checked"


def test_workflow_builder_rejects_non_step_argument() -> None:
    with pytest.raises(TypeError, match="expects a WorkflowStep"):
        workflow("invalid").step("not-a-step")  # type: ignore[arg-type]


def test_workflow_step_rejects_invalid_artifact_reference() -> None:
    with pytest.raises(TypeError, match="expects str/WorkflowRef/WorkflowArtifact/Artifact"):
        step("x", uses="normalize.alias").bind_input(123)  # type: ignore[arg-type]


def test_workflow_builder_run_executes_with_local_runner(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,5\n", encoding="utf-8")

    def double_amount(ctx, input):
        frame = input.copy()
        frame["amount"] = frame["amount"] * 2
        return {"output": frame}

    double_amount.declared_inputs = ["input"]
    double_amount.declared_outputs = ["output"]

    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = (
        workflow("workflow_run")
        .source(artifact("source__records").at("records.csv"))
        .steps(
            [
                step("double_amount", run=double_amount)
                .bind_input("source__records")
                .bind_output("records_norm")
            ]
        )
        .output("final", from_="records_norm")
        .run(runner, run_id="workflow-run")
    )

    assert result["status"] == "success"
    assert result["run_id"] == "workflow-run"
    assert (output_dir / "final.csv").exists()
    assert "1,10" in (output_dir / "final.csv").read_text(encoding="utf-8")


def test_workflow_builder_supports_const_literal_bindings(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,5\n", encoding="utf-8")

    def add_currency(ctx, input, currency):
        frame = input.copy()
        frame["currency"] = currency
        return {"output": frame}

    add_currency.declared_inputs = ["input", "currency"]
    add_currency.declared_outputs = ["output"]

    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = (
        workflow("workflow_const")
        .source(artifact("source__records").at("records.csv"))
        .step(
            step("add_currency", run=add_currency).bind(
                input="source__records",
                currency=const("usd"),
                output="records_norm",
            )
        )
        .output("final", from_="records_norm")
        .run(runner, run_id="workflow-const")
    )

    assert result["status"] == "success"
    output_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,5,usd" in output_text


def test_workflow_builder_params_helper_wraps_literal_strings(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,5\n", encoding="utf-8")

    def add_currency(ctx, input, currency):
        frame = input.copy()
        frame["currency"] = currency
        return {"output": frame}

    add_currency.declared_inputs = ["input", "currency"]
    add_currency.declared_outputs = ["output"]

    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = (
        workflow("workflow_params")
        .source(artifact("source__records").at("records.csv"))
        .step(
            step("add_currency", run=add_currency)
            .input(input=ref("source__records"))
            .params(currency="usd")
            .output(output=ref("records_norm"))
        )
        .output("final", from_="records_norm")
        .run(runner, run_id="workflow-params")
    )

    assert result["status"] == "success"
    output_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,5,usd" in output_text


def test_workflow_builder_output_supports_per_dataset_config() -> None:
    pipeline = (
        workflow("workflow_output_config")
        .source(artifact("source__records").at("records.csv"))
        .output(
            "final",
            from_="source__records",
            kind="csv",
            uri="custom/final_pipe.csv",
            metadata={"delimiter": "|"},
        )
        .build()
    )

    output = pipeline.outputs["final"]
    assert output.source == "source__records"
    assert output.kind == "csv"
    assert output.uri == "custom/final_pipe.csv"
    assert output.metadata["delimiter"] == "|"
