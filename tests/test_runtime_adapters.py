import json
from dataclasses import dataclass
from typing import Any

from trakt.core.artifacts import Artifact
from trakt.core.pipeline import Pipeline
from trakt.core.steps import Step
from trakt.runtime.glue_runner import GlueRunner
from trakt.runtime.lambda_runner import LambdaRunner


@dataclass(slots=True)
class CopyStep(Step):
    def run(self, ctx, **kwargs: Any) -> dict[str, Any]:
        return {"records_norm": kwargs["source__records"]}


def _build_pipeline() -> Pipeline:
    return Pipeline(
        name="runtime_adapter_demo",
        inputs={
            "source__records": Artifact(
                name="source__records",
                kind="csv",
                uri="records.csv",
            )
        },
        steps=[
            CopyStep(
                id="copy",
                inputs=["source__records"],
                outputs=["records_norm"],
            )
        ],
        outputs={"final": "records_norm"},
    )


def test_glue_runner_executes_with_local_parity(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10\n2,20\n", encoding="utf-8")

    result = GlueRunner(
        input_dir=input_dir,
        output_dir=output_dir,
        job_name="daily-travel-job",
    ).run(_build_pipeline(), run_id="glue-run")

    assert result["status"] == "success"
    assert (output_dir / "final.csv").exists()
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["runner"] == "GlueRunner"


def test_lambda_runner_executes_when_input_is_within_limit(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10\n2,20\n", encoding="utf-8")

    result = LambdaRunner(
        input_dir=input_dir,
        output_dir=output_dir,
        max_batch_rows=5,
    ).run(_build_pipeline(), run_id="lambda-run")

    assert result["status"] == "success"
    assert (output_dir / "final.csv").exists()


def test_lambda_runner_rejects_inputs_over_limit(tmp_path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text(
        "id,amount\n1,10\n2,20\n3,30\n",
        encoding="utf-8",
    )

    try:
        LambdaRunner(
            input_dir=input_dir,
            output_dir=output_dir,
            max_batch_rows=2,
        ).run(_build_pipeline(), run_id="lambda-too-big")
    except ValueError as exc:
        assert "max_batch_rows" in str(exc)
    else:
        raise AssertionError("Expected LambdaRunner to reject oversized input.")
