from pathlib import Path

from trakt.core.loader import load_pipeline_from_yaml
from trakt.runtime.glue_runner import GlueRunner


def test_glue_smoke_example_pipeline_runs(tmp_path, monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    demo_root = repo_root / "examples" / "glue_smoke"
    monkeypatch.syspath_prepend(str(demo_root))

    pipeline = load_pipeline_from_yaml(demo_root / "pipeline.yaml")
    output_dir = tmp_path / "glue-smoke-output"
    result = GlueRunner(
        input_dir=demo_root / "input",
        output_dir=output_dir,
        job_name="glue-smoke-job",
    ).run(
        pipeline,
        run_id="glue-smoke-run",
        context_metadata={"client_id": "demo", "batch_id": "smoke-20260205"},
    )

    assert result["status"] == "success"
    assert result["outputs"]["smoke_result"]["rows"] == 3

    output_text = (output_dir / "smoke_result.csv").read_text(encoding="utf-8")
    assert "USD" in output_text
    assert "EUR" in output_text
    assert (output_dir / "manifest.json").exists()
