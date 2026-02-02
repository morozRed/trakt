import json
import textwrap

import pytest

from trakt.core.loader import load_pipeline_from_yaml
from trakt.observability.manifest import write_manifest
from trakt.runtime.local_runner import LocalRunner


def test_write_manifest_persists_json(tmp_path) -> None:
    path = tmp_path / "out" / "manifest.json"
    payload = {"run_id": "test", "status": "success"}

    write_manifest(str(path), payload)

    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == payload


def test_runner_writes_manifest_even_on_failure(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "explode.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input, output):
                raise ValueError("boom")

            run.declared_inputs = ["input"]
            run.declared_outputs = ["output"]
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: failing_pipeline
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: explode
                uses: steps.normalize.explode
                with:
                  input: source__records
                  output: records_norm
            outputs:
              datasets:
                - name: final
                  from: records_norm
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    pipeline = load_pipeline_from_yaml(pipeline_file)
    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)

    with pytest.raises(ValueError, match="boom"):
        runner.run(pipeline, run_id="failure-run")

    manifest_path = output_dir / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_id"] == "failure-run"
    assert manifest["status"] == "failed"
    assert manifest["error"]["type"] == "ValueError"
