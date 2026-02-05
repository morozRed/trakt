import json
import textwrap

import pytest

from trakt.core.loader import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner


def test_quality_gate_step_warn_mode_persists_metrics(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "copy.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input):
                return {"output": input}

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
    (input_dir / "records.csv").write_text("id,amount\n1,10\n1,\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: quality_warn_pipeline
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: copy
                uses: steps.copy
                with:
                  input: source__records
                  output: records_norm
              - id: quality
                uses: trakt.steps.quality_gate
                with:
                  input: records_norm
                  policy:
                    const:
                      mode: warn
                      required_columns: [id, country]
                      unique_keys: [id]
                      max_null_ratio:
                        amount: 0.2
                  output: records_checked
            outputs:
              datasets:
                - name: final
                  from: records_checked
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    pipeline = load_pipeline_from_yaml(pipeline_file)
    result = LocalRunner(input_dir=input_dir, output_dir=output_dir).run(
        pipeline, run_id="quality-warn"
    )
    assert result["status"] == "success"

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    quality_step = next(step for step in manifest["steps"] if step["step_id"] == "quality")
    assert quality_step["metrics"]["quality_warnings"] == 3
    assert quality_step["metrics"]["quality_violations"] == 3


def test_quality_gate_step_fail_mode_stops_pipeline(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "copy.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input):
                return {"output": input}

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
    (input_dir / "records.csv").write_text("id,amount\n1,10\n1,20\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: quality_fail_pipeline
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: copy
                uses: steps.copy
                with:
                  input: source__records
                  output: records_norm
              - id: quality
                uses: trakt.steps.quality_gate
                with:
                  input: records_norm
                  policy:
                    const:
                      mode: fail
                      unique_keys: [id]
                  output: records_checked
            outputs:
              datasets:
                - name: final
                  from: records_checked
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    pipeline = load_pipeline_from_yaml(pipeline_file)
    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    with pytest.raises(ValueError, match="duplicate rows"):
        runner.run(pipeline, run_id="quality-fail")
