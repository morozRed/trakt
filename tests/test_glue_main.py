import io
import json
import textwrap
from contextlib import redirect_stdout

import pytest

from trakt.runtime import glue_main


def test_parse_input_overrides_rejects_invalid_items() -> None:
    with pytest.raises(ValueError, match="Expected NAME=PATH"):
        glue_main._parse_input_overrides(["invalid"])


def test_glue_main_runs_pipeline_with_required_contract(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "double_amount.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input, output):
                frame = input.copy()
                frame["amount"] = frame["amount"] * 2
                return {"output": frame}

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
    (input_dir / "records").mkdir(parents=True)
    (input_dir / "records" / "part1.csv").write_text("id,amount\n1,10\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: glue_contract_demo
            inputs:
              source__records:
                uri: records/*.csv
            steps:
              - id: normalize
                uses: steps.normalize.double_amount
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

    stdout = io.StringIO()
    with redirect_stdout(stdout):
        glue_main.main(
            [
                "--pipeline-file",
                str(pipeline_file),
                "--client-id",
                "acme",
                "--batch-id",
                "batch-20260205",
                "--input-dir",
                str(input_dir),
                "--output-dir",
                str(output_dir),
                "--job-name",
                "trakt-glue-demo",
            ]
        )

    payload = json.loads(stdout.getvalue())
    assert payload["status"] == "success"
    assert payload["outputs"]["final"]["rows"] == 1
    assert (output_dir / "manifest.json").exists()
