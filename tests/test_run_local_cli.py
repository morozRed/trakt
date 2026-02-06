"""Tests for the run_local.py CLI entrypoint."""

import io
import json
import textwrap
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest


def test_run_local_executes_pipeline_from_cli(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "double_amount.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input):
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
            name: run_local_demo
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

    from trakt.run_local import main

    stdout = io.StringIO()
    with redirect_stdout(stdout):
        with patch(
            "sys.argv",
            [
                "trakt-run-local",
                "--pipeline-file",
                str(pipeline_file),
                "--input-dir",
                str(input_dir),
                "--output-dir",
                str(output_dir),
                "--lenient",
            ],
        ):
            main()

    payload = json.loads(stdout.getvalue())
    assert payload["status"] == "success"
    assert payload["outputs"]["final"]["rows"] == 1
    assert (output_dir / "manifest.json").exists()


def test_run_local_raises_on_missing_pipeline_file() -> None:
    from trakt.run_local import main

    with pytest.raises(ValueError, match="--pipeline or --pipeline-file"):
        with patch("sys.argv", ["trakt-run-local"]):
            main()
