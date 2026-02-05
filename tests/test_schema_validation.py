import textwrap

import pytest

from trakt.core.loader import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner


def _write_pass_through_step(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "pass_through.py").write_text(
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


def test_schema_validation_rejects_column_mismatch(tmp_path, monkeypatch) -> None:
    _write_pass_through_step(tmp_path, monkeypatch)

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: schema_columns_mismatch
            inputs:
              source__records:
                uri: records.csv
                schema:
                  columns: [id, amount, currency]
            steps:
              - id: normalize
                uses: steps.normalize.pass_through
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

    with pytest.raises(ValueError, match="schema columns mismatch"):
        runner.run(pipeline, run_id="schema-columns-mismatch")


def test_schema_validation_rejects_dtype_mismatch(tmp_path, monkeypatch) -> None:
    _write_pass_through_step(tmp_path, monkeypatch)

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10.5\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: schema_dtype_mismatch
            inputs:
              source__records:
                uri: records.csv
                schema:
                  id: int64
                  amount: int64
            steps:
              - id: normalize
                uses: steps.normalize.pass_through
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

    with pytest.raises(ValueError, match="schema dtypes mismatch"):
        runner.run(pipeline, run_id="schema-dtypes-mismatch")
