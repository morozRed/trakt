import textwrap

import pytest

from trakt.core.loader import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner


def test_local_runner_executes_stream_pipeline(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "double_stream.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input, output):
                def _iter_chunks():
                    for chunk in input:
                        frame = chunk.copy()
                        frame["amount"] = frame["amount"] * 2
                        yield frame
                return {"output": _iter_chunks()}

            run.declared_inputs = ["input"]
            run.declared_outputs = ["output"]
            run.supports_batch = False
            run.supports_stream = True
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    (input_dir / "records").mkdir(parents=True)
    (input_dir / "records" / "part1.csv").write_text(
        "id,amount\n1,10\n2,30\n",
        encoding="utf-8",
    )
    (input_dir / "records" / "part2.csv").write_text(
        "id,amount\n3,5\n",
        encoding="utf-8",
    )

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: stream_demo
            execution:
              mode: stream
            inputs:
              source__records:
                uri: records/*.csv
                combine_strategy: concat
            steps:
              - id: normalize
                uses: steps.normalize.double_stream
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
    runner = LocalRunner(
        input_dir=input_dir,
        output_dir=output_dir,
        stream_chunk_size=1,
    )
    result = runner.run(pipeline, run_id="stream-run")

    assert result["status"] == "success"
    assert result["outputs"]["final"]["kind"] == "csv"
    final_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,20" in final_text
    assert "2,60" in final_text
    assert "3,10" in final_text


def test_stream_mode_rejects_non_concat_multi_file_inputs(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "pass_through.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input, output):
                return {"output": input}

            run.declared_inputs = ["input"]
            run.declared_outputs = ["output"]
            run.supports_stream = True
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
    (input_dir / "records" / "part2.csv").write_text("id,amount\n2,20\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: stream_non_concat
            execution:
              mode: stream
            inputs:
              source__records:
                uri: records/*.csv
                combine_strategy: union_by_name
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
    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir, stream_chunk_size=1)

    with pytest.raises(ValueError, match="combine_strategy='concat'"):
        runner.run(pipeline, run_id="stream-non-concat")
