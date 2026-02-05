import json
import textwrap

from trakt.core.loader import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner


def test_local_runner_executes_pipeline_end_to_end(tmp_path, monkeypatch) -> None:
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
    (input_dir / "records" / "part2.csv").write_text("id,amount\n2,30\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: integration_demo
            inputs:
              source__records:
                uri: records/*.csv
                combine_strategy: concat
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

    pipeline = load_pipeline_from_yaml(pipeline_file)
    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = runner.run(pipeline, run_id="integration-run")

    assert result["status"] == "success"
    assert result["outputs"]["final"]["rows"] == 2
    assert (output_dir / "manifest.json").exists()

    final_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,20" in final_text
    assert "2,60" in final_text

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["pipeline"]["name"] == "integration_demo"


def test_local_runner_passes_const_literal_and_numeric_config(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "scale_amount.py").write_text(
        textwrap.dedent(
            """
            def run(ctx, input, multiplier, currency):
                frame = input.copy()
                frame["amount"] = frame["amount"] * multiplier
                frame["currency"] = currency
                return {"output": frame}

            run.declared_inputs = ["input", "multiplier", "currency"]
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
    (input_dir / "records.csv").write_text("id,amount\n1,10\n2,30\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: const_config_demo
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: scale
                uses: steps.normalize.scale_amount
                with:
                  input: source__records
                  multiplier: 3
                  currency:
                    const: usd
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
    result = runner.run(pipeline, run_id="const-config-run")

    assert result["status"] == "success"
    final_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,30,usd" in final_text
    assert "2,90,usd" in final_text


def test_local_runner_honors_per_output_config(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "pass_through.py").write_text(
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
    (input_dir / "records.csv").write_text("id,amount\n1,10\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: output_config_demo
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: pass
                uses: steps.pass_through
                with:
                  input: source__records
                  output: records_norm
            outputs:
              datasets:
                - name: final
                  from: records_norm
                  kind: csv
                  uri: custom/final_pipe.csv
                  metadata:
                    delimiter: "|"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    pipeline = load_pipeline_from_yaml(pipeline_file)
    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = runner.run(pipeline, run_id="output-config-run")

    assert result["status"] == "success"
    output_path = output_dir / "custom" / "final_pipe.csv"
    assert output_path.exists()
    assert "id|amount" in output_path.read_text(encoding="utf-8")
    assert result["outputs"]["final"]["path"] == str(output_path)


def test_local_runner_csv_delimiter_autodetect(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "copy.py").write_text(
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
    (input_dir / "records.psv").write_text("id|amount\n1|10\n2|20\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: delimiter_auto_demo
            inputs:
              source__records:
                uri: records.psv
                metadata:
                  delimiter: auto
            steps:
              - id: copy
                uses: steps.normalize.copy
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
    result = runner.run(pipeline, run_id="delimiter-auto-run")

    assert result["status"] == "success"
    final_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,10" in final_text
    assert "2,20" in final_text


def test_local_runner_csv_read_options_block_is_supported(tmp_path, monkeypatch) -> None:
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "copy.py").write_text(
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
    (input_dir / "records.psv").write_text("id|amount\n1|10\n2|20\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: read_options_demo
            inputs:
              source__records:
                uri: records.psv
                metadata:
                  read_options:
                    delimiter: "|"
            steps:
              - id: copy
                uses: steps.normalize.copy
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
    result = runner.run(pipeline, run_id="read-options-run")

    assert result["status"] == "success"
    final_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,10" in final_text
    assert "2,20" in final_text
