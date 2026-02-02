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
