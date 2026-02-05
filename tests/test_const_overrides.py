import textwrap

import pytest

from trakt.core.loader import load_pipeline_from_yaml
from trakt.core.overrides import apply_const_overrides
from trakt.runtime.local_runner import LocalRunner


def _write_scale_step(tmp_path, monkeypatch) -> None:
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


def test_apply_const_overrides_updates_bindings(tmp_path, monkeypatch) -> None:
    _write_scale_step(tmp_path, monkeypatch)

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text("id,amount\n1,10\n", encoding="utf-8")

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: const_override_demo
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: scale
                uses: steps.normalize.scale_amount
                with:
                  input: source__records
                  multiplier:
                    const: 3
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
    apply_const_overrides(pipeline, {"scale": {"multiplier": 2, "currency": "eur"}})

    runner = LocalRunner(input_dir=input_dir, output_dir=output_dir)
    result = runner.run(pipeline, run_id="const-override")

    assert result["status"] == "success"
    final_text = (output_dir / "final.csv").read_text(encoding="utf-8")
    assert "1,20,eur" in final_text


def test_apply_const_overrides_rejects_non_const_binding(tmp_path, monkeypatch) -> None:
    _write_scale_step(tmp_path, monkeypatch)

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: const_override_reject
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
    with pytest.raises(ValueError, match="not a const binding"):
        apply_const_overrides(pipeline, {"scale": {"multiplier": 2}})
