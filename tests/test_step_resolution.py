import sys
import textwrap
import types

from trakt.core.loader import load_pipeline_from_yaml
from trakt.core.registry import StepRegistry


def _build_direct_module() -> None:
    def run(ctx, input, output):
        return {"output": input}

    run.declared_inputs = ["input"]
    run.declared_outputs = ["output"]

    steps_pkg = types.ModuleType("steps")
    normalize_pkg = types.ModuleType("steps.normalize")
    demo_module = types.ModuleType("steps.normalize.demo")
    demo_module.run = run

    sys.modules["steps"] = steps_pkg
    sys.modules["steps.normalize"] = normalize_pkg
    sys.modules["steps.normalize.demo"] = demo_module


def test_loader_resolves_direct_module_path(tmp_path) -> None:
    _build_direct_module()
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: direct_module
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: normalize
                uses: steps.normalize.demo
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
    assert pipeline.steps[0].id == "normalize"
    assert pipeline.steps[0].inputs == ["source__records"]
    assert pipeline.steps[0].outputs == ["records_norm"]


def test_loader_resolves_registry_alias(tmp_path) -> None:
    def run(ctx, source, target):
        return {"target": source}

    run.declared_inputs = ["source"]
    run.declared_outputs = ["target"]

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: alias_module
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: normalize
                uses: normalize.alias
                with:
                  source: source__records
                  target: records_norm
            outputs:
              datasets:
                - name: final
                  from: records_norm
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    registry = StepRegistry()
    registry.register("normalize.alias", run)
    pipeline = load_pipeline_from_yaml(pipeline_file, registry=registry)
    assert pipeline.steps[0].id == "normalize"
    assert pipeline.steps[0].inputs == ["source__records"]
