import sys
import textwrap
import types

import pytest

from trakt.core.loader import PipelineLoadError, load_pipeline_from_yaml
from trakt.core.registry import StepRegistry


def _build_direct_module() -> None:
    def run(ctx, input):
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
    def run(ctx, source):
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


def test_loader_parses_execution_mode_and_step_capabilities(tmp_path) -> None:
    def run(ctx, source):
        return {"target": source}

    run.declared_inputs = ["source"]
    run.declared_outputs = ["target"]
    run.supports_stream = True

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: stream_module
            execution:
              mode: stream
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: normalize
                uses: normalize.stream
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
    registry.register("normalize.stream", run)
    pipeline = load_pipeline_from_yaml(pipeline_file, registry=registry)
    assert pipeline.execution_mode == "stream"
    assert pipeline.steps[0].supports_stream is True


def test_loader_rejects_conflicting_execution_modes(tmp_path) -> None:
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: conflicting_mode
            execution_mode: batch
            execution:
              mode: stream
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

    _build_direct_module()
    with pytest.raises(PipelineLoadError, match="conflicting execution modes"):
        load_pipeline_from_yaml(pipeline_file)


def test_loader_supports_yaml_const_literal_binding(tmp_path) -> None:
    def run(ctx, source, currency):
        return {"target": source}

    run.declared_inputs = ["source", "currency"]
    run.declared_outputs = ["target"]

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: const_literal
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: normalize
                uses: normalize.const
                with:
                  source: source__records
                  currency:
                    const: usd
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
    registry.register("normalize.const", run)
    pipeline = load_pipeline_from_yaml(pipeline_file, registry=registry)
    assert pipeline.steps[0].inputs == ["source__records"]


def test_loader_requires_const_wrapper_for_literal_strings(tmp_path) -> None:
    def run(ctx, source, currency):
        return {"target": source}

    run.declared_inputs = ["source", "currency"]
    run.declared_outputs = ["target"]

    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: missing_const_literal
            inputs:
              source__records:
                uri: records.csv
            steps:
              - id: normalize
                uses: normalize.const
                with:
                  source: source__records
                  currency: usd
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
    registry.register("normalize.const", run)
    with pytest.raises(PipelineLoadError, match=r"missing inputs=.*normalize:usd"):
        load_pipeline_from_yaml(pipeline_file, registry=registry)


def test_loader_parses_per_output_dataset_config(tmp_path) -> None:
    _build_direct_module()
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(
        textwrap.dedent(
            """
            name: output_config
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
                  kind: csv
                  uri: exports/final_pipe.csv
                  metadata:
                    delimiter: "|"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    pipeline = load_pipeline_from_yaml(pipeline_file)
    output = pipeline.outputs["final"]
    assert output.source == "records_norm"
    assert output.kind == "csv"
    assert output.uri == "exports/final_pipe.csv"
    assert output.metadata["delimiter"] == "|"
