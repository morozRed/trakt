"""Shared test fixtures for Trakt test suite."""

import textwrap

import pytest


@pytest.fixture()
def step_dir(tmp_path, monkeypatch):
    """Create a steps/normalize/ directory with __init__.py files."""
    (tmp_path / "steps" / "normalize").mkdir(parents=True)
    (tmp_path / "steps" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "steps" / "normalize" / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    return tmp_path / "steps"


@pytest.fixture()
def make_step_module(step_dir):
    """Factory fixture that writes a step module file and returns its import path."""

    def _make(module_path: str, source: str) -> str:
        parts = module_path.split(".")
        parent = step_dir.parent
        for part in parts[:-1]:
            parent = parent / part
            parent.mkdir(exist_ok=True)
            init_file = parent / "__init__.py"
            if not init_file.exists():
                init_file.write_text("", encoding="utf-8")
        (parent / f"{parts[-1]}.py").write_text(
            textwrap.dedent(source).strip() + "\n",
            encoding="utf-8",
        )
        return module_path

    return _make


@pytest.fixture()
def make_pipeline_yaml(tmp_path):
    """Factory fixture that writes a pipeline.yaml and returns its Path."""

    def _make(content: str) -> "Path":
        from pathlib import Path

        pipeline_file = tmp_path / "pipeline.yaml"
        pipeline_file.write_text(
            textwrap.dedent(content).strip() + "\n",
            encoding="utf-8",
        )
        return pipeline_file

    return _make


@pytest.fixture()
def csv_input(tmp_path):
    """Create a sample CSV input directory with records.csv."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "records.csv").write_text(
        "id,amount\n1,10\n2,20\n", encoding="utf-8"
    )
    return input_dir
