from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trakt.core.artifacts import Artifact
from trakt.core.pipeline import Pipeline
from trakt.core.steps import Step
from trakt.io.adapters import (
    ArtifactAdapter,
    ArtifactAdapterRegistry,
    CsvArtifactAdapter,
)
from trakt.runtime.local_runner import LocalRunner


@dataclass(slots=True)
class CopyStep(Step):
    def run(self, ctx, **kwargs: Any) -> dict[str, Any]:
        return {"records_norm": kwargs["source__records"]}


class FancyAdapter(ArtifactAdapter):
    file_extension = ".fancy"

    def __init__(self) -> None:
        self.read_calls: list[list[Path]] = []
        self.write_calls: list[tuple[Any, str, str | None]] = []

    def read_many(self, paths: list[Path], *, artifact: Artifact) -> Any:
        self.read_calls.append(paths)
        return "loaded-by-fancy"

    def write(self, data: Any, uri: str, *, artifact_name: str | None = None) -> None:
        Path(uri).write_text(str(data), encoding="utf-8")
        self.write_calls.append((data, uri, artifact_name))


def test_registry_includes_builtin_csv_adapter() -> None:
    registry = ArtifactAdapterRegistry.with_defaults()
    adapter = registry.resolve("csv")
    assert isinstance(adapter, CsvArtifactAdapter)


def test_local_runner_dispatches_custom_adapter(tmp_path) -> None:
    fancy_adapter = FancyAdapter()
    registry = ArtifactAdapterRegistry.with_defaults()
    registry.register("fancy", fancy_adapter)

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "records.fancy").write_text("ignored", encoding="utf-8")

    pipeline = Pipeline(
        name="custom_adapter_pipeline",
        inputs={
            "source__records": Artifact(
                name="source__records",
                kind="fancy",
                uri="records.fancy",
            )
        },
        steps=[
            CopyStep(
                id="copy",
                inputs=["source__records"],
                outputs=["records_norm"],
            )
        ],
        outputs={"final": "records_norm"},
    )

    runner = LocalRunner(
        input_dir=input_dir,
        output_dir=output_dir,
        adapter_registry=registry,
        output_kind="fancy",
    )
    result = runner.run(pipeline, run_id="adapter-run")

    output_path = output_dir / "final.fancy"
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == "loaded-by-fancy"
    assert (
        fancy_adapter.read_calls and fancy_adapter.read_calls[0][0].name == "records.fancy"
    )
    assert (
        fancy_adapter.write_calls
        and fancy_adapter.write_calls[0][0] == "loaded-by-fancy"
    )
    assert result["outputs"]["final"]["path"] == str(output_path)
    assert result["outputs"]["final"]["kind"] == "fancy"
