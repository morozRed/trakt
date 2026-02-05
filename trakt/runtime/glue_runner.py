"""AWS Glue runtime adapter."""

from pathlib import Path
from typing import Any

from trakt.core.pipeline import Pipeline
from trakt.runtime.local_runner import LocalRunner


class GlueRunner(LocalRunner):
    """Execute pipelines with Glue-compatible runtime metadata."""

    def __init__(
        self,
        *,
        input_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        input_overrides: dict[str, str] | None = None,
        job_name: str | None = None,
    ) -> None:
        super().__init__(
            input_dir=input_dir,
            output_dir=output_dir,
            input_overrides=input_overrides,
        )
        self.job_name = job_name

    def run(self, pipeline: Pipeline, **kwargs: Any) -> dict[str, Any]:
        context_metadata = dict(kwargs.pop("context_metadata", {}))
        context_metadata["runtime"] = "glue"
        if self.job_name:
            context_metadata["glue.job_name"] = self.job_name

        return super().run(
            pipeline,
            context_metadata=context_metadata,
            **kwargs,
        )
