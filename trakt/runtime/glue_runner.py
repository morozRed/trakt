"""AWS Glue runtime adapter."""

from typing import Any

from trakt.core.pipeline import Pipeline
from trakt.runtime.runner_base import RunnerBase


class GlueRunner(RunnerBase):
    """Execute pipelines within AWS Glue."""

    def run(self, pipeline: Pipeline, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError("Glue runner is not implemented yet.")
