"""AWS Lambda runtime adapter."""

from typing import Any

from trakt.core.pipeline import Pipeline
from trakt.runtime.runner_base import RunnerBase


class LambdaRunner(RunnerBase):
    """Execute pipelines within AWS Lambda."""

    def run(self, pipeline: Pipeline, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError("Lambda runner is not implemented yet.")
