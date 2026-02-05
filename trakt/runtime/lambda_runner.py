"""AWS Lambda runtime adapter."""

from pathlib import Path
from typing import Any

from trakt.core.pipeline import Pipeline
from trakt.runtime.local_runner import LocalRunner


class LambdaRunner(LocalRunner):
    """Execute pipelines with Lambda-style bounded batch constraints."""

    def __init__(
        self,
        *,
        input_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        input_overrides: dict[str, str] | None = None,
        max_batch_rows: int = 50_000,
    ) -> None:
        super().__init__(
            input_dir=input_dir,
            output_dir=output_dir,
            input_overrides=input_overrides,
        )
        self.max_batch_rows = max_batch_rows

    def run(self, pipeline: Pipeline, **kwargs: Any) -> dict[str, Any]:
        context_metadata = dict(kwargs.pop("context_metadata", {}))
        context_metadata["runtime"] = "lambda"
        context_metadata["lambda.max_batch_rows"] = self.max_batch_rows
        return super().run(
            pipeline,
            context_metadata=context_metadata,
            **kwargs,
        )

    def load_inputs(
        self, pipeline: Pipeline, ctx, **kwargs: Any
    ) -> dict[str, Any]:
        loaded = super().load_inputs(pipeline, ctx, **kwargs)
        for input_name, payload in loaded.items():
            row_count = _safe_len(payload)
            if row_count is not None and row_count > self.max_batch_rows:
                raise ValueError(
                    "Lambda runner input exceeds max_batch_rows "
                    f"({row_count} > {self.max_batch_rows}) for '{input_name}'."
                )
        return loaded


def _safe_len(payload: Any) -> int | None:
    try:
        return len(payload)
    except TypeError:
        return None
