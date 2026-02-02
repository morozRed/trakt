"""Artifact models used by pipeline definitions and runners."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CombineStrategy(str, Enum):
    """Supported multi-file artifact combine strategies."""

    CONCAT = "concat"
    UNION_BY_NAME = "union_by_name"
    VALIDATE_SCHEMA = "validate_schema"


@dataclass(slots=True)
class Artifact:
    """Typed IO handle resolved by a runner."""

    name: str
    kind: str
    uri: str
    schema: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    combine_strategy: CombineStrategy = CombineStrategy.CONCAT

    def __post_init__(self) -> None:
        if isinstance(self.combine_strategy, str):
            self.combine_strategy = CombineStrategy(self.combine_strategy)


def combine_artifact_frames(
    frames: Sequence[Any], strategy: CombineStrategy | str
) -> Any:
    """Combine multiple DataFrame-like objects using a declared strategy."""
    if not frames:
        raise ValueError("Expected at least one frame to combine.")

    combine_strategy = (
        CombineStrategy(strategy) if isinstance(strategy, str) else strategy
    )

    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "Combining artifact frames requires pandas to be installed."
        ) from exc

    if combine_strategy is CombineStrategy.CONCAT:
        _ensure_same_columns(frames)
        return pd.concat(frames, ignore_index=True)

    if combine_strategy is CombineStrategy.VALIDATE_SCHEMA:
        _ensure_same_columns(frames)
        _ensure_same_dtypes(frames)
        return pd.concat(frames, ignore_index=True)

    if combine_strategy is CombineStrategy.UNION_BY_NAME:
        return pd.concat(frames, ignore_index=True, sort=False)

    raise ValueError(f"Unsupported combine strategy: {combine_strategy}")


def _ensure_same_columns(frames: Sequence[Any]) -> None:
    expected_columns = tuple(frames[0].columns)
    for index, frame in enumerate(frames[1:], start=1):
        columns = tuple(frame.columns)
        if columns != expected_columns:
            raise ValueError(
                "Column mismatch while combining frames: "
                f"frame 0 columns={expected_columns}, frame {index} columns={columns}"
            )


def _ensure_same_dtypes(frames: Sequence[Any]) -> None:
    expected_dtypes = tuple(frames[0].dtypes.astype(str))
    for index, frame in enumerate(frames[1:], start=1):
        dtypes = tuple(frame.dtypes.astype(str))
        if dtypes != expected_dtypes:
            raise ValueError(
                "Schema mismatch while combining frames: "
                f"frame 0 dtypes={expected_dtypes}, frame {index} dtypes={dtypes}"
            )
