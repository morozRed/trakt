"""Artifact models used by pipeline definitions and runners."""

from collections.abc import Mapping, Sequence
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


@dataclass(slots=True)
class OutputDataset:
    """Output dataset declaration with optional per-dataset config."""

    name: str
    source: str
    kind: str | None = None
    uri: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


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


def validate_artifact_schema(
    frame: Any,
    schema: Any,
    *,
    artifact_name: str | None = None,
    source: str | None = None,
) -> None:
    """Validate a DataFrame-like object against a declared schema."""
    if schema is None:
        return

    columns, dtypes = _parse_schema_definition(
        schema,
        artifact_name=artifact_name,
    )

    actual_columns = _coerce_frame_columns(frame, artifact_name=artifact_name)
    if columns is not None:
        if actual_columns != columns:
            raise ValueError(
                f"{_schema_label(artifact_name, source)} schema columns mismatch: "
                f"expected={columns}, got={actual_columns}."
            )
    elif dtypes is not None:
        missing = [name for name in dtypes if name not in actual_columns]
        extra = [name for name in actual_columns if name not in dtypes]
        if missing or extra:
            details: list[str] = []
            if missing:
                details.append(f"missing={missing}")
            if extra:
                details.append(f"extra={extra}")
            raise ValueError(
                f"{_schema_label(artifact_name, source)} schema columns mismatch: "
                + ", ".join(details)
                + "."
            )

    if dtypes:
        actual_dtypes = _coerce_frame_dtypes(frame, artifact_name=artifact_name)
        mismatched = {
            name: (expected, actual_dtypes.get(name))
            for name, expected in dtypes.items()
            if actual_dtypes.get(name) != expected
        }
        if mismatched:
            formatted = ", ".join(
                f"{name} expected={expected} got={actual}"
                for name, (expected, actual) in mismatched.items()
            )
            raise ValueError(
                f"{_schema_label(artifact_name, source)} schema dtypes mismatch: "
                + formatted
                + "."
            )


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


def _parse_schema_definition(
    schema: Any, *, artifact_name: str | None = None
) -> tuple[list[str] | None, dict[str, str] | None]:
    if isinstance(schema, Mapping):
        if "columns" in schema or "dtypes" in schema:
            extra = set(schema) - {"columns", "dtypes"}
            if extra:
                raise ValueError(
                    f"{_schema_label(artifact_name, None)} schema has unsupported keys: "
                    + ", ".join(sorted(extra))
                    + "."
                )
            columns = schema.get("columns")
            dtypes = schema.get("dtypes")
        else:
            columns = None
            dtypes = schema
    elif isinstance(schema, Sequence) and not isinstance(schema, (str, bytes)):
        columns = list(schema)
        dtypes = None
    else:
        raise ValueError(
            f"{_schema_label(artifact_name, None)} schema must be a mapping or list of column names."
        )

    normalized_columns: list[str] | None = None
    if columns is not None:
        if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
            raise ValueError(
                f"{_schema_label(artifact_name, None)} schema columns must be a list of strings."
            )
        normalized_columns = list(columns)
        if not all(isinstance(name, str) for name in normalized_columns):
            raise ValueError(
                f"{_schema_label(artifact_name, None)} schema columns must be strings."
            )
        if len(set(normalized_columns)) != len(normalized_columns):
            raise ValueError(
                f"{_schema_label(artifact_name, None)} schema columns contain duplicates."
            )

    normalized_dtypes: dict[str, str] | None = None
    if dtypes is not None:
        if not isinstance(dtypes, Mapping):
            raise ValueError(
                f"{_schema_label(artifact_name, None)} schema dtypes must be a mapping."
            )
        normalized_dtypes = {}
        for key, value in dtypes.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError(
                    f"{_schema_label(artifact_name, None)} schema dtypes must map strings to strings."
                )
            normalized_dtypes[key] = value

    if normalized_columns is not None and normalized_dtypes is not None:
        unknown = [name for name in normalized_dtypes if name not in normalized_columns]
        if unknown:
            raise ValueError(
                f"{_schema_label(artifact_name, None)} schema dtypes include unknown columns: "
                + ", ".join(unknown)
                + "."
            )

    return normalized_columns, normalized_dtypes


def _coerce_frame_columns(frame: Any, *, artifact_name: str | None = None) -> list[str]:
    if not hasattr(frame, "columns"):
        raise TypeError(
            f"{_schema_label(artifact_name, None)} schema validation expects a DataFrame-like object."
        )
    return list(frame.columns)


def _coerce_frame_dtypes(frame: Any, *, artifact_name: str | None = None) -> dict[str, str]:
    if not hasattr(frame, "dtypes"):
        raise TypeError(
            f"{_schema_label(artifact_name, None)} schema validation expects a DataFrame-like object."
        )
    dtypes = frame.dtypes
    if hasattr(dtypes, "astype"):
        dtypes = dtypes.astype(str)
    if hasattr(dtypes, "to_dict"):
        return {name: str(value) for name, value in dtypes.to_dict().items()}
    if isinstance(dtypes, Mapping):
        return {name: str(value) for name, value in dtypes.items()}
    raise TypeError(
        f"{_schema_label(artifact_name, None)} schema validation could not read dtypes."
    )


def _schema_label(artifact_name: str | None, source: str | None) -> str:
    label = f"Artifact '{artifact_name}'" if artifact_name else "Artifact"
    if source:
        label += f" source '{source}'"
    return label
