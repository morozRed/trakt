"""Reusable policy helpers for join, dedupe, and rename operations."""

from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any


@dataclass(slots=True)
class JoinPolicy:
    """Declarative options for dataframe joins."""

    keys: list[str]
    how: str = "left"
    collision: str = "suffix"
    suffixes: tuple[str, str] = ("_left", "_right")
    unmatched: str = "allow"  # allow | warn | drop | fail


@dataclass(slots=True)
class DedupePolicy:
    """Declarative options for deduplication."""

    keys: list[str]
    winner: str = "latest"  # latest | earliest | max | min | first | last | non_null
    order_by: str | None = None


@dataclass(slots=True)
class RenamePolicy:
    """Declarative options for column rename behavior."""

    mapping: dict[str, str]
    required: list[str] = field(default_factory=list)
    optional: list[str] = field(default_factory=list)
    warn_on_missing_optional: bool = True


@dataclass(slots=True)
class QualityGatePolicy:
    """Declarative data-quality checks with warn/fail modes."""

    mode: str = "fail"  # fail | warn
    required_columns: list[str] = field(default_factory=list)
    unique_keys: list[list[str]] = field(default_factory=list)
    row_count_min: int | None = None
    row_count_max: int | None = None
    max_null_ratio: dict[str, float] = field(default_factory=dict)
    gate_modes: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.mode = str(self.mode).strip().lower()
        self.gate_modes = {
            str(name): str(mode).strip().lower()
            for name, mode in self.gate_modes.items()
        }


def apply_join_policy(
    left: Any,
    right: Any,
    policy: JoinPolicy,
    *,
    ctx: Any | None = None,
) -> Any:
    """Join two dataframes using a shared policy contract."""
    _validate_join_inputs(left, right)
    _validate_join_policy(policy)

    missing_left = sorted(set(policy.keys) - set(left.columns))
    missing_right = sorted(set(policy.keys) - set(right.columns))
    if missing_left or missing_right:
        raise ValueError(
            "Join keys missing from input frames: "
            f"left={missing_left or '[]'}, right={missing_right or '[]'}."
        )

    suffixes = policy.suffixes if policy.collision == "suffix" else ("", "")
    if policy.unmatched == "allow":
        return left.merge(right, on=policy.keys, how=policy.how, suffixes=suffixes)

    merged = left.merge(
        right,
        on=policy.keys,
        how=policy.how,
        suffixes=suffixes,
        indicator=True,
    )
    unmatched_count = int((merged["_merge"] == "left_only").sum())
    if unmatched_count:
        if policy.unmatched == "warn":
            _emit_policy_event(
                ctx,
                "warning.join_unmatched",
                unmatched_count=unmatched_count,
                keys=policy.keys,
            )
        elif policy.unmatched == "drop":
            merged = merged.loc[merged["_merge"] == "both"]
        elif policy.unmatched == "fail":
            raise ValueError(
                f"Join policy failed due to {unmatched_count} unmatched rows."
            )

    return merged.drop(columns=["_merge"])


def apply_dedupe_policy(data: Any, policy: DedupePolicy) -> Any:
    """Deduplicate rows using key fields and a winner policy."""
    _validate_dedupe_policy(policy)

    missing_keys = sorted(set(policy.keys) - set(data.columns))
    if missing_keys:
        raise ValueError(f"Dedupe keys missing from dataframe: {missing_keys}.")

    frame = data.copy()
    if policy.winner in {"latest", "max"}:
        frame = _sort_frame(frame, policy.order_by, ascending=False)
        return frame.drop_duplicates(subset=policy.keys, keep="first").reset_index(
            drop=True
        )

    if policy.winner in {"earliest", "min"}:
        frame = _sort_frame(frame, policy.order_by, ascending=True)
        return frame.drop_duplicates(subset=policy.keys, keep="first").reset_index(
            drop=True
        )

    if policy.winner == "first":
        return frame.drop_duplicates(subset=policy.keys, keep="first").reset_index(
            drop=True
        )

    if policy.winner == "last":
        return frame.drop_duplicates(subset=policy.keys, keep="last").reset_index(
            drop=True
        )

    if policy.winner == "non_null":
        frame["__non_null_count"] = frame.notna().sum(axis=1)
        frame = frame.sort_values("__non_null_count", ascending=False)
        frame = frame.drop_duplicates(subset=policy.keys, keep="first")
        return frame.drop(columns=["__non_null_count"]).reset_index(drop=True)

    raise ValueError(f"Unsupported dedupe winner policy: {policy.winner}.")


def apply_rename_policy(data: Any, policy: RenamePolicy, *, ctx: Any | None = None) -> Any:
    """Rename columns while enforcing required/optional column behavior."""
    required_missing = sorted(set(policy.required) - set(data.columns))
    if required_missing:
        raise ValueError(f"Missing required columns for rename: {required_missing}.")

    optional_missing = sorted(set(policy.optional) - set(data.columns))
    if optional_missing and policy.warn_on_missing_optional:
        _emit_policy_event(
            ctx,
            "warning.rename_optional_missing",
            columns=optional_missing,
        )

    active_mapping = {old: new for old, new in policy.mapping.items() if old in data.columns}
    renamed = data.rename(columns=active_mapping)

    duplicated = sorted(
        column
        for column, count in renamed.columns.value_counts().items()
        if count > 1
    )
    if duplicated:
        raise ValueError(f"Rename produced duplicate target columns: {duplicated}.")
    return renamed


def evaluate_quality_gates(
    data: Any,
    policy: QualityGatePolicy | Mapping[str, Any],
    *,
    ctx: Any | None = None,
) -> tuple[Any, dict[str, int]]:
    """Evaluate quality checks and return passthrough data + gate metrics."""
    normalized = _coerce_quality_policy(policy)
    _validate_quality_policy(normalized)
    _validate_dataframe_like(data)

    metrics = {
        "quality_checks": 0,
        "quality_violations": 0,
        "quality_warnings": 0,
    }

    if normalized.required_columns:
        metrics["quality_checks"] += 1
        missing = sorted(set(normalized.required_columns) - set(data.columns))
        if missing:
            _handle_quality_violation(
                gate_name="required_columns",
                message=f"Missing required columns: {missing}.",
                policy=normalized,
                metrics=metrics,
                ctx=ctx,
                details={"columns": missing},
            )

    if normalized.unique_keys:
        for keys in normalized.unique_keys:
            metrics["quality_checks"] += 1
            missing = sorted(set(keys) - set(data.columns))
            if missing:
                _handle_quality_violation(
                    gate_name="unique_keys",
                    message=f"Unique key columns missing: {missing}.",
                    policy=normalized,
                    metrics=metrics,
                    ctx=ctx,
                    details={"keys": keys, "missing_columns": missing},
                )
                continue
            duplicate_count = int(len(data) - len(data.drop_duplicates(subset=keys)))
            if duplicate_count > 0:
                _handle_quality_violation(
                    gate_name="unique_keys",
                    message=(
                        f"Found {duplicate_count} duplicate rows for unique keys {keys}."
                    ),
                    policy=normalized,
                    metrics=metrics,
                    ctx=ctx,
                    details={"keys": keys, "duplicate_rows": duplicate_count},
                )

    if normalized.row_count_min is not None or normalized.row_count_max is not None:
        metrics["quality_checks"] += 1
        row_count = int(len(data))
        if normalized.row_count_min is not None and row_count < normalized.row_count_min:
            _handle_quality_violation(
                gate_name="row_count",
                message=(
                    f"Row count {row_count} is below minimum {normalized.row_count_min}."
                ),
                policy=normalized,
                metrics=metrics,
                ctx=ctx,
                details={
                    "row_count": row_count,
                    "min": normalized.row_count_min,
                },
            )
        if normalized.row_count_max is not None and row_count > normalized.row_count_max:
            _handle_quality_violation(
                gate_name="row_count",
                message=(
                    f"Row count {row_count} exceeds maximum {normalized.row_count_max}."
                ),
                policy=normalized,
                metrics=metrics,
                ctx=ctx,
                details={
                    "row_count": row_count,
                    "max": normalized.row_count_max,
                },
            )

    for column, threshold in normalized.max_null_ratio.items():
        metrics["quality_checks"] += 1
        if column not in data.columns:
            _handle_quality_violation(
                gate_name="max_null_ratio",
                message=f"Null ratio column is missing: {column}.",
                policy=normalized,
                metrics=metrics,
                ctx=ctx,
                details={"column": column, "max_null_ratio": threshold},
            )
            continue
        null_ratio = float(data[column].isna().mean())
        if null_ratio > threshold:
            _handle_quality_violation(
                gate_name="max_null_ratio",
                message=(
                    f"Column '{column}' null ratio {null_ratio:.4f} exceeds threshold {threshold:.4f}."
                ),
                policy=normalized,
                metrics=metrics,
                ctx=ctx,
                details={
                    "column": column,
                    "null_ratio": null_ratio,
                    "max_null_ratio": threshold,
                },
            )

    return data, metrics


def _sort_frame(data: Any, order_by: str | None, *, ascending: bool) -> Any:
    if order_by is None:
        raise ValueError("Dedupe policy requires 'order_by' for this winner rule.")
    if order_by not in data.columns:
        raise ValueError(f"Dedupe order_by column is missing: {order_by}.")
    return data.sort_values(order_by, ascending=ascending)


def _validate_dataframe_like(data: Any) -> None:
    required_attrs = ("columns", "drop_duplicates")
    if not all(hasattr(data, attr) for attr in required_attrs):
        raise TypeError("Quality gates require a pandas DataFrame-like input.")


def _validate_join_inputs(left: Any, right: Any) -> None:
    if not hasattr(left, "merge") or not hasattr(right, "merge"):
        raise TypeError("Join policy helpers require pandas DataFrame-like inputs.")


def _validate_join_policy(policy: JoinPolicy) -> None:
    if not policy.keys:
        raise ValueError("Join policy requires at least one key column.")
    if policy.how not in {"left", "right", "inner", "outer"}:
        raise ValueError(f"Unsupported join type: {policy.how}.")
    if policy.collision not in {"suffix", "none"}:
        raise ValueError(f"Unsupported collision policy: {policy.collision}.")
    if policy.unmatched not in {"allow", "warn", "drop", "fail"}:
        raise ValueError(f"Unsupported unmatched policy: {policy.unmatched}.")


def _validate_dedupe_policy(policy: DedupePolicy) -> None:
    if not policy.keys:
        raise ValueError("Dedupe policy requires at least one key column.")
    supported = {"latest", "earliest", "max", "min", "first", "last", "non_null"}
    if policy.winner not in supported:
        raise ValueError(f"Unsupported dedupe winner policy: {policy.winner}.")


def _coerce_quality_policy(policy: QualityGatePolicy | Mapping[str, Any]) -> QualityGatePolicy:
    if isinstance(policy, QualityGatePolicy):
        return policy
    if not isinstance(policy, Mapping):
        raise TypeError("Quality gate policy must be a QualityGatePolicy or mapping.")

    unique_keys_raw = policy.get("unique_keys", [])
    unique_keys = _normalize_unique_keys(unique_keys_raw)

    row_count = policy.get("row_count") or {}
    if row_count is None:
        row_count = {}
    if not isinstance(row_count, Mapping):
        raise TypeError("Quality gate 'row_count' must be a mapping when provided.")
    gate_modes = policy.get("gate_modes") or {}
    if not isinstance(gate_modes, Mapping):
        raise TypeError("Quality gate 'gate_modes' must be a mapping when provided.")

    return QualityGatePolicy(
        mode=str(policy.get("mode", "fail")),
        required_columns=_normalize_string_list(
            policy.get("required_columns", []),
            field_name="required_columns",
        ),
        unique_keys=unique_keys,
        row_count_min=_coerce_optional_int(row_count.get("min")),
        row_count_max=_coerce_optional_int(row_count.get("max")),
        max_null_ratio=_coerce_max_null_ratio(policy.get("max_null_ratio", {})),
        gate_modes={
            str(name): str(mode)
            for name, mode in gate_modes.items()
        },
    )


def _normalize_unique_keys(value: Any) -> list[list[str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError("Quality gate 'unique_keys' must be a list.")
    normalized: list[list[str]] = []
    for item in value:
        if isinstance(item, str):
            normalized.append([item])
            continue
        if isinstance(item, list):
            normalized.append([str(column) for column in item])
            continue
        raise TypeError("Quality gate 'unique_keys' items must be strings or lists.")
    return normalized


def _normalize_string_list(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"Quality gate '{field_name}' must be a list.")
    return [str(item) for item in value]


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Quality gate integer thresholds cannot be booleans.")
    if isinstance(value, (int, float)):
        return int(value)
    raise TypeError("Quality gate row_count thresholds must be numeric.")


def _coerce_max_null_ratio(value: Any) -> dict[str, float]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("Quality gate 'max_null_ratio' must be a mapping.")
    normalized: dict[str, float] = {}
    for column, threshold in value.items():
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
            raise TypeError(
                f"Quality gate max_null_ratio for column '{column}' must be numeric."
            )
        normalized[str(column)] = float(threshold)
    return normalized


def _validate_quality_policy(policy: QualityGatePolicy) -> None:
    _normalize_quality_mode(policy.mode, field_name="mode")
    for name, mode in policy.gate_modes.items():
        _normalize_quality_mode(mode, field_name=f"gate_modes.{name}")
    if policy.row_count_min is not None and policy.row_count_min < 0:
        raise ValueError("Quality gate row_count min must be >= 0.")
    if policy.row_count_max is not None and policy.row_count_max < 0:
        raise ValueError("Quality gate row_count max must be >= 0.")
    if (
        policy.row_count_min is not None
        and policy.row_count_max is not None
        and policy.row_count_min > policy.row_count_max
    ):
        raise ValueError("Quality gate row_count min cannot exceed max.")
    for column, threshold in policy.max_null_ratio.items():
        if threshold < 0 or threshold > 1:
            raise ValueError(
                f"Quality gate max_null_ratio for '{column}' must be between 0 and 1."
            )


def _normalize_quality_mode(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"Quality gate {field_name} must be a string.")
    normalized = value.strip().lower()
    if normalized not in {"warn", "fail"}:
        raise ValueError(f"Unsupported quality gate mode '{value}'.")
    return normalized


def _handle_quality_violation(
    *,
    gate_name: str,
    message: str,
    policy: QualityGatePolicy,
    metrics: dict[str, int],
    ctx: Any | None,
    details: dict[str, Any] | None = None,
) -> None:
    metrics["quality_violations"] += 1
    mode = policy.gate_modes.get(gate_name, policy.mode)
    payload = dict(details or {})
    payload["gate"] = gate_name
    payload["message"] = message
    if mode == "warn":
        metrics["quality_warnings"] += 1
        _emit_policy_event(ctx, "warning.quality_gate", **payload)
        return
    raise ValueError(message)


def _emit_policy_event(ctx: Any | None, event_name: str, **attributes: Any) -> None:
    if ctx is None:
        return
    emit_event = getattr(ctx, "emit_event", None)
    if callable(emit_event):
        emit_event(event_name, **attributes)
