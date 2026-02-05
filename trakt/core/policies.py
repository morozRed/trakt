"""Reusable policy helpers for join, dedupe, and rename operations."""

from dataclasses import dataclass, field
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


def _sort_frame(data: Any, order_by: str | None, *, ascending: bool) -> Any:
    if order_by is None:
        raise ValueError("Dedupe policy requires 'order_by' for this winner rule.")
    if order_by not in data.columns:
        raise ValueError(f"Dedupe order_by column is missing: {order_by}.")
    return data.sort_values(order_by, ascending=ascending)


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


def _emit_policy_event(ctx: Any | None, event_name: str, **attributes: Any) -> None:
    if ctx is None:
        return
    emit_event = getattr(ctx, "emit_event", None)
    if callable(emit_event):
        emit_event(event_name, **attributes)
