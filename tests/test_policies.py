import pandas as pd
import pytest

from trakt.core.policies import (
    DedupePolicy,
    JoinPolicy,
    RenamePolicy,
    apply_dedupe_policy,
    apply_join_policy,
    apply_rename_policy,
)


class EventContext:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def emit_event(self, event_name: str, **attributes: object) -> None:
        self.events.append((event_name, attributes))


def test_join_policy_warn_emits_event_for_unmatched_rows() -> None:
    left = pd.DataFrame([{"id": 1, "amount": 10}, {"id": 2, "amount": 30}])
    right = pd.DataFrame([{"id": 1, "country": "US"}])
    ctx = EventContext()

    result = apply_join_policy(
        left,
        right,
        JoinPolicy(keys=["id"], how="left", unmatched="warn"),
        ctx=ctx,
    )

    assert len(result) == 2
    assert ctx.events == [
        (
            "warning.join_unmatched",
            {"unmatched_count": 1, "keys": ["id"]},
        )
    ]


def test_join_policy_drop_removes_unmatched_rows() -> None:
    left = pd.DataFrame([{"id": 1, "amount": 10}, {"id": 2, "amount": 30}])
    right = pd.DataFrame([{"id": 1, "country": "US"}])

    result = apply_join_policy(
        left,
        right,
        JoinPolicy(keys=["id"], how="left", unmatched="drop"),
    )

    assert result["id"].tolist() == [1]


def test_join_policy_fail_raises_when_unmatched_rows_exist() -> None:
    left = pd.DataFrame([{"id": 1, "amount": 10}, {"id": 2, "amount": 30}])
    right = pd.DataFrame([{"id": 1, "country": "US"}])

    with pytest.raises(ValueError, match="unmatched rows"):
        apply_join_policy(
            left,
            right,
            JoinPolicy(keys=["id"], how="left", unmatched="fail"),
        )


def test_dedupe_policy_latest_keeps_most_recent_row() -> None:
    data = pd.DataFrame(
        [
            {"id": 1, "updated_at": "2024-01-01", "amount": 10},
            {"id": 1, "updated_at": "2024-02-01", "amount": 20},
            {"id": 2, "updated_at": "2024-03-01", "amount": 30},
        ]
    )

    result = apply_dedupe_policy(
        data,
        DedupePolicy(keys=["id"], winner="latest", order_by="updated_at"),
    )

    assert result.loc[result["id"] == 1, "amount"].item() == 20
    assert sorted(result["id"].tolist()) == [1, 2]


def test_dedupe_policy_non_null_prefers_more_complete_rows() -> None:
    data = pd.DataFrame(
        [
            {"id": 1, "country": None, "city": "Seattle"},
            {"id": 1, "country": "US", "city": "Seattle"},
        ]
    )

    result = apply_dedupe_policy(data, DedupePolicy(keys=["id"], winner="non_null"))

    assert len(result) == 1
    assert result.iloc[0]["country"] == "US"


def test_rename_policy_enforces_required_and_warns_optional() -> None:
    data = pd.DataFrame([{"id": 1, "amount": 10}])
    ctx = EventContext()

    renamed = apply_rename_policy(
        data,
        RenamePolicy(
            mapping={"amount": "total_amount"},
            required=["id"],
            optional=["country"],
        ),
        ctx=ctx,
    )

    assert "total_amount" in renamed.columns
    assert ctx.events == [
        (
            "warning.rename_optional_missing",
            {"columns": ["country"]},
        )
    ]


def test_rename_policy_raises_for_missing_required_columns() -> None:
    data = pd.DataFrame([{"id": 1, "amount": 10}])

    with pytest.raises(ValueError, match="Missing required columns"):
        apply_rename_policy(
            data,
            RenamePolicy(mapping={"amount": "total_amount"}, required=["country"]),
        )
