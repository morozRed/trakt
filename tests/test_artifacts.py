import pandas as pd
import pytest

from trakt.core.artifacts import CombineStrategy, combine_artifact_frames


def test_concat_requires_matching_columns() -> None:
    frame_a = pd.DataFrame([{"id": 1, "amount": 10}])
    frame_b = pd.DataFrame([{"id": 2, "amount": 20}])

    combined = combine_artifact_frames([frame_a, frame_b], CombineStrategy.CONCAT)

    assert list(combined.columns) == ["id", "amount"]
    assert combined["amount"].tolist() == [10, 20]


def test_union_by_name_aligns_columns() -> None:
    frame_a = pd.DataFrame([{"id": 1, "amount": 10}])
    frame_b = pd.DataFrame([{"id": 2, "currency": "USD"}])

    combined = combine_artifact_frames([frame_a, frame_b], CombineStrategy.UNION_BY_NAME)

    assert set(combined.columns) == {"id", "amount", "currency"}
    assert combined["id"].tolist() == [1, 2]


def test_validate_schema_rejects_dtype_mismatch() -> None:
    frame_a = pd.DataFrame([{"id": 1, "amount": 10}])
    frame_b = pd.DataFrame([{"id": 2, "amount": "20"}])

    with pytest.raises(ValueError, match="Schema mismatch"):
        combine_artifact_frames([frame_a, frame_b], CombineStrategy.VALIDATE_SCHEMA)
