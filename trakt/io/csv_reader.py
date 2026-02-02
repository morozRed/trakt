"""CSV reader adapter."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any


def read_csv(
    uri: str,
    *,
    delimiter: str = ",",
    encoding: str = "utf-8",
    header: int | str | None = "infer",
    date_columns: str | Sequence[str] | None = None,
    decimal: str = ".",
    **options: Any,
) -> Any:
    """Read a CSV file into a pandas DataFrame."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("CSV reading requires pandas to be installed.") from exc

    parse_dates = _normalize_date_columns(date_columns)
    normalized_header = _normalize_header(header)

    return pd.read_csv(
        Path(uri),
        sep=delimiter,
        encoding=encoding,
        header=normalized_header,
        parse_dates=parse_dates,
        decimal=decimal,
        **options,
    )


def _normalize_date_columns(value: str | Sequence[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",") if part.strip()]
        return parts or None
    return [str(item) for item in value]


def _normalize_header(value: int | str | None) -> int | None | str:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    normalized = value.strip().lower()
    if normalized in {"none", "null", "false"}:
        return None
    if normalized in {"infer", "true"}:
        return "infer"
    if normalized.isdigit():
        return int(normalized)
    raise ValueError(f"Unsupported CSV header value: {value}")
