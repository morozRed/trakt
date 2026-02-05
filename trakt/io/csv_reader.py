"""CSV reader adapter."""

import csv
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def read_csv(
    uri: str,
    *,
    delimiter: str | None = ",",
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
    resolved_delimiter = _normalize_delimiter(
        delimiter,
        uri=uri,
        encoding=encoding,
        delimiter_candidates=options.pop("delimiter_candidates", None),
    )

    return pd.read_csv(
        Path(uri),
        sep=resolved_delimiter,
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


def _normalize_header(value: int | str | bool | None) -> int | None | str:
    if value is None:
        return None
    if isinstance(value, bool):
        return "infer" if value else None
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


def _normalize_delimiter(
    value: str | None,
    *,
    uri: str,
    encoding: str,
    delimiter_candidates: str | Sequence[str] | None,
) -> str:
    if value is None:
        return ","
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"auto", "sniff"}:
            return _detect_delimiter(
                uri=uri,
                encoding=encoding,
                delimiter_candidates=delimiter_candidates,
            )
        if value:
            return value
    raise ValueError(f"Unsupported CSV delimiter value: {value}")


def _detect_delimiter(
    *,
    uri: str,
    encoding: str,
    delimiter_candidates: str | Sequence[str] | None,
) -> str:
    candidates = _normalize_delimiter_candidates(delimiter_candidates)
    try:
        sample = Path(uri).read_text(encoding=encoding, errors="ignore")[:8192]
    except OSError:
        return ","
    if not sample.strip():
        return ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=candidates)
    except csv.Error:
        return ","
    return str(getattr(dialect, "delimiter", ",") or ",")


def _normalize_delimiter_candidates(value: str | Sequence[str] | None) -> str:
    if value is None:
        return ",;\t|"
    if isinstance(value, str):
        return value
    return "".join(str(item) for item in value)
