"""CSV writer adapter."""

from pathlib import Path
from typing import Any


def write_csv(
    data: Any,
    uri: str,
    *,
    delimiter: str = ",",
    encoding: str = "utf-8",
    header: bool = True,
    index: bool = False,
    decimal: str = ".",
    **options: Any,
) -> None:
    """Write a pandas DataFrame-like object to CSV."""
    target = Path(uri)
    target.parent.mkdir(parents=True, exist_ok=True)

    to_csv = getattr(data, "to_csv", None)
    if not callable(to_csv):
        raise TypeError("CSV writing expects a DataFrame-like object with to_csv().")

    to_csv(
        target,
        sep=delimiter,
        encoding=encoding,
        header=header,
        index=index,
        decimal=decimal,
        **options,
    )
