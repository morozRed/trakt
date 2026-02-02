"""IO adapters for Trakt."""

from trakt.io.csv_reader import read_csv
from trakt.io.csv_writer import write_csv

__all__ = ["read_csv", "write_csv"]
