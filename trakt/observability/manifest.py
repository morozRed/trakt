"""Manifest writing utilities."""

import json
from pathlib import Path
from typing import Any


def write_manifest(path: str, payload: dict[str, Any]) -> None:
    """Write run manifest content as pretty JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
