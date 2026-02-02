"""Runtime context passed into steps."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

TelemetryHook = Callable[[str, dict[str, Any], "Context"], None]


@dataclass(slots=True)
class Context:
    """Holds run-level metadata and telemetry hooks."""

    run_id: str
    pipeline_name: str
    pipeline_version: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)
    telemetry_hooks: list[TelemetryHook] = field(default_factory=list)

    def add_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        return self.metadata.get(key, default)

    def register_telemetry_hook(self, hook: TelemetryHook) -> None:
        self.telemetry_hooks.append(hook)

    def emit_event(self, event_name: str, **attributes: Any) -> None:
        payload = dict(attributes)
        for hook in self.telemetry_hooks:
            hook(event_name, payload, self)
