"""OpenTelemetry integration helpers."""

from contextlib import contextmanager
from typing import Any


class NoOpSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        return

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        return

    def end(self) -> None:
        return


class NoOpTracer:
    @contextmanager
    def start_as_current_span(
        self, name: str, attributes: dict[str, Any] | None = None
    ):
        yield NoOpSpan()


def get_tracer(**kwargs: Any) -> Any:
    """Return a real tracer if enabled, otherwise a no-op tracer."""
    enabled = bool(kwargs.get("enabled", False))
    service_name = kwargs.get("service_name", "trakt")
    tracer_name = kwargs.get("tracer_name", "trakt")

    if not enabled:
        return NoOpTracer()

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
    except ImportError:
        return NoOpTracer()

    current_provider = trace.get_tracer_provider()
    if isinstance(current_provider, TracerProvider):
        return current_provider.get_tracer(tracer_name)

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(tracer_name)
