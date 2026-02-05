"""Base runner execution flow shared by runtime adapters."""

from abc import ABC
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from trakt.core.bindings import get_const_binding_value, is_const_binding
from trakt.core.context import Context
from trakt.core.pipeline import Pipeline
from trakt.core.steps import ResolvedStep, Step
from trakt.observability.manifest import write_manifest
from trakt.observability.otel import get_tracer


class RunnerBase(ABC):
    """Template runner that owns IO and step execution orchestration."""

    def run(
        self,
        pipeline: Pipeline,
        *,
        run_id: str | None = None,
        pipeline_version: str | None = None,
        context_metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute a validated pipeline and persist declared outputs."""
        pipeline.validate()

        ctx = Context(
            run_id=run_id or uuid4().hex,
            pipeline_name=pipeline.name,
            pipeline_version=pipeline_version,
            metadata=dict(context_metadata or {}),
        )
        tracer = kwargs.get("tracer") or get_tracer(
            enabled=kwargs.get("otel_enabled", False),
            service_name=kwargs.get("otel_service_name", "trakt"),
            tracer_name=kwargs.get("otel_tracer_name", "trakt.runner"),
        )
        ctx.add_metadata("tracer", tracer)
        ctx.register_telemetry_hook(_otel_event_hook)
        ctx.add_metadata("runner", self.__class__.__name__)
        started_at = datetime.now(timezone.utc)
        artifacts: dict[str, Any] = {}
        step_reports: list[dict[str, Any]] = []
        outputs: dict[str, Any] = {}
        result: dict[str, Any] | None = None
        error: dict[str, Any] | None = None
        started = perf_counter()

        with tracer.start_as_current_span(
            "pipeline.run",
            attributes={
                "pipeline.name": pipeline.name,
                "pipeline.version": pipeline_version or "",
                "pipeline.execution_mode": pipeline.execution_mode,
                "run.id": ctx.run_id,
            },
        ) as pipeline_span:
            ctx.add_metadata("pipeline_span", pipeline_span)
            ctx.emit_event("pipeline.started")

            try:
                artifacts = self.load_inputs(pipeline, ctx, **kwargs)
                for step in pipeline.steps:
                    step_report = self.execute_step(step, artifacts, ctx)
                    step_reports.append(step_report)

                outputs = self.write_outputs(pipeline, artifacts, ctx, **kwargs)
                ctx.emit_event("pipeline.completed", output_count=len(outputs))
                result = {
                    "run_id": ctx.run_id,
                    "pipeline": pipeline.name,
                    "outputs": outputs,
                    "steps": step_reports,
                    "status": "success",
                }
                _set_span_attribute(pipeline_span, "status", "success")
            except Exception as exc:
                error = {"type": type(exc).__name__, "message": str(exc)}
                ctx.emit_event(
                    "pipeline.failed",
                    error_type=error["type"],
                    error_message=error["message"],
                )
                _set_span_attribute(pipeline_span, "status", "failed")
                _set_span_attribute(pipeline_span, "error.type", error["type"])
                _set_span_attribute(pipeline_span, "error.message", error["message"])
                raise
            finally:
                finished_at = datetime.now(timezone.utc)
                duration_ms = round((perf_counter() - started) * 1000, 3)
                _set_span_attribute(pipeline_span, "duration.ms", duration_ms)
                manifest_payload = self._build_manifest_payload(
                    ctx=ctx,
                    step_reports=step_reports,
                    outputs=outputs,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                    error=error,
                )
                manifest_path = self.get_manifest_path(ctx=ctx, **kwargs)
                try:
                    write_manifest(str(manifest_path), manifest_payload)
                except Exception:
                    if error is None:
                        raise
                else:
                    ctx.add_metadata("manifest_path", str(manifest_path))
                    if result is not None:
                        result["manifest_path"] = str(manifest_path)

        if result is None:
            raise RuntimeError("Pipeline run finished without result.")
        return result

    def load_inputs(
        self, pipeline: Pipeline, ctx: Context, **kwargs: Any
    ) -> dict[str, Any]:
        """Resolve declared pipeline inputs into in-memory artifacts."""
        raise NotImplementedError("load_inputs must be implemented by runner adapters.")

    def write_outputs(
        self, pipeline: Pipeline, artifacts: dict[str, Any], ctx: Context, **kwargs: Any
    ) -> dict[str, Any]:
        """Persist declared pipeline outputs and return output metadata."""
        raise NotImplementedError("write_outputs must be implemented by runner adapters.")

    def execute_step(
        self, step: Step, artifacts: dict[str, Any], ctx: Context
    ) -> dict[str, Any]:
        """Execute one step and merge named outputs into the artifact state."""
        step_kwargs = self._resolve_step_inputs(step, artifacts)
        rows_in = _count_rows(step_kwargs)
        tracer = ctx.get_metadata("tracer") or get_tracer(enabled=False)

        with tracer.start_as_current_span(
            f"step.{step.id}",
            attributes={
                "pipeline.name": ctx.pipeline_name,
                "pipeline.version": ctx.pipeline_version or "",
                "step.id": step.id,
            },
        ) as step_span:
            ctx.add_metadata("active_span", step_span)
            try:
                ctx.emit_event("step.started", step_id=step.id, rows_in=rows_in)
                started = perf_counter()
                raw_result = step.run(ctx, **step_kwargs)
                duration_ms = round((perf_counter() - started) * 1000, 3)
                step_result, step_metrics = _extract_step_metrics(step.id, raw_result)

                materialized = self._materialize_step_outputs(step, step_result)
                artifacts.update(materialized)
                rows_out = _count_rows(materialized)

                _set_span_attribute(step_span, "rows.in", rows_in)
                _set_span_attribute(step_span, "rows.out", rows_out)
                _set_span_attribute(step_span, "duration.ms", duration_ms)
                _set_step_metric_span_attributes(step_span, step_metrics)

                ctx.emit_event(
                    "step.completed",
                    step_id=step.id,
                    duration_ms=duration_ms,
                    rows_in=rows_in,
                    rows_out=rows_out,
                    metrics=step_metrics,
                )
            finally:
                ctx.add_metadata("active_span", None)
        return {
            "step_id": step.id,
            "duration_ms": duration_ms,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "outputs": list(materialized),
            "metrics": step_metrics,
        }

    def get_manifest_path(self, ctx: Context, **kwargs: Any) -> Path:
        """Resolve where manifest.json should be written for this run."""
        manifest_path = kwargs.get("manifest_path")
        if manifest_path:
            return Path(manifest_path)

        output_dir = kwargs.get("output_dir", getattr(self, "output_dir", "."))
        return Path(output_dir) / "manifest.json"

    def _build_manifest_payload(
        self,
        *,
        ctx: Context,
        step_reports: list[dict[str, Any]],
        outputs: dict[str, Any],
        started_at: datetime,
        finished_at: datetime,
        duration_ms: float,
        error: dict[str, Any] | None,
    ) -> dict[str, Any]:
        input_stats = ctx.get_metadata("input_stats", {})
        return {
            "run_id": ctx.run_id,
            "status": "failed" if error else "success",
            "pipeline": {
                "name": ctx.pipeline_name,
                "version": ctx.pipeline_version,
            },
            "runner": ctx.get_metadata("runner"),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": duration_ms,
            "inputs": input_stats,
            "steps": [
                {
                    "step_id": report["step_id"],
                    "rows_in": report.get("rows_in"),
                    "rows_out": report.get("rows_out"),
                    "duration_ms": report.get("duration_ms"),
                    "outputs": report.get("outputs", []),
                    "metrics": report.get("metrics", {}),
                    "files_read": report.get("files_read"),
                    "rows_dropped": (
                        report.get("rows_dropped")
                        if report.get("rows_dropped") is not None
                        else report.get("metrics", {}).get("rows_dropped")
                    ),
                    "rows_unmatched": (
                        report.get("rows_unmatched")
                        if report.get("rows_unmatched") is not None
                        else report.get("metrics", {}).get("rows_unmatched")
                    ),
                }
                for report in step_reports
            ],
            "outputs": outputs,
            "error": error,
        }

    def _resolve_step_inputs(self, step: Step, artifacts: dict[str, Any]) -> dict[str, Any]:
        if isinstance(step, ResolvedStep):
            params: dict[str, Any] = {}
            for param_name, bound_name in step.input_bindings().items():
                params[param_name] = _resolve_bound_input(bound_name, artifacts, step.id)
            return params

        params = {}
        for input_name in step.inputs:
            if input_name not in artifacts:
                raise KeyError(f"Step '{step.id}' missing input artifact '{input_name}'.")
            params[input_name] = artifacts[input_name]
        return params

    def _materialize_step_outputs(
        self, step: Step, result: dict[str, Any]
    ) -> dict[str, Any]:
        if not isinstance(result, dict):
            raise TypeError(
                f"Step '{step.id}' must return a dict, got {type(result).__name__}."
            )

        if isinstance(step, ResolvedStep):
            return _map_result_with_bindings(step.id, result, step.output_bindings())

        if not step.outputs:
            return result

        if all(name in result for name in step.outputs):
            return {name: result[name] for name in step.outputs}

        if len(step.outputs) == 1 and len(result) == 1:
            only_value = next(iter(result.values()))
            return {step.outputs[0]: only_value}

        raise KeyError(
            f"Step '{step.id}' result keys {sorted(result)} do not match outputs {step.outputs}."
        )


def _resolve_bound_input(
    bound_name: Any, artifacts: dict[str, Any], step_id: str
) -> Any:
    if is_const_binding(bound_name):
        return get_const_binding_value(bound_name)

    if isinstance(bound_name, str):
        if bound_name not in artifacts:
            raise KeyError(
                f"Step '{step_id}' requires artifact '{bound_name}' but it is missing. "
                "Use const(...) or YAML {'const': ...} for literal strings."
            )
        return artifacts[bound_name]

    if isinstance(bound_name, list):
        return [_resolve_bound_input(name, artifacts, step_id) for name in bound_name]

    if isinstance(bound_name, tuple):
        return tuple(_resolve_bound_input(name, artifacts, step_id) for name in bound_name)

    if isinstance(bound_name, Mapping):
        if is_const_binding(bound_name):
            return get_const_binding_value(bound_name)
        return {
            key: _resolve_bound_input(name, artifacts, step_id)
            for key, name in bound_name.items()
        }

    if isinstance(bound_name, (bool, int, float)) or bound_name is None:
        return bound_name

    raise TypeError(
        f"Invalid step input binding in step '{step_id}': "
        "expected artifact refs, const literals, list/tuple/mapping, or primitive values."
    )


def _map_result_with_bindings(
    step_id: str, result: dict[str, Any], bindings: dict[str, Any]
) -> dict[str, Any]:
    if not bindings:
        return result

    materialized: dict[str, Any] = {}
    for output_key, target_name in bindings.items():
        if output_key not in result:
            raise KeyError(
                f"Step '{step_id}' did not return expected output key '{output_key}'."
            )
        _apply_output_binding(
            step_id=step_id,
            source_value=result[output_key],
            target_binding=target_name,
            collector=materialized,
        )
    return materialized


def _apply_output_binding(
    *, step_id: str, source_value: Any, target_binding: Any, collector: dict[str, Any]
) -> None:
    if isinstance(target_binding, str):
        collector[target_binding] = source_value
        return

    if isinstance(target_binding, list):
        if not isinstance(source_value, (list, tuple)):
            raise TypeError(
                f"Step '{step_id}' must return a list/tuple for multi-output binding."
            )
        if len(source_value) != len(target_binding):
            raise ValueError(
                f"Step '{step_id}' returned {len(source_value)} values for "
                f"{len(target_binding)} output targets."
            )
        for index, target_name in enumerate(target_binding):
            collector[target_name] = source_value[index]
        return

    if isinstance(target_binding, Mapping):
        if not isinstance(source_value, Mapping):
            raise TypeError(
                f"Step '{step_id}' must return a mapping for mapped output binding."
            )
        for source_key, target_name in target_binding.items():
            if source_key not in source_value:
                raise KeyError(
                    f"Step '{step_id}' missing nested output key '{source_key}'."
                )
            collector[target_name] = source_value[source_key]
        return

    raise TypeError(
        f"Step '{step_id}' has unsupported output binding type {type(target_binding)}."
    )


def _count_rows(payload: Any) -> int | None:
    if payload is None:
        return None

    if isinstance(payload, (str, bytes)):
        return None

    if isinstance(payload, Mapping):
        row_counts = [_count_rows(value) for value in payload.values()]
        counts = [count for count in row_counts if count is not None]
        return sum(counts) if counts else None

    if isinstance(payload, (list, tuple)):
        row_counts = [_count_rows(value) for value in payload]
        counts = [count for count in row_counts if count is not None]
        return sum(counts) if counts else None

    try:
        return len(payload)
    except TypeError:
        return None


def _set_span_attribute(span: Any, key: str, value: Any) -> None:
    if value is None:
        return
    span.set_attribute(key, value)


def _extract_step_metrics(
    step_id: str, raw_result: Any
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(raw_result, dict):
        raise TypeError(
            f"Step '{step_id}' must return a dict, got {type(raw_result).__name__}."
        )

    metrics_key = "__metrics__"
    if metrics_key not in raw_result:
        return raw_result, {}

    metrics_value = raw_result[metrics_key]
    if not isinstance(metrics_value, Mapping):
        raise TypeError(
            f"Step '{step_id}' metrics must be a mapping when '{metrics_key}' is returned."
        )

    metrics = {str(key): value for key, value in metrics_value.items()}
    payload = {key: value for key, value in raw_result.items() if key != metrics_key}
    return payload, metrics


def _set_step_metric_span_attributes(span: Any, metrics: Mapping[str, Any]) -> None:
    for key, value in metrics.items():
        if isinstance(value, bool):
            _set_span_attribute(span, f"metric.{key}", value)
            continue
        if isinstance(value, (int, float)):
            _set_span_attribute(span, f"metric.{key}", value)


def _otel_event_hook(event_name: str, attributes: dict[str, Any], ctx: Context) -> None:
    if not _is_otel_event(event_name):
        return

    span = ctx.get_metadata("active_span") or ctx.get_metadata("pipeline_span")
    if span is None:
        return
    span.add_event(event_name, attributes=_normalize_event_attributes(attributes))


def _is_otel_event(event_name: str) -> bool:
    normalized = event_name.lower().replace("-", "_")
    return (
        "warning" in normalized
        or "coercion" in normalized
        or "missing_column" in normalized
    )


def _normalize_event_attributes(attributes: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in attributes.items():
        if isinstance(value, (bool, int, float, str)):
            normalized[key] = value
            continue
        normalized[key] = str(value)
    return normalized
