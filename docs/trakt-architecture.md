# Trakt Architecture Draft

## Proposed Package Layout
```
trakt/
  __init__.py
  core/
    artifacts.py
    steps.py
    pipeline.py
    registry.py
    context.py
  runtime/
    runner_base.py
    local_runner.py
    glue_runner.py
    lambda_runner.py
  io/
    csv_reader.py
    csv_writer.py
  observability/
    otel.py
    manifest.py
```

## Key Interfaces
**Artifact**
- Fields: `name`, `kind`, `uri` (prefix/glob allowed), `schema` (optional), `metadata` (optional).
- Combine strategy for multi-file inputs: `concat`, `union_by_name`, `validate_schema`.
- CSV read options: `delimiter`, `encoding`, `header`, `date_columns`, `decimal`.

**Step**
- `id`, `inputs`, `outputs`, `run(ctx, **kwargs)`.
- No IO side effects by default.

**Pipeline**
- Holds step list and input/output bindings.
- Validates missing/unused inputs and output collisions.

**Runner**
- Resolves artifacts -> data objects (CSV -> DataFrame).
- Executes steps in order.
- Writes outputs and emits telemetry + manifest.

## Step Resolution
Support two paths:
1) Direct module import: `uses: steps.normalize.foo`.
2) Registry mapping: `uses: normalize.foo` resolved via registry/entry points.

## Observability
OpenTelemetry spans:
- Root span: pipeline run.
- Child spans: each step.
Attributes:
- `pipeline.name`, `pipeline.version`, `step.id`, `rows.in`, `rows.out`, `duration.ms`.
Events:
- Warnings, coercions, missing columns.

## Manifest (Draft)
JSON written per run, stored with outputs.
```json
{
  "run_id": "...",
  "pipeline": "travel_enrichment__cytric",
  "steps": [
    {
      "step_id": "normalize",
      "inputs": ["travel_report"],
      "outputs": ["travel_report_norm"],
      "rows_in": 1200,
      "rows_out": 1189,
      "duration_ms": 823,
      "files_read": 3,
      "rows_dropped": 11,
      "rows_unmatched": 25
    }
  ]
}
```

## Config Inputs
- YAML pipeline file.
- Runner config (e.g., local paths or S3 prefix).
- Optional environment variables for OTEL exporters.

## Joins, Dedupe, and Rename Policies
Add standard step options so pipelines are explicit and auditable:
- **Join**: keys, join type, collision policy (suffix/rename), unmatched policy.
- **Dedupe**: key fields + winner rule (latest timestamp, non-null, max/min).
- **Rename**: column map + required/optional columns; warn on missing.

## Compatibility Notes
- CSV-only IO at first, but artifact abstraction should allow new types.
- Dataframes are passed in-memory; keep batch sizes under control.
