# Trakt Framework Overview

## Purpose
Trakt is a lightweight ETL framework for building chainable pipelines in Python and running them locally or in cloud runtimes (AWS Glue, AWS Lambda, or future targets like Workers). It separates pipeline logic from runtime IO so the same steps can run in different environments.

## Goals
- Keep pipelines readable and reviewable (YAML-first, with optional Python DSL).
- Encourage pure, testable steps that transform tabular data.
- Provide consistent observability (OpenTelemetry + manifest output).
- Support CSV as the initial IO format with room to expand.

## Non-Goals (For Now)
- Distributed execution or Spark integration.
- Automatic schema inference across formats.
- Complex orchestration (beyond ordered steps).

## Core Concepts
- **Artifact**: A typed IO handle (e.g., CSV). Artifacts can represent a *set* of files (prefix/glob) and include a combine strategy.
- **Step**: A unit of work. Input artifacts in, output artifacts out. Runs as pure Python.
- **Pipeline**: A chain of steps with named inputs/outputs.
- **Runner**: Runtime adapter for local/Glue/Lambda. Handles IO and environment concerns.
- **Context**: Runtime metadata and telemetry hooks.

## Definition Formats
YAML is the primary pipeline description. A Python DSL is optional for advanced or programmatic pipelines.

YAML example:
```yaml
inputs:
  travel_report: { kind: csv, uri: s3://bucket/path }
steps:
  - id: normalize
    uses: normalize.travel_report
    with: { input: travel_report, output: travel_report_norm }
outputs:
  datasets:
    - name: travel_enriched
      from: travel_report_norm
```

## Execution Model
- Runner resolves artifacts (CSV -> DataFrame), including multi-file inputs.
- Steps run in-process and return outputs.
- Runner writes outputs and emits a manifest.

## Observability
- OpenTelemetry spans per pipeline and per step.
- Key attributes: step id, input/output names, row counts, duration.
- Manifest JSON always written for auditability (files read, join/dedupe stats).

## Multi-File Inputs (Artifact Sets)
Artifacts can point to a prefix/glob and resolve to multiple files. Each artifact declares a combine strategy, for example:
- `concat` (default): stack rows across files
- `union_by_name`: align columns by name, fill missing with nulls
- `validate_schema`: ensure same schema, then concat

## Join / Dedupe / Rename Policies
Steps should declare join keys and collision policies (suffixing or renaming). Deduplication should be explicit (keys + winner policy). Column renames should be tracked to keep schemas stable.

## Extension Points
- Custom artifact types (e.g., parquet, json).
- Step registry/entry points for plugin packages.
- New runners for other environments.
