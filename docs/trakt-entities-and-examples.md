# Trakt Entities and Examples

This document is the practical reference for the current Trakt API and runtime semantics.

## 1) Core Entities

### Artifact (input artifact)

Represents one named input payload to the pipeline.

Fields:
- `name`: artifact id used in bindings (example: `source__records`)
- `kind`: adapter kind (default: `csv`)
- `uri`: file path, directory, or glob relative to `--input-dir` (or absolute path)
- `combine_strategy`: `concat` (default), `union_by_name`, or `validate_schema`
- `metadata`: adapter-specific options (CSV read options, required flag, etc.)
- `schema`: optional schema payload

`combine_strategy` applies only when `uri` resolves to multiple files:
- `concat`: concatenates files in order; requires identical columns
- `validate_schema`: requires identical columns and dtypes before concatenating
- `union_by_name`: unions columns by name, filling missing values

In `stream` execution mode, only `combine_strategy: concat` is currently supported.

`schema` is optional and is validated during input load (and per chunk in stream
mode). Supported shapes:
- list of column names
- mapping of `column_name: dtype_string`
- mapping with `columns: [...]` and optional `dtypes: {column: dtype}`

Schema validation is enforced by the CSV adapter; custom adapters may define
their own validation behavior.

Example:

```yaml
inputs:
  source__records:
    uri: records.csv
    schema:
      columns: [id, amount]
      dtypes:
        id: int64
        amount: float64
```

YAML example:

```yaml
inputs:
  source__records:
    kind: csv
    uri: records/*.csv
    combine_strategy: concat
    metadata:
      delimiter: auto
```

### OutputDataset (output declaration)

Represents one named pipeline output and optional per-output write config.

Fields:
- `name`: output dataset name (example: `final`)
- `source`: produced artifact name from steps (YAML key: `from`)
- `kind`: optional output adapter override (`csv`, custom adapter kind, etc.)
- `uri`: optional output file path (relative to `--output-dir` or absolute)
- `metadata`: optional output adapter settings (for CSV, write options)

YAML example:

```yaml
outputs:
  datasets:
    - name: final
      from: records_norm
      kind: csv
      uri: exports/final_pipe.csv
      metadata:
        write_options:
          delimiter: "|"
```

### Step / ResolvedStep

A step is an executable unit that transforms data.

Step modules export:
- `run(ctx, **kwargs)` callable
- optional `run.declared_inputs = [...]`
- optional `run.declared_outputs = [...]`
- optional capabilities:
  - `run.supports_batch` (default `True`)
  - `run.supports_stream` (default `False`)

Important runtime rule:
- Output bindings are used only to map returned result keys to artifact names.
- Output artifact names are **not** passed into `run(...)` as arguments.

Example:

```python
def run(ctx, input, multiplier, currency):
    frame = input.copy()
    frame["amount"] = frame["amount"] * multiplier
    frame["currency"] = currency
    return {"output": frame}

run.declared_inputs = ["input", "multiplier", "currency"]
run.declared_outputs = ["output"]
```

### Pipeline

A validated chain of inputs, ordered steps, and outputs.

Validation includes:
- missing step inputs
- output collisions
- invalid/missing output source bindings
- invalid execution mode
- stream compatibility checks

### Runner

Runtime adapter that executes pipeline semantics.

Current runners:
- `LocalRunner`: local IO execution
- `GlueRunner`: local parity + Glue metadata contract
- `LambdaRunner`: local parity + `max_batch_rows` guard

### Context

Run context passed into each step:
- `run_id`, `pipeline_name`, `pipeline_version`
- metadata storage (`add_metadata/get_metadata`)
- telemetry event API (`emit_event`)

## 2) Binding Semantics

Bindings are declared under step `with:`.

Input/config bindings:
- artifact references by name (string)
- primitive literals (`int`, `float`, `bool`, `null`)
- explicit literal strings via `const`
- nested list/mapping combinations

Output bindings:
- map returned keys to artifact names
- must resolve to artifact-name strings

### Literal strings (`const`)

Because plain strings are interpreted as artifact references, literal strings must be explicit.

YAML:

```yaml
steps:
  - id: enrich
    uses: steps.enrich.currency
    with:
      input: source__records
      currency:
        const: usd
      output: records_enriched
```

Python DSL:

```python
from trakt import ref, step

spec = (
    step("enrich", run=enrich)
    .input(input=ref("source__records"))
    .params(currency="usd")
    .output(output=ref("records_enriched"))
)
```

Python DSL recommendation:
- use `.input(...)` for artifact references
- use `.params(...)` for literal config values
- use `.output(...)` for output artifact bindings
- `.in_(...)` / `.out(...)` remain available as aliases

When loading YAML, you can enable strict key validation:

```python
from trakt import load_pipeline_from_yaml

pipeline = load_pipeline_from_yaml("pipeline.yaml", strict_unknown_keys=True)
```

Strict mode raises on unknown fields in input/step/output definitions instead of
silently keeping them in metadata.

## 3) CSV Adapter Semantics

The CSV adapter only loads files with the `.csv` extension. Non-`.csv` inputs are rejected.

### Read options

You can provide CSV read options either as direct metadata keys or grouped under `read_options`.

Common options:
- `delimiter` (supports `"auto"` / `"sniff"`)
- `encoding`
- `header`
- `date_columns`
- `decimal`
- `quotechar`
- `escapechar`
- `skipinitialspace`
- `dtype`
- `na_values`
- `keep_default_na`

Example:

```yaml
inputs:
  source__records:
    uri: records.csv
    metadata:
      delimiter: auto
      read_options:
        encoding: utf-8
```

### Write options

Per-output CSV write options go under output dataset metadata.

Common options:
- `delimiter`
- `encoding`
- `header`
- `index`
- `decimal`
- `quotechar`
- `escapechar`
- `lineterminator`

Example:

```yaml
outputs:
  datasets:
    - name: final
      from: records_norm
      metadata:
        write_options:
          delimiter: "|"
```

### Stream execution (CSV v1)

To read artifacts as a stream, set the pipeline execution mode to `stream`.
Stream-capable steps receive iterators of DataFrame chunks and should return
iterators of chunks.

YAML:

```yaml
execution:
  mode: stream
```

Python step:

```python
def run(ctx, input):
    for chunk in input:
        frame = chunk.copy()
        frame["amount"] = frame["amount"] * 2
        yield frame

run.declared_inputs = ["input"]
run.declared_outputs = ["output"]
run.supports_stream = True
```

Tune chunk size with `--stream-chunk-size` (CSV adapters only).

## 4) Step Metrics Contract

Steps may return metrics using reserved key `__metrics__`.

Example:

```python
def run(ctx, input):
    # ...transform
    return {
        "output": input,
        "__metrics__": {
            "rows_dropped": 12,
            "matched": 205,
            "unmatched": 7,
        },
    }
```

Runtime behavior:
- `__metrics__` must be a mapping
- metrics are persisted in each step entry in `manifest.json`
- numeric/bool metrics are also attached to step OTEL span attributes with `metric.<name>`

## 5) Built-in Quality Gates

Trakt includes a built-in quality-gate step:
- `uses: trakt.steps.quality_gate`

Input bindings:
- `input`: dataframe payload
- `policy`: `QualityGatePolicy`-compatible mapping

Output binding:
- `output`

Policy supports:
- `mode`: `fail` (default) or `warn`
- `required_columns`: list of required columns
- `unique_keys`: list of key specs (single key or key list)
- `row_count.min` / `row_count.max`
- `max_null_ratio`: map of column -> threshold (`0.0` to `1.0`)
- `gate_modes`: optional per-gate override (`warn`/`fail`)

Example:

```yaml
- id: quality_gate
  uses: trakt.steps.quality_gate
  with:
    input: records_norm
    policy:
      const:
        mode: warn
        required_columns: [id, amount]
        unique_keys: [id]
        row_count:
          min: 1
        max_null_ratio:
          amount: 0.05
    output: records_validated
```

When mode is `warn`, violations emit `warning.quality_gate` events and continue.
When mode is `fail`, first violation raises `ValueError`.

## 6) End-to-End YAML Example

```yaml
name: customer_transactions
execution:
  mode: batch

inputs:
  source__records:
    uri: records/*.csv
    combine_strategy: concat
    metadata:
      delimiter: auto

steps:
  - id: normalize
    uses: steps.normalize.amounts
    with:
      input: source__records
      currency:
        const: usd
      output: records_norm

  - id: quality_gate
    uses: trakt.steps.quality_gate
    with:
      input: records_norm
      policy:
        const:
          mode: warn
          required_columns: [id, amount]
          unique_keys: [id]
      output: records_valid

outputs:
  datasets:
    - name: final
      from: records_valid
      kind: csv
      uri: exports/final.csv
      metadata:
        write_options:
          delimiter: "|"
```

## 7) Python DSL Example

```python
from trakt import artifact, ref, step, workflow
from trakt.runtime.local_runner import LocalRunner


def normalize(ctx, input, currency):
    frame = input.copy()
    frame["currency"] = currency
    return {"output": frame, "__metrics__": {"matched": len(frame)}}


normalize.declared_inputs = ["input", "currency"]
normalize.declared_outputs = ["output"]

runner = LocalRunner(input_dir="data/input", output_dir="data/output")

result = (
    workflow("dsl_demo")
    .source(artifact("source__records").at("records.csv"))
    .step(
        step("normalize", run=normalize)
        .input(input=ref("source__records"))
        .params(currency="usd")
        .output(output=ref("records_norm"))
    )
    .output(
        "final",
        from_="records_norm",
        kind="csv",
        uri="exports/final_pipe.csv",
        metadata={"write_options": {"delimiter": "|"}},
    )
    .run(runner, run_id="dsl-run")
)
```

## 8) Manifest Shape (important fields)

`manifest.json` includes:
- run metadata (`run_id`, pipeline name/version, status, duration)
- input stats
- step list with:
  - `step_id`, `rows_in`, `rows_out`, `duration_ms`
  - `outputs` (artifact names produced by step)
  - `metrics` (from `__metrics__`)
  - compatibility fields (`rows_dropped`, `rows_unmatched`)
- output dataset metadata (`path`, `rows`, `kind`, `source`)
- error payload (on failure)

## 9) Migration Notes

If you are upgrading existing pipelines:
- remove output-name params from step handler signatures (they are no longer injected)
- keep `declared_outputs` so result keys are still mapped correctly
- wrap literal string configs with `const` in YAML (`{ const: ... }`)
- Python DSL can use `.params(...)` for literal configs; `const(...)` still works for legacy `.bind(...)`
- optionally move output adapter config from runner defaults into per-output `outputs.datasets` entries
