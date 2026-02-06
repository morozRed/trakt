# ⚙️ Trakt ETL Framework

Trakt is a lightweight, YAML-first ETL framework for tabular pipelines.

Current MVP supports:
- local pipeline execution
- CSV input/output
- multi-file input combine strategies
- step resolution from module paths or alias registry
- per-run `manifest.json` output

If you want the full API/entity reference with examples, start with:
- `docs/README.md`
- `docs/trakt-entities-and-examples.md`

## Install (Local Development)

From this repository:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

Optional extras:

```bash
python -m pip install -e ".[dev,excel]"
```

## Usage

### YAML pipeline definition

Define your pipeline in YAML:

```yaml
name: multi_file_demo

inputs:
  source__records:
    uri: records/*.csv
    combine_strategy: concat

steps:
  - id: double_amount
    uses: steps.normalize.double_amount
    with:
      input: source__records
      output: records_norm

outputs:
  datasets:
    - name: final
      from: records_norm
      kind: csv
      uri: exports/final.csv
      metadata:
        write_options:
          delimiter: "|"
```

`combine_strategy` applies when an input resolves to multiple files (globs or directories):
- `concat` (default): concatenates files in order; requires identical columns
- `validate_schema`: requires identical columns and dtypes before concatenating
- `union_by_name`: unions columns by name, filling missing values

In `stream` execution, only `combine_strategy: concat` is currently supported.

You can optionally set pipeline execution mode:

```yaml
execution:
  mode: stream  # batch (default) or stream
```

Step modules must export `run(ctx, **kwargs)` and can declare bindings:

```python
from trakt import step_contract


@step_contract(inputs=["input"], outputs=["output"])
def run(ctx, input):
    frame = input.copy()
    frame["amount"] = frame["amount"] * 2
    return {"output": frame}
```

Output bindings are used only to map returned result keys to artifact names.
Output artifact names are not passed into step handlers at runtime.

Literal string config must be explicit in YAML using `const`:

```yaml
steps:
  - id: normalize
    uses: steps.normalize.double_amount
    with:
      input: source__records
      currency:
        const: usd
      output: records_norm
```

Binding cheat sheet (`with:`):
- bare string (`source__records`) => artifact reference
- `const: "usd"` => literal string
- numbers/bools/null => literal values
- lists/maps are recursive (any bare string inside is still an artifact ref)

`outputs.datasets` supports per-output `kind`, `uri`, and `metadata`.
When omitted, runner-level defaults are used.

When `execution.mode: stream` is set, input bindings receive an iterator of
DataFrame chunks (CSV only) instead of a single DataFrame. For stream mode,
mark steps as stream-capable and return chunk iterators:

```python
from trakt import step_contract


@step_contract(
    inputs=["input"],
    outputs=["output"],
    supports_batch=False,
    supports_stream=True,
)
def run(ctx, input):
    def _iter_chunks():
        for chunk in input:
            frame = chunk.copy()
            frame["amount"] = frame["amount"] * 2
            yield frame
    return {"output": _iter_chunks()}
```

Steps can also return metrics using the reserved `__metrics__` key.
Those metrics are persisted automatically into `manifest.json`:

```python
def run(ctx, input):
    return {
        "output": input,
        "__metrics__": {
            "rows_dropped": 12,
            "matched": 205,
            "unmatched": 7,
        },
    }
```

CSV delimiter auto-detection is available via `metadata.delimiter: auto`
or explicit `metadata.read_options`. CSV inputs must be `.csv` files:

```yaml
inputs:
  source__records:
    uri: records.csv
    metadata:
      delimiter: auto
      read_options:
        encoding: utf-8
```

### Run from CLI (YAML-first)

The recommended entry point is the unified `trakt` CLI:

```bash
trakt run --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output
```

Or by pipeline name (`pipelines/<name>/pipeline.yaml`):

```bash
trakt run --pipeline <pipeline_name> --input-dir <in> --output-dir <out>
```

Step modules are auto-discovered relative to the pipeline file location
(no `PYTHONPATH` needed).

Override one input source:

```bash
python -m trakt.run_local \
  --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output \
  --input source__records=/tmp/records.csv
```

Override const bindings:

```bash
python -m trakt.run_local \
  --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output \
  --lenient \
  --param normalize.currency=usd \
  --param normalize.multiplier=2
```

Param values are parsed as YAML; quote values to force strings.

Override manifest destination:

```bash
python -m trakt.run_local \
  --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output \
  --manifest-path /path/to/output/custom-manifest.json
```

Tune stream chunk size (CSV stream mode):

```bash
python -m trakt.run_local \
  --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output \
  --stream-chunk-size 10000
```

### Run with Glue Contract

Glue entrypoint command:

```bash
python -m trakt.runtime.glue_main \
  --pipeline-file /path/to/pipeline.yaml \
  --client-id acme \
  --batch-id batch-20260205 \
  --input-dir /path/to/input \
  --output-dir /path/to/output \
  --job-name trakt-glue-demo
```

Required runtime args:
- `--client-id`
- `--batch-id`
- one of `--pipeline` or `--pipeline-file`
- `--input-dir`
- `--output-dir`

### Run from Python API

If your project lives elsewhere, install this framework in editable mode:

```bash
python -m pip install -e /path/to/trakt
```

Then execute a pipeline programmatically:

```python
from trakt import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner

pipeline = load_pipeline_from_yaml(
    "pipelines/my_pipeline/pipeline.yaml",
    strict_unknown_keys=True,
)
runner = LocalRunner(input_dir="data/input", output_dir="data/output")
result = runner.run(pipeline, run_id="local-dev")

print(result["status"])
print(result["manifest_path"])
```

Strict validation of unknown fields is enabled by default. Use `strict_unknown_keys=False`
(or CLI `--lenient`) to allow unknown fields in input/step/output definitions.

Or define the workflow directly in Python:

```python
from trakt import artifact, ref, step, step_contract, workflow
from trakt.runtime.local_runner import LocalRunner


@step_contract(inputs=["input"], outputs=["output"])
def double_amount(ctx, input):
    frame = input.copy()
    frame["amount"] = frame["amount"] * 2
    return {"output": frame}

source_records = artifact("source__records").as_kind("csv").at("records.csv")
double_step = (
    step("double_amount", run=double_amount)
    .input(input=ref("source__records"))
    .output(output=ref("records_norm"))
)

runner = LocalRunner(input_dir="data/input", output_dir="data/output")
result = (
    workflow("python_workflow")
    .source(source_records)
    .steps([double_step])
    .output("final", from_="records_norm")
    .run(runner, run_id="py-dev")
)
```

Preferred Python DSL pattern:
- `.input(...)` for artifact references
- `.params(...)` for literal config values
- `.output(...)` for output artifact bindings

Example with literal strings (no `const(...)` required):

```python
step("normalize", run=double_amount).input(input=ref("source__records")).params(currency="usd").output(output=ref("records_norm"))
```

Use `.input(...)` for artifact refs, `.params(...)` for literals, `.output(...)` for output bindings.

Built-in quality gate step:

```yaml
- id: quality_gate
  uses: trakt.steps.quality_gate
  with:
    input: records_norm
    policy:
      const:
        mode: warn        # warn or fail
        required_columns: [id, amount]
        unique_keys: [id]
        row_count:
          min: 1
        max_null_ratio:
          amount: 0.05
    output: records_validated
```

Multiple inputs for one step:

```python
from trakt import artifact, ref, step, step_contract

input_1 = artifact("source__records").at("records.csv")
input_2 = artifact("source__countries").at("countries.csv")


@step_contract(inputs=["inputs"], outputs=["output"])
def join_inputs(ctx, inputs):
    left, right = inputs
    return {"output": left.merge(right, on="id", how="left")}

join_step = (
    step("join_inputs", run=join_inputs)
    .input(inputs=[ref("source__records"), ref("source__countries")])
    .output(output=ref("records_joined"))
)
```

### Happy Path One-Liners

Python DSL (80% case):

```python
step("normalize", run=normalize).input(input=ref("source__records")).params(currency="usd").output(output=ref("records_norm"))
```

CLI (80% case):

```bash
python -m trakt.run_local --pipeline-file pipelines/demo/pipeline.yaml --input-dir data/in --output-dir data/out --lenient
```

## Run Included Example

```bash
trakt run --pipeline-file examples/multi_file_demo/pipeline.yaml \
  --input-dir examples/multi_file_demo/input \
  --output-dir /tmp/trakt-demo-output
```

Glue smoke example (anonymized):

```bash
python -m trakt.runtime.glue_main \
  --pipeline-file examples/glue_smoke/pipeline.yaml \
  --client-id demo \
  --batch-id smoke-20260205 \
  --input-dir examples/glue_smoke/input \
  --output-dir /tmp/trakt-glue-smoke-output \
  --job-name trakt-glue-smoke
```

## Execution Semantics (Important)

`batch` mode (default):
- inputs are loaded as full CSV files into pandas DataFrames
- multi-file inputs are combined into one DataFrame per input artifact
- each step receives full DataFrame objects
- step outputs stay in memory until final outputs are written

`stream` mode (CSV, v1):
- inputs are read in chunks (`--stream-chunk-size`)
- stream-capable steps receive chunk iterators and return chunk iterators
- CSV outputs are written incrementally chunk-by-chunk
- multi-file stream combine currently supports `concat` only

## Outputs

Each run writes:
- output artifacts defined by `outputs.datasets`
- `manifest.json` (default: `<output-dir>/manifest.json`)

`manifest.json` includes run status, timings, per-step stats/metrics, and error info on failure.

## Current Notes

- OpenTelemetry spans are emitted when `otel_enabled=True` is passed to `runner.run(...)`.
- Warning/coercion/missing-column events emitted via `ctx.emit_event(...)` are attached as span events.
- Glue/Lambda runners currently provide local parity wrappers (Lambda enforces `max_batch_rows`).
- Glue deployment runbook lives in `docs/trakt-glue-deployment.md`.
- Planning checklist lives in `TASKS.md`.
