# ⚙️ Trakt ETL Framework

Trakt is a lightweight, YAML-first ETL framework for tabular pipelines.

Current MVP supports:
- local pipeline execution
- CSV input/output
- multi-file input combine strategies
- step resolution from module paths or alias registry
- per-run `manifest.json` output

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
```

You can optionally set pipeline execution mode:

```yaml
execution:
  mode: stream  # batch (default) or stream
```

Step modules must export `run(ctx, **kwargs)` and can declare bindings:

```python
def run(ctx, input, output):
    frame = input.copy()
    frame["amount"] = frame["amount"] * 2
    return {"output": frame}

run.declared_inputs = ["input"]
run.declared_outputs = ["output"]
```

For stream mode, mark steps as stream-capable and return chunk iterators:

```python
def run(ctx, input, output):
    def _iter_chunks():
        for chunk in input:
            frame = chunk.copy()
            frame["amount"] = frame["amount"] * 2
            yield frame
    return {"output": _iter_chunks()}

run.declared_inputs = ["input"]
run.declared_outputs = ["output"]
run.supports_stream = True
```

### Run from CLI (YAML-first)

You can run by explicit YAML path:

```bash
PYTHONPATH=/path/to/your/project python -m trakt.run_local \
  --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output
```

Or by pipeline name (`pipelines/<name>/pipeline.yaml`):

```bash
python -m trakt.run_local --pipeline <pipeline_name> --input-dir <in> --output-dir <out>
```

Override one input source:

```bash
python -m trakt.run_local \
  --pipeline-file /path/to/pipeline.yaml \
  --input-dir /path/to/input \
  --output-dir /path/to/output \
  --input source__records=/tmp/records.csv
```

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

### Run from Python API

If your project lives elsewhere, install this framework in editable mode:

```bash
python -m pip install -e /path/to/trakt
```

Then execute a pipeline programmatically:

```python
from trakt import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner

pipeline = load_pipeline_from_yaml("pipelines/my_pipeline/pipeline.yaml")
runner = LocalRunner(input_dir="data/input", output_dir="data/output")
result = runner.run(pipeline, run_id="local-dev")

print(result["status"])
print(result["manifest_path"])
```

Or define the workflow directly in Python:

```python
from trakt import workflow
from trakt.runtime.local_runner import LocalRunner


def double_amount(ctx, input, output):
    frame = input.copy()
    frame["amount"] = frame["amount"] * 2
    return {"output": frame}


double_amount.declared_inputs = ["input"]
double_amount.declared_outputs = ["output"]

runner = LocalRunner(input_dir="data/input", output_dir="data/output")
result = (
    workflow("python_workflow")
    .input("source__records", uri="records.csv")
    .step(
        "double_amount",
        run=double_amount,
        with_={"input": "source__records", "output": "records_norm"},
    )
    .output("final", from_="records_norm")
    .run(runner, run_id="py-dev")
)
```

## Run Included Example

```bash
PYTHONPATH=examples/multi_file_demo python -m trakt.run_local \
  --pipeline-file examples/multi_file_demo/pipeline.yaml \
  --input-dir examples/multi_file_demo/input \
  --output-dir /tmp/trakt-demo-output
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
- output CSV files defined by `outputs.datasets`
- `manifest.json` (default: `<output-dir>/manifest.json`)

`manifest.json` includes run status, timings, per-step stats, and error info on failure.

## Current Notes

- OpenTelemetry spans are emitted when `otel_enabled=True` is passed to `runner.run(...)`.
- Warning/coercion/missing-column events emitted via `ctx.emit_event(...)` are attached as span events.
- Cloud runners (Glue/Lambda parity) are scaffolded but not implemented.
- Planning checklist lives in `TASKS.md`.
