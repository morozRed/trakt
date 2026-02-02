# Trakt ETL Framework

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

## Use Trakt From Another Project

If your project lives elsewhere, install this framework in editable mode:

```bash
python -m pip install -e /path/to/trakt
```

Then in your project code:

```python
from trakt import load_pipeline_from_yaml
from trakt.runtime.local_runner import LocalRunner
```

## Step Contract

Step modules must export `run(ctx, **kwargs)` and can declare bindings:

```python
def run(ctx, input, output):
    frame = input.copy()
    frame["amount"] = frame["amount"] * 2
    return {"output": frame}

run.declared_inputs = ["input"]
run.declared_outputs = ["output"]
```

## Pipeline YAML

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

## Run Locally

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

## Run Included Example

```bash
PYTHONPATH=examples/multi_file_demo python -m trakt.run_local \
  --pipeline-file examples/multi_file_demo/pipeline.yaml \
  --input-dir examples/multi_file_demo/input \
  --output-dir /tmp/trakt-demo-output
```

## Execution Semantics (Important)

Current MVP is in-memory batch processing:
- inputs are loaded as full CSV files into pandas DataFrames
- multi-file inputs are combined into one DataFrame per input artifact
- each step receives full DataFrame objects (not row-by-row records)
- step outputs stay in memory until final outputs are written

This means processing is not streaming/chunked yet, so very large datasets may require
future chunked IO or distributed/runtime extensions.

## Outputs

Each run writes:
- output CSV files defined by `outputs.datasets`
- `manifest.json` (default: `<output-dir>/manifest.json`)

`manifest.json` includes run status, timings, per-step stats, and error info on failure.

## Current Notes

- OpenTelemetry integration is intentionally deferred for MVP.
- OTEL exporter env vars are currently ignored (for example `OTEL_EXPORTER_OTLP_ENDPOINT`).
- Cloud runners (Glue/Lambda parity) are scaffolded but not implemented.
- Planning checklist lives in `TASKS.md`.
