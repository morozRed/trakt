# Multi File Demo

Run from repo root (YAML pipeline):

```bash
PYTHONPATH=examples/multi_file_demo python3 -m trakt.run_local \
  --pipeline-file examples/multi_file_demo/pipeline.yaml \
  --input-dir examples/multi_file_demo/input \
  --output-dir /tmp/trakt-demo-output
```

Run the same demo with Python DSL:

```bash
PYTHONPATH=.:examples/multi_file_demo python3 examples/multi_file_demo/run_python_dsl.py \
  --input-dir examples/multi_file_demo/input \
  --output-dir /tmp/trakt-demo-output-dsl
```

Expected outputs:
- `/tmp/trakt-demo-output/final.csv`
- `/tmp/trakt-demo-output/manifest.json`
