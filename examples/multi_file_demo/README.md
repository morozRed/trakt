# Multi File Demo

Run from repo root:

```bash
PYTHONPATH=examples/multi_file_demo python3 -m trakt.run_local \
  --pipeline-file examples/multi_file_demo/pipeline.yaml \
  --input-dir examples/multi_file_demo/input \
  --output-dir /tmp/trakt-demo-output
```

Expected outputs:
- `/tmp/trakt-demo-output/final.csv`
- `/tmp/trakt-demo-output/manifest.json`
