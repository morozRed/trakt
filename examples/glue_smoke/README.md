# Glue Smoke Demo

Run with Glue entrypoint contract:

```bash
PYTHONPATH=examples/glue_smoke python -m trakt.runtime.glue_main \
  --pipeline-file examples/glue_smoke/pipeline.yaml \
  --client-id demo \
  --batch-id smoke-20260205 \
  --input-dir examples/glue_smoke/input \
  --output-dir /tmp/trakt-glue-smoke-output \
  --job-name trakt-glue-smoke
```

Expected outputs:
- `/tmp/trakt-glue-smoke-output/smoke_result.csv`
- `/tmp/trakt-glue-smoke-output/manifest.json`
