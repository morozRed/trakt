# Trakt Glue Deployment Runbook

This runbook covers packaging, deployment, rollout, rollback, and smoke testing
for Glue-based Trakt pipeline runs.

## 1) Build and publish artifacts

Build framework + dependency artifacts:

```bash
python3 scripts/package_glue_artifacts.py --output-dir dist/glue
```

Publish directly to S3 (optional):

```bash
python3 scripts/package_glue_artifacts.py \
  --output-dir dist/glue \
  --s3-prefix s3://my-bucket/trakt/releases/v1
```

Artifacts produced:
- `framework/*.whl` and `framework/*.tar.gz`
- `dependencies.zip`
- `artifact_manifest.json` (checksums + sizes)

## 2) Configure environment (dev/stage/prod)

Terraform scaffolding lives under:
- `infra/terraform/envs/dev`
- `infra/terraform/envs/stage`
- `infra/terraform/envs/prod`

Each environment contains `terraform.tfvars.example`.
Copy to `terraform.tfvars` and set:
- Glue script S3 path (`script_s3_path`)
- wheel S3 path (`extra_py_files_s3_path`)
- temp dir (`temp_dir`)
- input/output S3 prefixes
- tags and optional alarm SNS topic

## 3) Apply Terraform

Example for stage:

```bash
cd infra/terraform/envs/stage
terraform init
terraform plan -var-file terraform.tfvars
terraform apply -var-file terraform.tfvars
```

Module provisions:
- Glue job
- IAM role + least-privilege S3/log permissions
- CloudWatch log group
- CloudWatch failed-run alarm

## 4) Trigger Glue run

Glue runtime contract requires:
- `--client-id`
- `--batch-id`
- `--pipeline` or `--pipeline-file`
- `--input-dir`
- `--output-dir`

Example:

```bash
aws glue start-job-run \
  --job-name trakt-stage-etl \
  --arguments '{
    "--client-id":"acme",
    "--batch-id":"batch-20260205",
    "--pipeline":"travel_enrichment__cytric",
    "--input-dir":"s3://my-bucket-stage/raw/client=acme/batch=batch-20260205/",
    "--output-dir":"s3://my-bucket-stage/curated/client=acme/batch=batch-20260205/"
  }'
```

## 5) Smoke test checklist

Use anonymized input only.

Verify:
1. Glue run succeeds.
2. Output dataset exists in expected S3 prefix.
3. `manifest.json` exists and status is `success`.
4. Row counts and key columns match expected values.
5. CloudWatch logs contain no unexpected errors.

## 6) Rollout strategy

Recommended rollout:
1. Package and publish new versioned artifacts (`vN` prefix in S3).
2. Apply Terraform update in `dev`, run smoke test.
3. Promote to `stage`, run smoke test + data quality checks.
4. Promote to `prod` during change window.
5. Keep prior version artifacts available for rollback.

## 7) Rollback strategy

If run quality degrades:
1. Repoint Glue job to previous `script_s3_path` and `extra_py_files_s3_path`.
2. Re-apply Terraform in impacted environment.
3. Re-run last known-good batch.
4. Compare output/manifest with baseline before resuming normal schedules.
