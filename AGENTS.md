# Repository Guidelines

## Project Structure & Module Organization
- The repository root is the `etl` Python package (note `__init__.py` at the top level).
- Pipeline definitions live in `pipelines/<pipeline_name>/pipeline.yaml`.
- Transform steps are organized by type in `steps/normalize/`, `steps/dedupe/`, `steps/reshape/`, and `steps/enrich/`.
- Shared helpers live in `steps/shared.py`.
- Local execution entry point is `run_local.py`.

## Build, Test, and Development Commands
- Run a pipeline locally by name:
  `python3 -m etl.run_local --pipeline travel_enrichment__cytric --input-dir <in> --output-dir <out>`
- Run a pipeline from an explicit YAML path:
  `python3 -m etl.run_local --pipeline-file pipelines/travel_enrichment__cytric/pipeline.yaml --input-dir <in> --output-dir <out>`
- Override input discovery with explicit mappings:
  `--input westtours__cost_centers=/path/to/cost_centers.xlsx`
- Dependencies are imported directly in code (e.g., `pandas`, `pyyaml`, `openpyxl`); ensure they are installed in your environment.

## Coding Style & Naming Conventions
- Python, 4-space indentation, and PEP 8 conventions.
- Step modules are snake_case and must export `run(...)` (see `steps/*/*.py`).
- Pipeline names follow `<domain>__<primary_source>__<client?>` (see `README.md`).
- Input names should be `<source>__<logical_input>` and referenced consistently in `pipeline.yaml`.

## Testing Guidelines
- No test suite is present in this repository.
- If adding tests, place them under `tests/` and use `pytest`-style names (`test_*.py`).

## Commit & Pull Request Guidelines
- Git history is not available in this checkout, so no commit message convention is documented.
- Keep commits small and imperative (e.g., “Add cytric leg split reshape step”).
- PRs should include a short summary, pipeline/step files touched, and any sample input/output or schema changes.

## Data & Configuration Notes
- Do not commit raw client data; use anonymized samples.
- S3 path and pipeline/input naming conventions are documented in `README.md`.
