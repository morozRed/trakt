# Repository Guidelines

## Project Structure & Module Organization
- Core framework package is `trakt/`.
- Main framework modules:
  - `trakt/core/` (artifacts, pipeline model, loader, step bindings, registry)
  - `trakt/runtime/` (runner base, local runner, Glue/Lambda stubs)
  - `trakt/io/` (CSV reader/writer)
  - `trakt/observability/` (manifest + OTEL helpers)
- Local CLI entrypoint is `trakt/run_local.py` (`python -m trakt.run_local`).
- Example pipeline and demo steps live in `examples/multi_file_demo/`.
- Tests live in `tests/`. Design and planning docs live in `docs/` and `TASKS.md`.

## Build, Test, and Development Commands
- Create env + install (editable):
  - `python3 -m venv .venv && source .venv/bin/activate`
  - `python -m pip install -e .`
- Install dev extras:
  - `python -m pip install -e ".[dev,excel]"`
- Run tests:
  - `python -m pytest -q`
- Run included example pipeline:
  - `PYTHONPATH=examples/multi_file_demo python -m trakt.run_local --pipeline-file examples/multi_file_demo/pipeline.yaml --input-dir examples/multi_file_demo/input --output-dir /tmp/trakt-demo-output`
- Run by pipeline name (when a project provides `pipelines/<name>/pipeline.yaml`):
  - `python -m trakt.run_local --pipeline <pipeline_name> --input-dir <in> --output-dir <out>`
- Override an input source:
  - `--input source__records=/path/to/file.csv`

## Coding Style & Naming Conventions
- Python, 4-space indentation, and PEP 8 conventions.
- Step handlers are snake_case modules that export `run(ctx, **kwargs)` and may define `declared_inputs` / `declared_outputs`.
- Keep framework modules focused by layer (`core`, `runtime`, `io`, `observability`).
- Pipeline and input naming conventions are documented in `README.md`.

## Testing Guidelines
- Use `pytest`; tests are already present under `tests/`.
- Add new tests as `tests/test_*.py`.
- Prefer unit tests for core validation/loader logic and integration tests for local runner behavior.

## Commit & Pull Request Guidelines
- Keep commits small and imperative (e.g., “Add cytric leg split reshape step”).
- PRs should include a short summary, modules touched, behavior changes, and test coverage updates.

## Data & Configuration Notes
- Do not commit raw client data; use anonymized samples.
- Example input/output fixtures should stay small and reviewable.
- Runtime notes and MVP scope are tracked in `README.md` and `TASKS.md`.
