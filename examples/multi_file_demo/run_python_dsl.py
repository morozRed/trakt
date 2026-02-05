"""Run the multi-file demo using the Python DSL."""

import argparse
import json

from steps.normalize.double_amount import run as double_amount
from trakt import artifact, ref, step, workflow
from trakt.runtime.local_runner import LocalRunner


def build_workflow():
    source_records = artifact("source__records").at("records/*.csv").combine("concat")
    normalize_step = (
        step("double_amount", run=double_amount)
        .input(records=source_records)
        .output(normalized=ref("records_norm"))
    )
    return (
        workflow("multi_file_demo")
        .source(source_records)
        .steps([normalize_step])
        .output("final", from_="records_norm")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi_file_demo with Python DSL.")
    parser.add_argument("--input-dir", required=True, help="Directory for demo input files")
    parser.add_argument("--output-dir", required=True, help="Directory for output artifacts")
    parser.add_argument("--run-id", default="multi-file-dsl", help="Optional explicit run id")
    args = parser.parse_args()

    runner = LocalRunner(input_dir=args.input_dir, output_dir=args.output_dir)
    result = build_workflow().run(runner, run_id=args.run_id)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
