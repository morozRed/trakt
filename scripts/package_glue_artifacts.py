#!/usr/bin/env python3
"""Build and optionally publish Trakt artifacts for AWS Glue jobs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build Trakt wheel/sdist + third-party dependency bundle for Glue."
    )
    parser.add_argument(
        "--output-dir",
        default="dist/glue",
        help="Directory where build artifacts will be written.",
    )
    parser.add_argument(
        "--requirements-file",
        default="requirements.txt",
        help="Requirements file used to build third-party dependency bundle.",
    )
    parser.add_argument(
        "--python-bin",
        default="python3",
        help="Python binary used for build commands.",
    )
    parser.add_argument(
        "--s3-prefix",
        default=None,
        help="Optional S3 destination prefix (e.g. s3://bucket/trakt/releases/v1).",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    output_dir = (repo_root / args.output_dir).resolve()
    requirements_file = (repo_root / args.requirements_file).resolve()
    if not requirements_file.exists():
        raise FileNotFoundError(f"Requirements file does not exist: {requirements_file}")

    framework_dir = output_dir / "framework"
    deps_dir = output_dir / "dependencies"
    deps_zip = output_dir / "dependencies.zip"
    manifest_path = output_dir / "artifact_manifest.json"

    if output_dir.exists():
        shutil.rmtree(output_dir)
    framework_dir.mkdir(parents=True)
    deps_dir.mkdir(parents=True)

    _run(
        [args.python_bin, "-m", "build", "--wheel", "--sdist", "--outdir", str(framework_dir)],
        cwd=repo_root,
        hint="Install build backend with: python3 -m pip install build",
    )
    _run(
        [
            args.python_bin,
            "-m",
            "pip",
            "download",
            "-r",
            str(requirements_file),
            "-d",
            str(deps_dir),
        ],
        cwd=repo_root,
    )

    _zip_directory(deps_dir, deps_zip)

    artifact_manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "framework": [_artifact_info(path) for path in sorted(framework_dir.iterdir())],
        "dependency_bundle": _artifact_info(deps_zip),
        "requirements_file": str(requirements_file.relative_to(repo_root)),
    }
    manifest_path.write_text(json.dumps(artifact_manifest, indent=2), encoding="utf-8")

    if args.s3_prefix:
        _publish_to_s3(output_dir=output_dir, s3_prefix=args.s3_prefix)

    print(f"Wrote Glue artifacts to: {output_dir}")
    print(f"Manifest: {manifest_path}")


def _artifact_info(path: Path) -> dict[str, str | int]:
    return {
        "name": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _zip_directory(source_dir: Path, target_zip: Path) -> None:
    if target_zip.exists():
        target_zip.unlink()
    archive_path = shutil.make_archive(
        base_name=str(target_zip.with_suffix("")),
        format="zip",
        root_dir=str(source_dir),
    )
    if Path(archive_path) != target_zip:
        os.replace(archive_path, target_zip)


def _publish_to_s3(*, output_dir: Path, s3_prefix: str) -> None:
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(output_dir).as_posix()
        destination = f"{s3_prefix.rstrip('/')}/{relative}"
        _run(["aws", "s3", "cp", str(path), destination], cwd=output_dir)


def _run(cmd: list[str], *, cwd: Path, hint: str | None = None) -> None:
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        if hint:
            raise RuntimeError(f"Command failed ({' '.join(cmd)}). {hint}") from exc
        raise


if __name__ == "__main__":
    main()
