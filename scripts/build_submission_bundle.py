#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "SUBMISSION_MANIFEST.txt"

SUBMISSION_FILES = [
    ".gitignore",
    "README.md",
    "SUBMISSION.md",
    "SUBMISSION_MANIFEST.txt",
    "PROOF_MATRIX.md",
    "EXTERNAL_VALIDATION_BLOCKERS.md",
    "POSITION_3.md",
    "SDK_EVIDENCE_CHECKLIST.md",
    "benchmark_results.txt",
    "cost_model.py",
    "cost_model_results.txt",
    "docker-compose.yml",
    "AWS_LOAD_TEST_PLAN.md",
    "load/ingest_spike_k6.js",
    "pipeline.py",
    "schema.sql",
    "sdk_contract.py",
    "scripts/build_submission_bundle.py",
    "scripts/check_external_readiness.py",
    "scripts/run_external_validation.py",
    "scripts/verify_submission.py",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def render_manifest() -> str:
    lines = [
        "# SUBMISSION_MANIFEST.txt",
        "",
        "Deterministic SHA-256 manifest for the Engineer-004 proof packet.",
        "The manifest intentionally excludes its own hash; rerun the checker after any artifact change.",
        "",
        "| Path | Bytes | SHA-256 |",
        "|---|---:|---|",
    ]
    for relative in SUBMISSION_FILES:
        if relative == "SUBMISSION_MANIFEST.txt":
            continue
        path = ROOT / relative
        data = path.read_bytes()
        lines.append(f"| `{relative}` | {len(data)} | `{hashlib.sha256(data).hexdigest()}` |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Render or verify the Engineer-004 submission manifest")
    parser.add_argument("--write", action="store_true", help="write SUBMISSION_MANIFEST.txt")
    parser.add_argument("--check", action="store_true", help="fail if SUBMISSION_MANIFEST.txt is stale")
    args = parser.parse_args()

    missing = [path for path in SUBMISSION_FILES if path != "SUBMISSION_MANIFEST.txt" and not (ROOT / path).exists()]
    if missing:
        print(f"missing submission files: {missing}")
        return 1

    rendered = render_manifest()
    if args.write:
        MANIFEST.write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0
    if args.check:
        if not MANIFEST.exists():
            print("SUBMISSION_MANIFEST.txt is missing; run: python scripts/build_submission_bundle.py --write")
            return 1
        current = MANIFEST.read_text(encoding="utf-8")
        if current != rendered:
            print("SUBMISSION_MANIFEST.txt is stale; run: python scripts/build_submission_bundle.py --write")
            return 1
        print("[PASS] SUBMISSION_MANIFEST.txt is current")
        return 0
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
