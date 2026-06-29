#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BLOCKERS = ROOT / "EXTERNAL_VALIDATION_BLOCKERS.md"


@dataclass(frozen=True)
class Readiness:
    sdk_command_set: bool
    ingest_endpoint_set: bool
    aws_cli_present: bool
    aws_identity_available: bool
    k6_present: bool

    @property
    def externally_ready(self) -> bool:
        return (
            self.sdk_command_set
            and self.ingest_endpoint_set
            and self.aws_cli_present
            and self.aws_identity_available
            and self.k6_present
        )


def aws_identity_available() -> bool:
    if shutil.which("aws") is None:
        return False
    completed = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--output", "json"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )
    return completed.returncode == 0


def inspect_readiness() -> Readiness:
    return Readiness(
        sdk_command_set=bool(os.environ.get("SDK_COMMAND", "").strip()),
        ingest_endpoint_set=bool(os.environ.get("INGEST_ENDPOINT", "").strip()),
        aws_cli_present=shutil.which("aws") is not None,
        aws_identity_available=aws_identity_available(),
        k6_present=shutil.which("k6") is not None,
    )


def status(value: bool) -> str:
    return "present" if value else "missing"


def render(readiness: Readiness) -> str:
    blockers = []
    if not readiness.sdk_command_set:
        blockers.append("`SDK_COMMAND` is not set to a real production JavaScript SDK smoke command.")
    if not readiness.ingest_endpoint_set:
        blockers.append("`INGEST_ENDPOINT` is not set to an AWS/pre-prod ingestion endpoint.")
    if not readiness.aws_cli_present:
        blockers.append("AWS CLI is not installed or not on `PATH`.")
    if readiness.aws_cli_present and not readiness.aws_identity_available:
        blockers.append("AWS CLI is present but `aws sts get-caller-identity` did not succeed.")
    if not readiness.k6_present:
        blockers.append("k6 is not installed or not on `PATH`.")

    lines = [
        "# External Validation Blockers",
        "",
        "This file records why engineer-004 cannot be honestly marked externally verified from this machine yet.",
        "",
        "## Current Readiness",
        "",
        f"- SDK_COMMAND: {status(readiness.sdk_command_set)}",
        f"- INGEST_ENDPOINT: {status(readiness.ingest_endpoint_set)}",
        f"- aws_cli: {status(readiness.aws_cli_present)}",
        f"- aws_identity: {status(readiness.aws_identity_available)}",
        f"- k6: {status(readiness.k6_present)}",
        f"- externally_ready: {int(readiness.externally_ready)}",
        "",
        "## Blocking Items",
        "",
    ]
    if blockers:
        lines.extend(f"- {blocker}" for blocker in blockers)
    else:
        lines.append("- None. Run the external validation commands below.")

    lines.extend(
        [
            "",
            "## Commands Once Access Exists",
            "",
            "```bash",
            "SDK_COMMAND='node path/to/real-sdk-smoke-test.js' python sdk_contract.py",
            "SDK_COMMAND='node path/to/real-sdk-smoke-test.js' python pipeline.py bench",
            "INGEST_ENDPOINT='https://preprod.example.com/ingest' RATE=5790 DURATION=30m k6 run load/ingest_spike_k6.js",
            "SDK_COMMAND='node path/to/real-sdk-smoke-test.js' INGEST_ENDPOINT='https://preprod.example.com/ingest' python scripts/run_external_validation.py --run-id external-YYYYMMDD",
            "aws sts get-caller-identity",
            "aws ce get-cost-and-usage --time-period Start=YYYY-MM-DD,End=YYYY-MM-DD --granularity DAILY --metrics UnblendedCost",
            "python scripts/build_submission_bundle.py --write",
            "python scripts/verify_submission.py",
            "```",
            "",
            "## Evidence Needed",
            "",
            "- `benchmark_results.txt` regenerated with `real_sdk_contract_verified = 1`.",
            "- Full `sdk_contract.py` output for the production SDK command.",
            "- k6 summary output from `load/ingest_spike_k6.js` at 5790 events/second.",
            "- CloudWatch/ALB request count and p99 latency export.",
            "- Redpanda topic offsets, consumer lag, and under-replicated partition checks.",
            "- ClickHouse row counts by `run_id`, customer, event type, and time bucket.",
            "- DLQ sample records for malformed inputs.",
            "- AWS Pricing Calculator export or Cost Explorer run-rate proving the $50k/month ceiling.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Check external validation readiness for engineer-004")
    parser.add_argument("--write", action="store_true", help="write EXTERNAL_VALIDATION_BLOCKERS.md")
    parser.add_argument("--check", action="store_true", help="fail if EXTERNAL_VALIDATION_BLOCKERS.md is stale")
    args = parser.parse_args()

    rendered = render(inspect_readiness())
    if args.write:
        BLOCKERS.write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0
    if args.check:
        if not BLOCKERS.exists():
            print("EXTERNAL_VALIDATION_BLOCKERS.md is missing; run: python scripts/check_external_readiness.py --write")
            return 1
        current = BLOCKERS.read_text(encoding="utf-8")
        if current != rendered:
            print("EXTERNAL_VALIDATION_BLOCKERS.md is stale; run: python scripts/check_external_readiness.py --write")
            return 1
        print("[PASS] EXTERNAL_VALIDATION_BLOCKERS.md is current")
        return 0
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
