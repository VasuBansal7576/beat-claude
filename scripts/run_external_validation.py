#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from check_external_readiness import inspect_readiness, render


ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str], output: Path, env: dict[str, str] | None = None, timeout: int | None = None) -> int:
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    output.write_text(completed.stdout, encoding="utf-8")
    return completed.returncode


def require_ok(code: int, step: str, output: Path) -> None:
    if code != 0:
        raise RuntimeError(f"{step} failed with exit {code}; see {output}")


def redact_aws_identity(raw: str) -> str:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if "Account" in data:
        data["Account"] = "[redacted]"
    if "Arn" in data:
        data["Arn"] = "[redacted]"
    if "UserId" in data:
        data["UserId"] = "[redacted]"
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run external engineer-004 validation once SDK/AWS/k6 access exists")
    parser.add_argument("--run-id", default=f"external-{int(time.time())}", help="run id for evidence folder and load test")
    parser.add_argument("--duration", default=os.environ.get("DURATION", "30m"), help="k6 load-test duration")
    parser.add_argument("--rate", default=os.environ.get("RATE", "5790"), help="k6 constant arrival rate")
    parser.add_argument("--evidence-dir", default="external_evidence", help="directory where evidence folders are written")
    parser.add_argument("--allow-missing-cost", action="store_true", help="skip Cost Explorer capture if account permissions block it")
    args = parser.parse_args()

    readiness = inspect_readiness()
    evidence_root = ROOT / args.evidence_dir / args.run_id
    evidence_root.mkdir(parents=True, exist_ok=True)
    (evidence_root / "readiness.md").write_text(render(readiness), encoding="utf-8")

    if not readiness.externally_ready:
        print(f"[BLOCKED] External validation prerequisites are missing. See {evidence_root / 'readiness.md'}")
        return 2

    sdk_command = os.environ["SDK_COMMAND"]
    ingest_endpoint = os.environ["INGEST_ENDPOINT"]
    env = os.environ.copy()

    sdk_output = evidence_root / "sdk_contract_output.txt"
    require_ok(run([sys.executable, "sdk_contract.py"], sdk_output, env=env, timeout=60), "sdk contract", sdk_output)

    bench_output = evidence_root / "pipeline_bench_with_sdk_output.txt"
    require_ok(run([sys.executable, "pipeline.py", "bench"], bench_output, env=env, timeout=600), "pipeline bench", bench_output)

    k6_env = env | {
        "INGEST_ENDPOINT": ingest_endpoint,
        "RUN_ID": args.run_id,
        "RATE": str(args.rate),
        "DURATION": args.duration,
    }
    k6_summary = evidence_root / "k6_summary.json"
    k6_output = evidence_root / "k6_output.txt"
    require_ok(
        run(["k6", "run", "--summary-export", str(k6_summary), "load/ingest_spike_k6.js"], k6_output, env=k6_env, timeout=60 * 60),
        "k6 load test",
        k6_output,
    )

    aws_identity_raw = evidence_root / "aws_identity_redacted.json"
    require_ok(
        run(["aws", "sts", "get-caller-identity", "--output", "json"], aws_identity_raw, env=env, timeout=30),
        "aws identity",
        aws_identity_raw,
    )
    aws_identity_raw.write_text(redact_aws_identity(aws_identity_raw.read_text(encoding="utf-8")), encoding="utf-8")

    cost_output = evidence_root / "aws_cost_explorer.json"
    today = dt.date.today()
    default_cost_period = f"Start={(today - dt.timedelta(days=7)).isoformat()},End={today.isoformat()}"
    cost_cmd = [
        "aws",
        "ce",
        "get-cost-and-usage",
        "--time-period",
        os.environ.get("AWS_COST_TIME_PERIOD", default_cost_period),
        "--granularity",
        "DAILY",
        "--metrics",
        "UnblendedCost",
        "--output",
        "json",
    ]
    cost_code = run(cost_cmd, cost_output, env=env, timeout=60)
    if cost_code != 0 and not args.allow_missing_cost:
        require_ok(cost_code, "aws cost explorer", cost_output)

    manifest = {
        "run_id": args.run_id,
        "sdk_command_present": bool(sdk_command),
        "ingest_endpoint_present": bool(ingest_endpoint),
        "rate": args.rate,
        "duration": args.duration,
        "evidence_dir": str(evidence_root.relative_to(ROOT)),
        "cost_explorer_captured": cost_code == 0,
        "next_steps": [
            "Attach CloudWatch/ALB p99 latency export.",
            "Attach Redpanda topic offsets, consumer lag, and under-replicated partition checks.",
            "Attach ClickHouse row counts by run_id, customer, event type, and time bucket.",
            "Regenerate SUBMISSION_MANIFEST.txt after copying final evidence into the repo.",
        ],
    }
    (evidence_root / "external_validation_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[PASS] External validation evidence captured in {evidence_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
