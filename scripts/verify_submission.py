#!/usr/bin/env python3
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_TRACKED_FILES = [
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

REQUIRED_BENCHMARK_MARKERS = [
    "real_sdk_contract_verified =",
    "e2e_latency_p99_ms =",
    "poison_events_seen_on_dlq_topic =",
    "pii_probe_email_redacted = 1",
    "pii_probe_phone_redacted = 1",
    "pii_probe_ip_redacted = 1",
]

REQUIRED_COST_MARKERS = [
    "budget_ceiling_usd = 50000",
    "estimated_monthly_total_usd =",
    "budget_pass = 1",
]

REQUIRED_LOAD_TEST_MARKERS = [
    "50M events/day",
    "5790 events/second",
    "Pass/Fail Gates",
    "accepted_events == clickhouse_rows + dlq_rows",
    "load/ingest_spike_k6.js",
]

REQUIRED_LOAD_DRIVER_MARKERS = [
    "constant-arrival-rate",
    "p(99)<5000",
    "INGEST_ENDPOINT",
    "RATE || \"5790\"",
]

REQUIRED_PROOF_MATRIX_MARKERS = [
    "Required Submission Packet",
    "Brief Constraints",
    "Remaining External Evidence",
    "SDK_EVIDENCE_CHECKLIST.md",
    "AWS_LOAD_TEST_PLAN.md",
]

REQUIRED_EXTERNAL_BLOCKER_MARKERS = [
    "externally_ready: 0",
    "SDK_COMMAND",
    "INGEST_ENDPOINT",
    "aws_cli",
    "k6",
    "Evidence Needed",
    "scripts/run_external_validation.py",
]


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(cmd, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and completed.returncode != 0:
        raise AssertionError(f"{' '.join(cmd)} failed:\n{completed.stdout}")
    return completed


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def check_required_files_exist() -> None:
    missing = [path for path in REQUIRED_TRACKED_FILES if not (ROOT / path).exists()]
    require(not missing, f"missing required submission files: {missing}")


def check_required_files_are_tracked() -> None:
    untracked = []
    for path in REQUIRED_TRACKED_FILES:
        completed = run(["git", "ls-files", "--error-unmatch", path], check=False)
        if completed.returncode != 0:
            untracked.append(path)
    require(not untracked, f"required files are not tracked/staged for git submission: {untracked}")


def check_benchmark_source_labels() -> None:
    text = (ROOT / "benchmark_results.txt").read_text(encoding="utf-8")
    unlabeled_metrics = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if line.startswith("- ") and not re.match(r"- \[(Observed|Estimated|Assumed|Benchmarked)\] ", line):
            unlabeled_metrics.append(f"{line_no}: {line}")
    require(not unlabeled_metrics, "benchmark metrics without source labels:\n" + "\n".join(unlabeled_metrics))
    for marker in REQUIRED_BENCHMARK_MARKERS:
        require(marker in text, f"benchmark_results.txt missing marker: {marker}")


def check_cost_model() -> None:
    run([sys.executable, "cost_model.py", "--check"])
    text = (ROOT / "cost_model_results.txt").read_text(encoding="utf-8")
    unlabeled_metrics = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if line.startswith("- ") and not re.match(r"- \[(Observed|Estimated|Assumed|Benchmarked)\] ", line):
            unlabeled_metrics.append(f"{line_no}: {line}")
    require(not unlabeled_metrics, "cost metrics without source labels:\n" + "\n".join(unlabeled_metrics))
    for marker in REQUIRED_COST_MARKERS:
        require(marker in text, f"cost_model_results.txt missing marker: {marker}")


def check_submission_manifest() -> None:
    run([sys.executable, "scripts/build_submission_bundle.py", "--check"])


def check_proof_matrix() -> None:
    text = (ROOT / "PROOF_MATRIX.md").read_text(encoding="utf-8")
    for marker in REQUIRED_PROOF_MATRIX_MARKERS:
        require(marker in text, f"PROOF_MATRIX.md missing marker: {marker}")


def check_external_readiness() -> None:
    run([sys.executable, "scripts/check_external_readiness.py", "--check"])
    text = (ROOT / "EXTERNAL_VALIDATION_BLOCKERS.md").read_text(encoding="utf-8")
    for marker in REQUIRED_EXTERNAL_BLOCKER_MARKERS:
        require(marker in text, f"EXTERNAL_VALIDATION_BLOCKERS.md missing marker: {marker}")


def check_load_test_plan() -> None:
    text = (ROOT / "AWS_LOAD_TEST_PLAN.md").read_text(encoding="utf-8")
    for marker in REQUIRED_LOAD_TEST_MARKERS:
        require(marker in text, f"AWS_LOAD_TEST_PLAN.md missing marker: {marker}")
    driver = (ROOT / "load/ingest_spike_k6.js").read_text(encoding="utf-8")
    for marker in REQUIRED_LOAD_DRIVER_MARKERS:
        require(marker in driver, f"load/ingest_spike_k6.js missing marker: {marker}")


def check_submission_guardrails() -> None:
    submission = (ROOT / "SUBMISSION.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for marker in [
    "real_sdk_contract_verified",
    "SDK_EVIDENCE_CHECKLIST.md",
        "AWS load tests",
        "PII",
        "cost_model_results.txt",
        "AWS_LOAD_TEST_PLAN.md",
        "python scripts/verify_submission.py",
    ]:
        require(marker in submission or marker in readme, f"submission packet missing guardrail marker: {marker}")


def check_python_compiles() -> None:
    run(
        [
            sys.executable,
            "-m",
            "py_compile",
            "pipeline.py",
            "sdk_contract.py",
            "cost_model.py",
            "scripts/build_submission_bundle.py",
            "scripts/check_external_readiness.py",
            "scripts/run_external_validation.py",
            "scripts/verify_submission.py",
        ]
    )


def check_sdk_gate_is_honest_when_missing() -> None:
    completed = run([sys.executable, "sdk_contract.py"], check=False)
    require(completed.returncode == 2, f"sdk_contract.py without SDK_COMMAND must exit 2, got {completed.returncode}")
    require("[UNVERIFIED]" in completed.stdout, "sdk_contract.py missing UNVERIFIED output without SDK_COMMAND")


def main() -> int:
    checks = [
        check_required_files_exist,
        check_required_files_are_tracked,
        check_benchmark_source_labels,
        check_cost_model,
        check_submission_manifest,
        check_proof_matrix,
        check_external_readiness,
        check_load_test_plan,
        check_submission_guardrails,
        check_python_compiles,
        check_sdk_gate_is_honest_when_missing,
    ]
    for check in checks:
        check()
        print(f"[PASS] {check.__name__}")
    print("[PASS] submission packet is self-contained and source-labeled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
