# External Validation Blockers

This file records why engineer-004 cannot be honestly marked externally verified from this machine yet.

## Current Readiness

- SDK_COMMAND: missing
- INGEST_ENDPOINT: missing
- aws_cli: missing
- aws_identity: missing
- k6: missing
- externally_ready: 0

## Blocking Items

- `SDK_COMMAND` is not set to a real production JavaScript SDK smoke command.
- `INGEST_ENDPOINT` is not set to an AWS/pre-prod ingestion endpoint.
- AWS CLI is not installed or not on `PATH`.
- k6 is not installed or not on `PATH`.

## Commands Once Access Exists

```bash
SDK_COMMAND='node path/to/real-sdk-smoke-test.js' python sdk_contract.py
SDK_COMMAND='node path/to/real-sdk-smoke-test.js' python pipeline.py bench
INGEST_ENDPOINT='https://preprod.example.com/ingest' RATE=5790 DURATION=30m k6 run load/ingest_spike_k6.js
SDK_COMMAND='node path/to/real-sdk-smoke-test.js' INGEST_ENDPOINT='https://preprod.example.com/ingest' python scripts/run_external_validation.py --run-id external-YYYYMMDD
aws sts get-caller-identity
aws ce get-cost-and-usage --time-period Start=YYYY-MM-DD,End=YYYY-MM-DD --granularity DAILY --metrics UnblendedCost
python scripts/build_submission_bundle.py --write
python scripts/verify_submission.py
```

## Evidence Needed

- `benchmark_results.txt` regenerated with `real_sdk_contract_verified = 1`.
- Full `sdk_contract.py` output for the production SDK command.
- k6 summary output from `load/ingest_spike_k6.js` at 5790 events/second.
- CloudWatch/ALB request count and p99 latency export.
- Redpanda topic offsets, consumer lag, and under-replicated partition checks.
- ClickHouse row counts by `run_id`, customer, event type, and time bucket.
- DLQ sample records for malformed inputs.
- AWS Pricing Calculator export or Cost Explorer run-rate proving the $50k/month ceiling.
