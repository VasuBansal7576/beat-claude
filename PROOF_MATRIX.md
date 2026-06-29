# Engineer-004 Proof Matrix

This maps the brief's required packet and hard constraints to the exact proof artifact in this repo.

## Required Submission Packet

| Brief requirement | Evidence in packet | Proof status |
|---|---|---|
| Written answer | `SUBMISSION.md` | Complete |
| Operating artifact | `docker-compose.yml`, `pipeline.py`, `schema.sql`, `sdk_contract.py`, `load/ingest_spike_k6.js` | Complete for local artifact; AWS execution still external |
| Evidence log | `SUBMISSION.md` artifact evidence table, `benchmark_results.txt`, `cost_model_results.txt`, `SUBMISSION_MANIFEST.txt`, `EXTERNAL_VALIDATION_BLOCKERS.md` | Complete |
| Number source labels | `benchmark_results.txt`, `cost_model_results.txt`, source labels inside `SUBMISSION.md` | Complete; enforced by `scripts/verify_submission.py` |
| AI usage disclosure | `SUBMISSION.md` section "AI Usage Disclosure" | Complete |
| What breaks it | `SUBMISSION.md` section "Tradeoffs And Failure Modes", `POSITION_3.md`, `SDK_EVIDENCE_CHECKLIST.md` | Complete |
| What stays human | `SUBMISSION.md` section "What Stays Human" | Complete |

## Brief Constraints

| Constraint | Evidence in packet | Proof status |
|---|---|---|
| Runs on AWS | `AWS_LOAD_TEST_PLAN.md`, `cost_model_results.txt`, `load/ingest_spike_k6.js` | Planned and cost-modeled; AWS execution still external |
| No breaking SDK update | `sdk_contract.py`, `SDK_EVIDENCE_CHECKLIST.md`, `POSITION_3.md` | Honest gate; real SDK proof still external |
| Multi-tenant 500+ customers | `SUBMISSION.md` explains `customer_id` keying and ClickHouse primary key; `schema.sql` implements `(customer_id, timestamp, event_id)` ordering | Complete design proof |
| SOC 2, GDPR, CCPA | `SUBMISSION.md` PII separation plan, `pipeline.py` property redaction, `benchmark_results.txt` PII probe | Partial local guardrail; full compliance program still external |
| Real-time below 5 seconds | `benchmark_results.txt` local p99 `3993 ms`, `AWS_LOAD_TEST_PLAN.md` p99 gate, `load/ingest_spike_k6.js` threshold `p(99)<5000` | Complete local proof; production proof still external |
| 50M events/day and 10x spike | `benchmark_results.txt` local 5790-event spike sample, `AWS_LOAD_TEST_PLAN.md` 5790 events/sec production gate, `load/ingest_spike_k6.js` rate driver | Local sample plus executable production gate |
| Zero data loss / reliability claim | `SUBMISSION.md` explicitly rejects literal zero-data-loss and exactly-once claims; uses at-least-once durable log plus replay and query dedup | Honest scoped claim |
| Migration without breaking existing integrations | `SUBMISSION.md` CDC/shadow migration plan and human sign-off gate | Complete design proof |

## Reviewer Commands

```bash
docker compose up -d
python pipeline.py bench
python cost_model.py --check
python scripts/build_submission_bundle.py --check
python scripts/verify_submission.py
```

## Remaining External Evidence

- Run `SDK_EVIDENCE_CHECKLIST.md` against the production JavaScript SDK to prove `real_sdk_contract_verified = 1`.
- Run `AWS_LOAD_TEST_PLAN.md` with `load/ingest_spike_k6.js` against an AWS/pre-prod endpoint to convert the local latency proof into production SLA evidence.
- Attach an AWS Pricing Calculator export for exact instance classes and region before production approval.
- See `EXTERNAL_VALIDATION_BLOCKERS.md` for the current machine-readable blocker list and exact commands.
- Use `scripts/run_external_validation.py` to capture SDK, k6, AWS identity, and Cost Explorer evidence once external access exists.
