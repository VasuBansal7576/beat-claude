# AWS Load Test Plan

This is the production validation plan for the local artifact. It has not been executed against AWS yet; it defines the gate that must pass before the local `benchmark_results.txt` can be treated as a production SLA.

## Load Shape

- [Assumed from brief] Current load: 50M events/day.
- [Estimated] Current average rate: 579 events/second (`50000000 / 86400`).
- [Estimated] 10x spike rate: 5790 events/second.
- [Assumed] Event payload: existing JavaScript SDK payload shape, no breaking SDK change.
- [Assumed] Test region: `us-east-1`, matching `cost_model.py`.

## Environment

- Three-AZ VPC with private subnets for Redpanda, ClickHouse, and workers.
- Public ALB in front of the Node.js ingestion service.
- Redpanda broker count starts at three.
- ClickHouse hot-query cluster starts at three nodes.
- S3 raw archive consumer and warehouse export consumer run as separate consumer groups.
- The old pipeline remains primary; this test runs shadow traffic only.

## Test Phases

1. Warm-up: 30 minutes at 579 events/second.
2. Sustained load: 2 hours at 579 events/second.
3. Spike: 30 minutes at 5790 events/second.
4. Recovery: 30 minutes at 579 events/second after the spike.
5. Failure injection: kill one writer, kill one Redpanda broker, pause ClickHouse writes for 10 minutes, and send malformed events.

## Load Driver

Use `load/ingest_spike_k6.js` from AWS load-generator hosts in the same region as the pre-production ingestion ALB:

```bash
INGEST_ENDPOINT='https://preprod.example.com/ingest' \
RUN_ID="aws-load-$(date +%Y%m%d%H%M%S)" \
RATE=5790 \
DURATION=30m \
k6 run load/ingest_spike_k6.js
```

Do not run the 5790 events/second spike from a laptop; the client-side network path becomes the bottleneck and invalidates the result.

## Pass/Fail Gates

- [Estimated] Accepted-event durability: `accepted_events == clickhouse_rows + dlq_rows` for the run.
- [Estimated] Dashboard freshness p99 stays below 5000 ms for the spike window.
- [Estimated] Consumer lag returns to zero within 120 seconds after each phase.
- [Estimated] Valid-event DLQ rate stays below 0.1 percent.
- [Estimated] Segment query p99 stays below 250 ms for the 50k-user pricing-page segment.
- [Estimated] Broker loss does not lose accepted events; replicas remain in sync after recovery.
- [Estimated] ClickHouse outage recovery replays from Redpanda without manual row patching.
- [Estimated] AWS cost run-rate remains below the `cost_model.py` budget envelope.

## Evidence To Attach After Running

- ALB request count and p99 latency export.
- Redpanda topic offsets, consumer lag, and under-replicated partition checks.
- ClickHouse row counts by `run_id`, customer, event type, and time bucket.
- DLQ sample records with malformed inputs.
- Dashboard freshness histogram.
- AWS Cost Explorer daily run-rate snapshot.
- Exact AWS Pricing Calculator export for the chosen instance sizes and region.
- k6 summary output from `load/ingest_spike_k6.js`.

## Rollback Rule

Do not cut over customers on this load test alone. Cutover requires human sign-off after old-vs-new parity checks pass by customer, event type, and time bucket, and after the real SDK retry gate in `sdk_contract.py` passes or `POSITION_3.md` is accepted as the product decision.
