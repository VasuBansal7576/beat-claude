# Position 3: SDK Retry Contract Fails

If `SDK_COMMAND='...' python sdk_contract.py` fails against the real production SDK, Path A is invalid.

If `python sdk_contract.py` exits `2`, the real SDK was not supplied to the harness. That is not a pass. It means Path A remains unverified and should not be claimed.

If the harness exits `1`, the existing SDK does not reuse the same `event_id` across retries. Server-side `count(DISTINCT event_id)` cannot reliably collapse retry duplicates because each retry looks like a new logical event.

## Decision

Do not add an idempotency store to save this design inside the hot path. It would add a fourth stateful component, increase operational risk, and violate the simplicity constraint that makes this plan better for a two-senior-engineer team.

## What To Document Instead

- Raw storage is at-least-once only.
- Duplicate rate can be measured but not fully removed server-side.
- Segment queries may overcount retry-generated events until the SDK contract changes.
- The safe fix is a non-breaking SDK patch that generates `event_id` once per event before retry attempts.
- Until then, dashboard numbers should disclose retry duplicate sensitivity.

## Migration Implication

Proceed with Redpanda and ClickHouse as the simpler analytics backbone, but mark server-side SDK-retry dedup as unsupported until the real SDK passes this gate. Compare new-pipeline counts against the existing system during shadow traffic to quantify duplicate sensitivity before customer rollout.
