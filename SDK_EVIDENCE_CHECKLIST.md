# SDK Evidence Checklist

This checklist is the exact evidence needed to turn `real_sdk_contract_verified = 0` into `1`.

## Required Input

- Production JavaScript SDK source or package version under test.
- A smoke command that sends one logical event to `SDK_ENDPOINT`.
- The smoke command must read these environment variables:
  - `SDK_ENDPOINT`
  - `SDK_EVENT_NAME`
  - `SDK_CUSTOMER_ID`

Example shape:

```bash
SDK_COMMAND='node path/to/real-sdk-smoke-test.js' python sdk_contract.py
```

## Pass Criteria

`sdk_contract.py` must pass both retry probes:

- HTTP 503 retry: server returns `503` for the first request and `202` for the retry.
- Lost ACK retry: server receives and stores the first request, drops the TCP connection before ACK, and expects the SDK retry.

For both probes:

- Exactly two HTTP requests must arrive.
- Both requests must include `event_id`.
- The retry request must reuse the original `event_id`.

## Evidence To Attach

- Exact SDK package version or commit SHA.
- Exact `SDK_COMMAND` used.
- Full `sdk_contract.py` output.
- Updated `benchmark_results.txt` showing `real_sdk_contract_verified = 1`.

## If It Fails

Follow `POSITION_3.md`: do not claim query-time SDK retry dedup is production-safe for old SDKs, and do not add a server-side idempotency store to the hot path just to hide that contract failure.
