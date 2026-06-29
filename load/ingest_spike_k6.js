import http from "k6/http";
import { check } from "k6";

const rate = Number(__ENV.RATE || "5790");
const duration = __ENV.DURATION || "30m";
const preAllocatedVUs = Number(__ENV.PREALLOCATED_VUS || "1500");
const maxVUs = Number(__ENV.MAX_VUS || "6000");
const endpoint = __ENV.INGEST_ENDPOINT;
const customerId = __ENV.CUSTOMER_ID || "aws_load_test_customer";
const runId = __ENV.RUN_ID || `aws-load-${Date.now()}`;

if (!endpoint) {
  throw new Error("INGEST_ENDPOINT is required, for example https://ingest.example.com/ingest");
}

export const options = {
  scenarios: {
    ingest_spike: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.001"],
    http_req_duration: ["p(99)<5000"],
  },
};

export default function () {
  const id = `${runId}-${__VU}-${__ITER}`;
  const now = Date.now();
  const payload = JSON.stringify({
    event_id: id,
    customer_id: customerId,
    timestamp: new Date(now).toISOString(),
    received_at: new Date(now).toISOString(),
    sent_at_ms: now,
    run_id: runId,
    user_hash: `load_user_${__VU}_${__ITER % 50000}`,
    session_id: `load_session_${__VU}`,
    event_type: "page_view",
    event_name: __ITER % 3 === 0 ? "pricing" : "page_view",
    properties: {
      path: __ITER % 3 === 0 ? "/pricing" : "/",
      load_test: true,
    },
  });
  const response = http.post(endpoint, payload, {
    headers: { "Content-Type": "application/json" },
    timeout: "5s",
  });
  check(response, {
    "accepted": (r) => r.status === 202,
  });
}
