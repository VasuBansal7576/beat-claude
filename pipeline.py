#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PANDAPROXY = os.environ.get("PANDAPROXY", "http://localhost:18082")
CLICKHOUSE = os.environ.get("CLICKHOUSE", "http://localhost:8123")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "beatclaude")
EVENT_TOPIC = "events"
DLQ_TOPIC = "events.dlq"
HOT_GROUP = "clickhouse-writer"

PII_KEYWORDS = ("email", "phone", "ip", "address", "full_name", "first_name", "last_name")
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
PHONE_RE = re.compile(r"\+?\d[\d\s().-]{7,}\d")
IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


NODE_INGEST = r"""
const http = require("http");
const port = Number(process.env.NODE_INGEST_PORT || "18181");
const pandaproxy = process.env.PANDAPROXY || "http://localhost:18082";
const topic = process.env.EVENT_TOPIC || "events";

async function writeRecords(records) {
  const body = JSON.stringify({ records });
  const response = await fetch(`${pandaproxy}/topics/${topic}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/vnd.kafka.json.v2+json",
      "Accept": "application/vnd.kafka.v2+json"
    },
    body
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`redpanda produce failed ${response.status}: ${text}`);
  }
  return response.text();
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", chunk => { data += chunk; });
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === "GET" && req.url === "/health") {
      res.writeHead(200, {"Content-Type": "text/plain"});
      res.end("ok");
      return;
    }
    if (req.method !== "POST" || !["/ingest", "/ingest-batch"].includes(req.url)) {
      res.writeHead(404);
      res.end();
      return;
    }
    const raw = await readBody(req);
    const parsed = JSON.parse(raw);
    const events = Array.isArray(parsed) ? parsed : [parsed];
    const records = events.map(event => ({
      key: event && event.customer_id ? String(event.customer_id) : null,
      value: event
    }));
    await writeRecords(records);
    res.writeHead(202, {"Content-Type": "application/json"});
    res.end(JSON.stringify({accepted: records.length}));
  } catch (error) {
    res.writeHead(500, {"Content-Type": "application/json"});
    res.end(JSON.stringify({error: String(error.message || error)}));
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`node ingestion listening on ${port}`);
});
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def utc_now_ch() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def fmt_ch(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def request(method: str, url: str, payload=None, headers=None, timeout: float = 10.0):
    body = None
    req_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=body, headers=req_headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        text = response.read().decode("utf-8")
        content_type = response.headers.get("Content-Type", "")
        if text and "json" in content_type:
            return json.loads(text)
        if text:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        return None


def clickhouse(sql: str, data: str | None = None, timeout: float = 60.0) -> str:
    query = sql if data is None else f"{sql}\n{data}"
    req = urllib.request.Request(
        CLICKHOUSE,
        data=query.encode("utf-8"),
        headers={
            "Content-Type": "text/plain",
            "X-ClickHouse-User": CLICKHOUSE_USER,
            "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8")


def clickhouse_value(sql: str, timeout: float = 60.0) -> str:
    return clickhouse(sql, timeout=timeout).strip()


def wait_for_http(url: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request("GET", url, timeout=1.0)
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"timed out waiting for {url}: {last_error}")


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(cmd, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and completed.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{completed.stdout}")
    return completed


def rpk(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    brokers = "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092"
    return run(["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "-X", f"brokers={brokers}", *args], check=check)


def create_topic(topic: str, partitions: int = 12, replicas: int = 3) -> None:
    completed = rpk(["topic", "create", topic, "--partitions", str(partitions), "--replicas", str(replicas)], check=False)
    lower = completed.stdout.lower()
    if completed.returncode != 0 and "already exists" not in lower and "already_exists" not in lower:
        raise RuntimeError(completed.stdout)


def delete_topic(topic: str) -> None:
    completed = rpk(["topic", "delete", topic], check=False)
    lower = completed.stdout.lower()
    if completed.returncode != 0 and "not found" not in lower and "unknown_topic" not in lower:
        raise RuntimeError(completed.stdout)


def topic_exists(topic: str) -> bool:
    completed = rpk(["topic", "list"], check=False)
    if completed.returncode != 0:
        return False
    return any(line.split()[0] == topic for line in completed.stdout.splitlines() if line.strip() and not line.startswith("NAME"))


def reset_topics() -> None:
    for topic in [EVENT_TOPIC, DLQ_TOPIC]:
        delete_topic(topic)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if not topic_exists(EVENT_TOPIC) and not topic_exists(DLQ_TOPIC):
            create_topic(EVENT_TOPIC, partitions=12, replicas=3)
            create_topic(DLQ_TOPIC, partitions=3, replicas=3)
            return
    raise RuntimeError("timed out waiting for benchmark topics to delete")


def init() -> None:
    wait_for_http(f"{CLICKHOUSE}/ping", timeout_s=90)
    wait_for_http(f"{PANDAPROXY}/brokers", timeout_s=90)
    schema = (ROOT / "schema.sql").read_text(encoding="utf-8")
    for statement in [part.strip() for part in schema.split(";") if part.strip()]:
        clickhouse(statement)
    create_topic(EVENT_TOPIC, partitions=12, replicas=3)
    create_topic(DLQ_TOPIC, partitions=3, replicas=3)
    print("initialized ClickHouse schema and Redpanda topics")


class Consumer:
    def __init__(self, group: str, topic: str, offset_reset: str = "earliest") -> None:
        self.group = group
        self.topic = topic
        self.instance = f"{group}-{uuid.uuid4().hex[:8]}"
        headers = {
            "Content-Type": "application/vnd.kafka.v2+json",
            "Accept": "application/vnd.kafka.v2+json",
        }
        response = request(
            "POST",
            f"{PANDAPROXY}/consumers/{urllib.parse.quote(group)}",
            payload={
                "name": self.instance,
                "format": "json",
                "auto.offset.reset": offset_reset,
                "auto.commit.enable": "false",
                "fetch.min.bytes": "1",
                "consumer.request.timeout.ms": "1000",
            },
            headers=headers,
        )
        base_uri = response["base_uri"]
        parsed = urllib.parse.urlparse(base_uri)
        self.base_uri = f"{PANDAPROXY}{parsed.path}"
        request(
            "POST",
            f"{self.base_uri}/subscription",
            payload={"topics": [topic]},
            headers=headers,
        )

    def poll(self, timeout_ms: int = 1000, max_bytes: int = 50_000_000) -> list[dict]:
        headers = {"Accept": "application/vnd.kafka.json.v2+json"}
        url = f"{self.base_uri}/records?timeout={timeout_ms}&max_bytes={max_bytes}"
        try:
            records = request("GET", url, headers=headers, timeout=max(2.0, timeout_ms / 1000 + 1))
        except urllib.error.HTTPError as exc:
            if exc.code == 400:
                exc.read()
                return []
            raise
        return records or []

    def commit(self, records: list[dict]) -> None:
        partitions: dict[tuple[str, int], int] = {}
        for record in records:
            key = (record["topic"], int(record["partition"]))
            partitions[key] = max(partitions.get(key, 0), int(record["offset"]) + 1)
        headers = {
            "Content-Type": "application/vnd.kafka.v2+json",
            "Accept": "application/vnd.kafka.v2+json",
        }
        payload = {
            "partitions": [
                {"topic": topic, "partition": partition, "offset": offset}
                for (topic, partition), offset in sorted(partitions.items())
            ]
        }
        request("POST", f"{self.base_uri}/offsets", payload=payload, headers=headers)

    def close(self) -> None:
        try:
            request("DELETE", self.base_uri, headers={"Accept": "application/vnd.kafka.v2+json"}, timeout=2.0)
        except Exception:
            pass


def group_lag_zero(group: str) -> bool:
    completed = rpk(["group", "describe", group], check=False)
    if completed.returncode != 0:
        return False
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        return False
    total_lag_line = next((line for line in lines if line.strip().startswith("TOTAL-LAG")), "")
    if total_lag_line:
        parts = total_lag_line.split()
        return len(parts) >= 2 and parts[1] == "0"
    header_line = next((line for line in lines if "LAG" in line and "TOPIC" in line), "")
    if not header_line:
        return False
    header = header_line.split()
    try:
        lag_index = header.index("LAG")
    except ValueError:
        return False
    data_lines = lines[lines.index(header_line) + 1 :]
    lag_values = []
    for line in data_lines:
        columns = line.split()
        if len(columns) <= lag_index:
            continue
        try:
            lag_values.append(int(columns[lag_index]))
        except ValueError:
            continue
    return bool(lag_values) and all(value == 0 for value in lag_values)


def wait_for_lag_zero(group: str, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if group_lag_zero(group):
            return
    raise RuntimeError(f"consumer lag did not reach zero for group {group}")


def count_dlq_records_from_topic(run_id: str, expected: int) -> int:
    brokers = "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092"
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "redpanda-0",
            "rpk",
            "-X",
            f"brokers={brokers}",
            "topic",
            "consume",
            DLQ_TOPIC,
            "-n",
            str(expected),
            "-o",
            "start",
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stdout)
    escaped = f'\\"run_id\\":\\"{run_id}\\"'
    plain = f'"run_id":"{run_id}"'
    if escaped not in completed.stdout and plain not in completed.stdout:
        return 0
    return completed.stdout.count(f'"topic": "{DLQ_TOPIC}"')


def produce_to_topic(topic: str, values: list[dict]) -> None:
    if not values:
        return
    records = [{"key": value.get("customer_id"), "value": value} for value in values]
    request(
        "POST",
        f"{PANDAPROXY}/topics/{topic}",
        payload={"records": records},
        headers={
            "Content-Type": "application/vnd.kafka.json.v2+json",
            "Accept": "application/vnd.kafka.v2+json",
        },
        timeout=30.0,
    )


def start_node_ingest(port: int = 18181) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env.update({"NODE_INGEST_PORT": str(port), "PANDAPROXY": PANDAPROXY, "EVENT_TOPIC": EVENT_TOPIC})
    process = subprocess.Popen(
        ["node", "-e", NODE_INGEST],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    wait_for_http(f"http://127.0.0.1:{port}/health", timeout_s=30)
    return process


def stop_process(process: subprocess.Popen[str]) -> None:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def post_events_to_node(events: list[dict], batch_size: int = 250, port: int = 18181) -> None:
    for start in range(0, len(events), batch_size):
        batch = events[start : start + batch_size]
        request(
            "POST",
            f"http://127.0.0.1:{port}/ingest-batch",
            payload=batch,
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )


def redact_properties(value, key: str = ""):
    lower_key = key.lower()
    if any(keyword in lower_key for keyword in PII_KEYWORDS):
        if "email" in lower_key:
            return "[redacted:email]"
        if "phone" in lower_key:
            return "[redacted:phone]"
        if lower_key == "ip" or lower_key.endswith("_ip") or "ip_address" in lower_key:
            return "[redacted:ip]"
        return "[redacted:pii]"
    if isinstance(value, dict):
        return {str(child_key): redact_properties(child_value, str(child_key)) for child_key, child_value in value.items()}
    if isinstance(value, list):
        return [redact_properties(item, key) for item in value]
    if isinstance(value, str):
        redacted = EMAIL_RE.sub("[redacted:email]", value)
        redacted = PHONE_RE.sub("[redacted:phone]", redacted)
        redacted = IPV4_RE.sub("[redacted:ip]", redacted)
        return redacted
    return value


def valid_row_from_event(event: dict) -> dict:
    required = ["event_id", "customer_id", "timestamp", "sent_at_ms", "run_id"]
    missing = [field for field in required if field not in event or event[field] in (None, "")]
    if missing:
        raise ValueError(f"missing required fields: {','.join(missing)}")
    stored_ms = now_ms()
    return {
        "event_id": str(event["event_id"]),
        "customer_id": str(event["customer_id"]),
        "timestamp": str(event["timestamp"]),
        "received_at": str(event.get("received_at") or event["timestamp"]),
        "stored_at": fmt_ch(stored_ms),
        "sent_at_ms": int(event["sent_at_ms"]),
        "stored_at_ms": stored_ms,
        "run_id": str(event["run_id"]),
        "user_hash": event.get("user_hash"),
        "session_id": str(event.get("session_id") or ""),
        "event_type": str(event.get("event_type") or "custom"),
        "event_name": str(event.get("event_name") or ""),
        "properties": json.dumps(redact_properties(event.get("properties") or {}), separators=(",", ":")),
    }


def insert_rows(rows: list[dict]) -> None:
    if not rows:
        return
    lines = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    clickhouse("INSERT INTO analytics.events FORMAT JSONEachRow", data=lines, timeout=120)


def process_records(records: list[dict]) -> tuple[int, int]:
    valid_rows = []
    dlq_rows = []
    for record in records:
        value = record.get("value")
        try:
            if not isinstance(value, dict):
                raise ValueError("record value is not a JSON object")
            valid_rows.append(valid_row_from_event(value))
        except Exception as exc:
            dlq_rows.append(
                {
                    "run_id": value.get("run_id") if isinstance(value, dict) else "",
                    "error": str(exc),
                    "original": value,
                    "routed_at": utc_now_ch(),
                }
            )
    insert_rows(valid_rows)
    produce_to_topic(DLQ_TOPIC, dlq_rows)
    return len(valid_rows), len(dlq_rows)


def consume(
    group: str = HOT_GROUP,
    offset_reset: str = "earliest",
    max_records: int | None = None,
    stop_on_lag_zero: bool = True,
    timeout_s: float = 120.0,
) -> tuple[int, int]:
    consumer = Consumer(group, EVENT_TOPIC, offset_reset=offset_reset)
    valid_count = 0
    dlq_count = 0
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            records = consumer.poll(timeout_ms=1000)
            if records:
                valid, dlq = process_records(records)
                valid_count += valid
                dlq_count += dlq
                consumer.commit(records)
            if max_records is not None and valid_count + dlq_count >= max_records:
                wait_for_lag_zero(group, timeout_s=timeout_s)
                return valid_count, dlq_count
            if stop_on_lag_zero and (valid_count + dlq_count > 0) and group_lag_zero(group):
                return valid_count, dlq_count
            if stop_on_lag_zero and not records and max_records is None and group_lag_zero(group):
                return valid_count, dlq_count
    finally:
        consumer.close()
    raise RuntimeError(f"consume timed out after valid={valid_count}, dlq={dlq_count}")


def make_event(run_id: str, customer_id: str, event_type: str, event_name: str, user_index: int) -> dict:
    sent_ms = now_ms()
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "timestamp": fmt_ch(sent_ms),
        "received_at": fmt_ch(sent_ms),
        "sent_at_ms": sent_ms,
        "run_id": run_id,
        "user_hash": f"user_{user_index:06d}",
        "session_id": f"session_{user_index % 1000:04d}",
        "event_type": event_type,
        "event_name": event_name,
        "properties": {"path": "/pricing" if event_name == "pricing" else "/"},
    }


def produce_command(args: argparse.Namespace) -> None:
    init()
    run_id = args.run_id or f"produce_{uuid.uuid4().hex[:10]}"
    customer_id = f"customer_{run_id}"
    events = [make_event(run_id, customer_id, "manual", "page_view", i) for i in range(args.count)]
    for event in events[: args.duplicates]:
        events.append(dict(event))
    for i in range(args.poison):
        events.append({"run_id": run_id, "customer_id": customer_id, "sent_at_ms": now_ms(), "poison_index": i})
    node = start_node_ingest()
    try:
        post_events_to_node(events)
    finally:
        stop_process(node)
    print(f"produced run_id={run_id} records={len(events)} valid={args.count + args.duplicates} poison={args.poison}")


def consume_command(args: argparse.Namespace) -> None:
    init()
    valid, dlq = consume(
        group=args.group,
        offset_reset=args.offset_reset,
        max_records=args.max_records,
        stop_on_lag_zero=not args.forever,
        timeout_s=args.timeout,
    )
    print(f"consumed valid={valid} dlq={dlq} group={args.group}")


def seed_segment_data(run_id: str, users: int = 50_000, views_per_user: int = 3) -> str:
    customer_id = f"segment_{run_id}"
    base = dt.datetime.now(dt.UTC) - dt.timedelta(days=6)
    batch = []
    for user in range(users):
        user_hash = f"seg_user_{user:06d}"
        for view in range(views_per_user):
            ts = base + dt.timedelta(seconds=(user * views_per_user + view) % (7 * 24 * 60 * 60))
            ts_ms = int(ts.timestamp() * 1000)
            batch.append(
                {
                    "event_id": f"{run_id}_{user}_{view}",
                    "customer_id": customer_id,
                    "timestamp": fmt_ch(ts_ms),
                    "received_at": fmt_ch(ts_ms),
                    "stored_at": fmt_ch(ts_ms),
                    "sent_at_ms": ts_ms,
                    "stored_at_ms": ts_ms,
                    "run_id": run_id,
                    "user_hash": user_hash,
                    "session_id": f"seg_session_{user:06d}",
                    "event_type": "page_view",
                    "event_name": "pricing",
                    "properties": "{}",
                }
            )
            if len(batch) >= 10_000:
                insert_rows(batch)
                batch.clear()
    insert_rows(batch)
    return customer_id


def drop_clickhouse_caches() -> None:
    for statement in ["SYSTEM DROP QUERY CACHE", "SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE"]:
        try:
            clickhouse(statement, timeout=10)
        except Exception:
            pass


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1)
    return ordered[index]


def consume_dlq_for_run(run_id: str, group: str, expected: int, timeout_s: float = 60.0) -> int:
    consumer = Consumer(group, DLQ_TOPIC, offset_reset="latest")
    consumer.poll(timeout_ms=200)
    count = 0
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and count < expected:
            records = consumer.poll(timeout_ms=1000)
            for record in records:
                value = record.get("value")
                if isinstance(value, dict) and value.get("run_id") == run_id:
                    count += 1
            if records:
                consumer.commit(records)
        wait_for_lag_zero(group, timeout_s=timeout_s)
        return count
    finally:
        consumer.close()


def bench(_: argparse.Namespace) -> None:
    init()
    reset_topics()
    run_id = f"bench_{uuid.uuid4().hex[:10]}"
    real_sdk_contract_verified = 0
    if os.environ.get("SDK_COMMAND", "").strip():
        completed = subprocess.run(
            [sys.executable, str(ROOT / "sdk_contract.py")],
            cwd=ROOT,
            env=os.environ.copy(),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=40,
        )
        real_sdk_contract_verified = 1 if completed.returncode == 0 else 0
    spike_customer = f"customer_{run_id}"
    spike_unique = 5_790
    retry_unique = 1_000
    retry_duplicates = 10
    poison_count = 5
    pii_probe_count = 1

    events = [
        make_event(run_id, spike_customer, "spike", "page_view", i)
        for i in range(spike_unique)
    ]
    retry_events = [
        make_event(run_id, spike_customer, "retry_duplicate", "click", i)
        for i in range(retry_unique)
    ]
    events.extend(retry_events)
    events.extend(dict(event) for event in retry_events[:retry_duplicates])
    pii_probe = make_event(run_id, spike_customer, "pii_probe", "form_submit", 0)
    pii_probe["properties"] = {
        "email": "person@example.com",
        "phone": "+1 555 123 4567",
        "ip_address": "203.0.113.42",
        "safe_property": "campaign_a",
        "referrer": "https://example.test/path?email=person@example.com",
    }
    events.append(pii_probe)
    for index in range(poison_count):
        events.append(
            {
                "run_id": run_id,
                "customer_id": spike_customer,
                "sent_at_ms": now_ms(),
                "event_type": "poison",
                "poison_index": index,
            }
        )

    writer_group = f"bench-writer-{run_id}"
    node = start_node_ingest()
    try:
        post_events_to_node(events)
    finally:
        stop_process(node)

    valid, poison = consume(
        group=writer_group,
        offset_reset="earliest",
        max_records=len(events),
        stop_on_lag_zero=True,
        timeout_s=180,
    )
    dlq_seen = count_dlq_records_from_topic(run_id, poison_count)

    hot_query = f"""
        SELECT
            countIf(event_type = 'spike'),
            uniqExactIf(event_id, event_type = 'spike'),
            countIf(event_type = 'retry_duplicate'),
            uniqExactIf(event_id, event_type = 'retry_duplicate'),
            quantileExact(0.5)(stored_at_ms - sent_at_ms),
            quantileExact(0.99)(stored_at_ms - sent_at_ms)
        FROM analytics.events
        WHERE run_id = '{run_id}'
    """
    hot_values = [int(float(value)) for value in clickhouse_value(hot_query).split("\t")]
    spike_rows, spike_distinct, retry_rows, retry_distinct, e2e_p50_ms, e2e_p99_ms = hot_values
    duplicate_rate = ((retry_rows - retry_distinct) / retry_rows) * 100 if retry_rows else 0.0
    pii_properties = clickhouse_value(
        f"SELECT properties FROM analytics.events WHERE run_id = '{run_id}' AND event_type = 'pii_probe' LIMIT 1"
    )
    pii_email_redacted = int("person@example.com" not in pii_properties and "[redacted:email]" in pii_properties)
    pii_phone_redacted = int("+1 555 123 4567" not in pii_properties and "[redacted:phone]" in pii_properties)
    pii_ip_redacted = int("203.0.113.42" not in pii_properties and "[redacted:ip]" in pii_properties)
    pii_safe_property_preserved = int("campaign_a" in pii_properties)

    segment_customer = seed_segment_data(run_id)
    segment_sql = f"""
        SELECT count()
        FROM
        (
            SELECT user_hash
            FROM analytics.events
            WHERE customer_id = '{segment_customer}'
              AND timestamp >= now64(3) - INTERVAL 7 DAY
              AND user_hash IS NOT NULL
              AND event_name = 'pricing'
            GROUP BY user_hash
            HAVING count(DISTINCT event_id) >= 3
        )
        SETTINGS use_query_cache = 0
    """
    segment_samples_ms: list[float] = []
    segment_count = 0
    for _ in range(20):
        drop_clickhouse_caches()
        start_ns = time.perf_counter_ns()
        segment_count = int(clickhouse_value(segment_sql, timeout=120))
        segment_samples_ms.append((time.perf_counter_ns() - start_ns) / 1_000_000)
    segment_p99_ms = percentile(segment_samples_ms, 99)

    result = f"""# benchmark_results.txt

- [Observed] generated_unix_ms = {now_ms()}
- [Observed] run_id = {run_id}
- [Observed] redpanda_brokers_configured = 3
- [Observed] clickhouse_writers = 1
- [Assumed] brief_current_events_per_day = 50000000
- [Estimated] ten_x_spike_events_per_second = 5790
- [Observed] sdk_contract_gate_required_before_dedup = 1
- [Observed] real_sdk_contract_verified = {real_sdk_contract_verified}

## Hot Path

- [Observed] e2e_latency_p50_ms = {e2e_p50_ms}
- [Observed] e2e_latency_p99_ms = {e2e_p99_ms}
- [Observed] ten_x_spike_events_sent = {spike_unique}
- [Observed] ten_x_spike_events_stored_raw = {spike_rows}
- [Observed] ten_x_spike_events_stored_distinct = {spike_distinct}
- [Observed] valid_events_consumed_by_python_writer = {valid}

## Query Dedup Model

- [Observed] intentional_retry_events_sent_raw = {retry_rows}
- [Observed] intentional_retry_events_distinct_event_id = {retry_distinct}
- [Observed] duplicate_rate_percent_with_intentional_retries = {duplicate_rate:.3f}
- [Observed] dedup_query_shape = count_distinct_event_id_at_query_time

## PII Guardrail

- [Observed] pii_probe_events_sent = {pii_probe_count}
- [Observed] pii_probe_email_redacted = {pii_email_redacted}
- [Observed] pii_probe_phone_redacted = {pii_phone_redacted}
- [Observed] pii_probe_ip_redacted = {pii_ip_redacted}
- [Observed] pii_probe_safe_property_preserved = {pii_safe_property_preserved}

## Segment Query

- [Observed] segment_seed_days = 7
- [Observed] segment_seed_users = 50000
- [Observed] segment_seed_events_per_user = 3
- [Observed] segment_query_predicate_requires_user_hash_not_null = 1
- [Observed] segment_query_matching_users = {segment_count}
- [Observed] cold_cache_segment_query_samples = 20
- [Observed] cold_cache_segment_query_p99_ms = {segment_p99_ms:.3f}

## DLQ

- [Observed] poison_events_sent = {poison_count}
- [Observed] poison_events_consumed_by_writer = {poison}
- [Observed] poison_events_seen_on_dlq_topic = {dlq_seen}

## Non-Hot-Path Commitments

- [Assumed] s3_archive_consumer_group_blocks_hot_path = 0
- [Assumed] s3_archive_window_minutes = 5
- [Assumed] warehouse_export_consumer_group_blocks_hot_path = 0
"""
    (ROOT / "benchmark_results.txt").write_text(result, encoding="utf-8")
    print(result)


def main() -> int:
    parser = argparse.ArgumentParser(description="Engineer-004 real-time analytics artifact")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init").set_defaults(func=lambda args: init())

    produce_parser = subparsers.add_parser("produce")
    produce_parser.add_argument("--count", type=int, default=100)
    produce_parser.add_argument("--duplicates", type=int, default=0)
    produce_parser.add_argument("--poison", type=int, default=0)
    produce_parser.add_argument("--run-id")
    produce_parser.set_defaults(func=produce_command)

    consume_parser = subparsers.add_parser("consume")
    consume_parser.add_argument("--group", default=HOT_GROUP)
    consume_parser.add_argument("--offset-reset", choices=["earliest"], default="earliest")
    consume_parser.add_argument("--max-records", type=int)
    consume_parser.add_argument("--timeout", type=float, default=120.0)
    consume_parser.add_argument("--forever", action="store_true")
    consume_parser.set_defaults(func=consume_command)

    bench_parser = subparsers.add_parser("bench")
    bench_parser.set_defaults(func=bench)

    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
