CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events
(
    event_id String,
    customer_id String,
    timestamp DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    stored_at DateTime64(3, 'UTC'),
    sent_at_ms UInt64,
    stored_at_ms UInt64,
    run_id String,
    user_hash Nullable(String),
    session_id String,
    event_type LowCardinality(String),
    event_name LowCardinality(String),
    properties String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
PRIMARY KEY (customer_id, timestamp, event_id)
ORDER BY (customer_id, timestamp, event_id);
