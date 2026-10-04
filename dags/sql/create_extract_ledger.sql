CREATE TABLE IF NOT EXISTS extract_ledger(
    sensor_id BIGINT NOT NULL, 
    period_month DATE NOT NULL, 
    status TEXT NOT NULL 
        CHECK(status IN ('success', 'failed_transient', 'failed_permanent', 'poison')),
    row_count INTEGER, 
    object_key TEXT, 
    http_status INTEGER, 
    error_message TEXT,
    attempts INTEGER NOT NULL DEFAULT 1,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(sensor_id, period_month)
); 

