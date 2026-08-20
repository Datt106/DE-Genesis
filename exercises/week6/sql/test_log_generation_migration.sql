\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA week6_control;
CREATE SCHEMA week6_log;

-- Snapshot tối thiểu của schema epoch-only trước migration generation.
CREATE TABLE week6_control.log_stream_batches (
    stream_batch_id BIGINT PRIMARY KEY,
    query_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    max_event_time TIMESTAMPTZ,
    raw_count BIGINT NOT NULL DEFAULT 0,
    valid_count BIGINT NOT NULL DEFAULT 0,
    invalid_count BIGINT NOT NULL DEFAULT 0,
    ingestion_lag_seconds NUMERIC(14, 3),
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    error_message TEXT
);

CREATE TABLE week6_log.requests_per_minute_stream_staging (
    stream_batch_id BIGINT NOT NULL,
    minute_start TIMESTAMPTZ NOT NULL,
    service TEXT NOT NULL,
    request_count BIGINT NOT NULL,
    latency_sum_ms NUMERIC(20, 3) NOT NULL,
    max_latency_ms INTEGER NOT NULL,
    PRIMARY KEY (stream_batch_id, minute_start, service)
);

CREATE TABLE week6_log.status_distribution_stream_staging (
    stream_batch_id BIGINT NOT NULL,
    minute_start TIMESTAMPTZ NOT NULL,
    service TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    request_count BIGINT NOT NULL,
    PRIMARY KEY (stream_batch_id, minute_start, service, status_code)
);

CREATE TABLE week6_log.requests_per_minute_stream (
    stream_batch_id BIGINT NOT NULL,
    minute_start TIMESTAMPTZ NOT NULL,
    service TEXT NOT NULL,
    request_count BIGINT NOT NULL,
    latency_sum_ms NUMERIC(20, 3) NOT NULL,
    max_latency_ms INTEGER NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_batch_id, minute_start, service)
);

CREATE TABLE week6_log.status_distribution_stream (
    stream_batch_id BIGINT NOT NULL,
    minute_start TIMESTAMPTZ NOT NULL,
    service TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    request_count BIGINT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_batch_id, minute_start, service, status_code)
);

INSERT INTO week6_control.log_stream_batches(
    stream_batch_id, query_name, finished_at, status
) VALUES
    (6, 'de_genesis_week6_service_logs', NOW() - INTERVAL '30 seconds', 'success'),
    (7, 'de_genesis_week6_service_logs', NOW(), 'success');

\ir create_week6_schemas.sql
\ir create_week6_schemas.sql

DO $$
DECLARE
    legacy_high_water BIGINT;
    legacy_checkpoint TEXT;
    legacy_lineage TEXT;
    legacy_active BOOLEAN;
    batch_pk TEXT;
BEGIN
    SELECT last_successful_batch_id, checkpoint_path, lineage_id, is_active
    INTO legacy_high_water, legacy_checkpoint, legacy_lineage, legacy_active
    FROM week6_control.log_stream_generations
    WHERE stream_generation_id='legacy-v1';

    IF legacy_high_water <> 7
       OR legacy_checkpoint <> 'migration-lock://legacy-v1'
       OR legacy_lineage <> 'legacy-v1'
       OR NOT legacy_active THEN
        RAISE EXCEPTION 'Legacy generation chưa được seed/khóa đúng contract';
    END IF;

    SELECT pg_get_constraintdef(oid)
    INTO batch_pk
    FROM pg_constraint
    WHERE conrelid='week6_control.log_stream_batches'::regclass
      AND contype='p';
    IF batch_pk <> 'PRIMARY KEY (stream_generation_id, stream_batch_id)' THEN
        RAISE EXCEPTION 'Primary key telemetry chưa có generation: %', batch_pk;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname='week6_control'
          AND indexname='uq_week6_log_stream_active_generation'
          AND indexdef LIKE '%WHERE is_active%'
    ) THEN
        RAISE EXCEPTION 'Thiếu unique active-generation contract';
    END IF;
END $$;

ROLLBACK;
