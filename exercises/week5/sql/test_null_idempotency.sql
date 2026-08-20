-- Regression test chạy trong transaction và ROLLBACK toàn bộ dữ liệu kiểm thử.
-- Yêu cầu chạy create_week5_schemas.sql trước file này.
BEGIN;

DO $$
DECLARE
    test_prefix TEXT := 'week5-null-idempotency-' || txid_current()::text;
    airflow_same_batch BIGINT;
    airflow_all_batches BIGINT;
    nifi_same_batch BIGINT;
    airflow_index_valid BOOLEAN;
    nifi_index_valid BOOLEAN;
BEGIN
    INSERT INTO week5_raw.promotions_airflow(
        batch_id, source_system, promotion_id, product_id, payload,
        payload_hash, is_valid, validation_error
    ) VALUES (
        test_prefix || '-same', 'airflow', NULL, NULL,
        '{"invalid":true}'::jsonb, 'null-idempotency-hash', FALSE, 'missing id'
    )
    ON CONFLICT (batch_id, source_system, promotion_id, payload_hash) DO NOTHING;

    INSERT INTO week5_raw.promotions_airflow(
        batch_id, source_system, promotion_id, product_id, payload,
        payload_hash, is_valid, validation_error
    ) VALUES (
        test_prefix || '-same', 'airflow', NULL, NULL,
        '{"invalid":true}'::jsonb, 'null-idempotency-hash', FALSE, 'missing id'
    )
    ON CONFLICT (batch_id, source_system, promotion_id, payload_hash) DO NOTHING;

    INSERT INTO week5_raw.promotions_airflow(
        batch_id, source_system, promotion_id, product_id, payload,
        payload_hash, is_valid, validation_error
    ) VALUES (
        test_prefix || '-new', 'airflow', NULL, NULL,
        '{"invalid":true}'::jsonb, 'null-idempotency-hash', FALSE, 'missing id'
    )
    ON CONFLICT (batch_id, source_system, promotion_id, payload_hash) DO NOTHING;

    INSERT INTO week5_raw.promotions_nifi(
        batch_id, source_system, promotion_id, product_id, payload,
        payload_hash, is_valid, validation_error
    ) VALUES (
        test_prefix || '-same', 'nifi', NULL, NULL,
        '{"invalid":true}'::jsonb, 'null-idempotency-hash', FALSE, 'missing id'
    )
    ON CONFLICT (batch_id, source_system, promotion_id, payload_hash) DO NOTHING;

    INSERT INTO week5_raw.promotions_nifi(
        batch_id, source_system, promotion_id, product_id, payload,
        payload_hash, is_valid, validation_error
    ) VALUES (
        test_prefix || '-same', 'nifi', NULL, NULL,
        '{"invalid":true}'::jsonb, 'null-idempotency-hash', FALSE, 'missing id'
    )
    ON CONFLICT (batch_id, source_system, promotion_id, payload_hash) DO NOTHING;

    SELECT COUNT(*) INTO airflow_same_batch
    FROM week5_raw.promotions_airflow
    WHERE batch_id = test_prefix || '-same';
    SELECT COUNT(*) INTO airflow_all_batches
    FROM week5_raw.promotions_airflow
    WHERE batch_id IN (test_prefix || '-same', test_prefix || '-new');
    SELECT COUNT(*) INTO nifi_same_batch
    FROM week5_raw.promotions_nifi
    WHERE batch_id = test_prefix || '-same';

    SELECT indnullsnotdistinct INTO airflow_index_valid
    FROM pg_index
    WHERE indexrelid = 'week5_raw.uq_promotions_airflow_batch_payload'::regclass;
    SELECT indnullsnotdistinct INTO nifi_index_valid
    FROM pg_index
    WHERE indexrelid = 'week5_raw.uq_promotions_nifi_batch_payload'::regclass;

    IF airflow_same_batch <> 1 OR nifi_same_batch <> 1 THEN
        RAISE EXCEPTION 'Retry record NULL đã tạo duplicate: airflow=%, nifi=%',
            airflow_same_batch, nifi_same_batch;
    END IF;
    IF airflow_all_batches <> 2 THEN
        RAISE EXCEPTION 'Batch mới không được giữ độc lập: count=%', airflow_all_batches;
    END IF;
    IF NOT airflow_index_valid OR NOT nifi_index_valid THEN
        RAISE EXCEPTION 'Unique index chưa bật NULLS NOT DISTINCT';
    END IF;
END
$$;

ROLLBACK;
