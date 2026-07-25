-- =============================================================================
-- Function: fn_normalize_phone
-- Layer: Silver
-- Inputs: raw_phone STRING
-- Returns: STRING — digits only with optional + prefix
-- Called by: Spark 02_bronze_to_silver (referenced), sp_enrich_account_balances
-- References: none (pure calculation)
-- =============================================================================

-- Registration (Hive):
-- CREATE FUNCTION db.fn_normalize_phone AS 'com.bank.udf.NormalizePhone' USING JAR 'hdfs:///udf/normalize_phone.jar';

-- Inline SQL equivalent:
-- CASE
--     WHEN raw_phone IS NULL THEN NULL
--     ELSE REGEXP_REPLACE(raw_phone, '[^0-9+]', '')
-- END

-- Full implementation:
SELECT
    CASE
        WHEN raw_phone IS NULL
            THEN NULL
        ELSE REGEXP_REPLACE(raw_phone, '[^0-9+]', '')
    END AS normalized_phone;
