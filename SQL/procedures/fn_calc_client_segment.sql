-- =============================================================================
-- Function: fn_calc_client_segment
-- Layer: Gold
-- Inputs: annual_income DECIMAL(18,2), credit_score INT
-- Returns: STRING — client segment
-- Called by: sp_build_dim_client
-- References: none (pure calculation)
-- =============================================================================

-- Registration (Hive):
-- CREATE FUNCTION db.fn_calc_client_segment AS 'com.bank.udf.CalcClientSegment' USING JAR 'hdfs:///udf/calc_client_segment.jar';

-- Inline SQL equivalent:
-- CASE
--     WHEN annual_income > 150000 AND credit_score > 750 THEN 'VIP'
--     WHEN annual_income > 100000 AND credit_score > 700 THEN 'PREMIUM'
--     WHEN annual_income > 50000 AND credit_score > 650 THEN 'REGULAR'
--     ELSE 'BASIC'
-- END

-- Full implementation:
SELECT
    CASE
        WHEN annual_income > 150000 AND credit_score > 750
            THEN 'VIP'
        WHEN annual_income > 100000 AND credit_score > 700
            THEN 'PREMIUM'
        WHEN annual_income > 50000 AND credit_score > 650
            THEN 'REGULAR'
        ELSE 'BASIC'
    END AS client_segment;
