-- =============================================================================
-- Function: fn_calc_credit_utilization
-- Layer: Silver
-- Inputs: current_balance DECIMAL(18,2), credit_limit DECIMAL(18,2)
-- Returns: DECIMAL(5,4) — utilization ratio (0.0000 to 1.0+)
-- Called by: sp_enrich_account_balances
-- References: none (pure calculation)
-- =============================================================================

-- Registration (Hive):
-- CREATE FUNCTION db.fn_calc_credit_utilization AS 'com.bank.udf.CalcCreditUtilization' USING JAR 'hdfs:///udf/calc_credit_utilization.jar';

-- Inline SQL equivalent:
-- CASE
--     WHEN credit_limit IS NULL OR credit_limit = 0 THEN CAST(0.0000 AS DECIMAL(5,4))
--     WHEN current_balance < 0 THEN CAST(0.0000 AS DECIMAL(5,4))
--     ELSE CAST(current_balance / credit_limit AS DECIMAL(5,4))
-- END

-- Full implementation:
SELECT
    CASE
        WHEN credit_limit IS NULL OR credit_limit = 0
            THEN CAST(0.0000 AS DECIMAL(5,4))
        WHEN current_balance < 0
            THEN CAST(0.0000 AS DECIMAL(5,4))
        ELSE CAST(current_balance / credit_limit AS DECIMAL(5,4))
    END AS credit_utilization;
