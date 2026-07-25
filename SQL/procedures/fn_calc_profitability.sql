-- =============================================================================
-- Function: fn_calc_profitability
-- Layer: Gold
-- Inputs: total_revenue DECIMAL(18,2), total_fees_paid DECIMAL(18,2), customer_tenure_years DECIMAL(10,2)
-- Returns: DECIMAL(10,2) — profitability score (0-100)
-- Called by: sp_build_client_360
-- References: none (pure calculation)
-- =============================================================================

-- Registration (Hive):
-- CREATE FUNCTION db.fn_calc_profitability AS 'com.bank.udf.CalcProfitability' USING JAR 'hdfs:///udf/calc_profitability.jar';

-- Inline SQL equivalent:
-- CASE
--     WHEN total_revenue IS NULL THEN 0.00
--     ELSE LEAST(
--         LEAST((total_revenue + total_fees_paid) / 5000.0, 60.0)
--         + LEAST(customer_tenure_years * 4, 40.0),
--         100.0
--     )
-- END

-- Full implementation:
SELECT
    CASE
        WHEN total_revenue IS NULL
            THEN CAST(0.00 AS DECIMAL(10,2))
        ELSE CAST(
            LEAST(
                LEAST(
                    (total_revenue + total_fees_paid) / 5000.0,
                    60.0
                )
                + LEAST(customer_tenure_years * 4, 40.0),
                100.0
            ) AS DECIMAL(10,2))
    END AS profitability_score;
