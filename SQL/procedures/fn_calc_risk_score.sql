-- =============================================================================
-- Function: fn_calc_risk_score
-- Layer: Gold
-- Inputs: delinquent_loans INT, suspicious_activities INT, credit_score INT
-- Returns: DECIMAL(5,2) — risk score (0-100, higher = riskier)
-- Called by: sp_build_client_360
-- References: none (pure calculation)
-- =============================================================================

-- Registration (Hive):
-- CREATE FUNCTION db.fn_calc_risk_score AS 'com.bank.udf.CalcRiskScore' USING JAR 'hdfs:///udf/calc_risk_score.jar';

-- Inline SQL equivalent:
-- LEAST(
--     LEAST(delinquent_loans * 25, 50)
--     + LEAST(suspicious_activities * 10, 30)
--     + CASE WHEN credit_score >= 740 THEN 0 WHEN credit_score >= 670 THEN 5
--            WHEN credit_score >= 580 THEN 10 ELSE 20 END,
--     100
-- )

-- Full implementation:
SELECT
    CAST(
        LEAST(
            LEAST(delinquent_loans * 25, 50)
            + LEAST(suspicious_activities * 10, 30)
            + CASE
                WHEN credit_score >= 740 THEN 0
                WHEN credit_score >= 670 THEN 5
                WHEN credit_score >= 580 THEN 10
                ELSE 20
              END,
            100
        ) AS DECIMAL(5,2)
    ) AS risk_score;
