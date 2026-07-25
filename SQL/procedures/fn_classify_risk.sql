-- =============================================================================
-- Function: fn_classify_risk
-- Layer: Silver -> Gold
-- Inputs: credit_score INT, annual_income DECIMAL(18,2), delinquency_status STRING
-- Returns: STRING — risk category
-- Called by: sp_build_dim_client
-- References: none (pure calculation)
-- =============================================================================

-- Registration (Hive):
-- CREATE FUNCTION db.fn_classify_risk AS 'com.bank.udf.ClassifyRisk' USING JAR 'hdfs:///udf/classify_risk.jar';

-- Inline SQL equivalent:
-- CASE
--     WHEN delinquency_status IN ('DELINQUENT_90', 'DELINQUENT_120_PLUS') THEN 'CRITICAL'
--     WHEN delinquency_status = 'DELINQUENT_60' THEN 'HIGH'
--     WHEN credit_score < 580 OR annual_income < 20000 THEN 'HIGH'
--     WHEN delinquency_status = 'DELINQUENT_30' THEN 'MEDIUM'
--     WHEN credit_score < 670 OR annual_income < 40000 THEN 'MEDIUM'
--     WHEN credit_score >= 740 AND annual_income >= 100000 THEN 'LOW'
--     ELSE 'MODERATE'
-- END

-- Full implementation:
SELECT
    CASE
        WHEN delinquency_status IN ('DELINQUENT_90', 'DELINQUENT_120_PLUS')
            THEN 'CRITICAL'
        WHEN delinquency_status = 'DELINQUENT_60'
            THEN 'HIGH'
        WHEN credit_score < 580 OR annual_income < 20000
            THEN 'HIGH'
        WHEN delinquency_status = 'DELINQUENT_30'
            THEN 'MEDIUM'
        WHEN credit_score < 670 OR annual_income < 40000
            THEN 'MEDIUM'
        WHEN credit_score >= 740 AND annual_income >= 100000
            THEN 'LOW'
        ELSE 'MODERATE'
    END AS risk_category;
