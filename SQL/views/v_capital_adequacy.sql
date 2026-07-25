-- View:    reporting.v_capital_adequacy
-- Layer:   Regulatory
-- Sources: gold.fact_loan_performance
-- Purpose: Capital adequacy and loan exposure report

CREATE VIEW reporting.v_capital_adequacy AS
SELECT
    delinquency_status,
    ltv_category,
    COUNT(*) AS loan_count,
    SUM(loan_amount) AS total_exposure,
    SUM(outstanding_balance) AS total_outstanding,
    SUM(collateral_value) AS total_collateral,
    AVG(loan_to_value_ratio) AS avg_ltv,
    AVG(default_probability) AS avg_default_prob,
    SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) AS delinquent_count
FROM gold.fact_loan_performance
GROUP BY delinquency_status, ltv_category;
