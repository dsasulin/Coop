-- View: reporting.v_delinquency_report
-- Layer: reporting
-- Sources: gold.fact_loan_performance, gold.dim_client
-- Purpose: Delinquent loans for risk management

CREATE VIEW reporting.v_delinquency_report AS
SELECT
    flp.loan_id,
    dc.full_name,
    dc.client_segment,
    dc.credit_score,
    flp.loan_amount,
    flp.outstanding_balance,
    flp.delinquency_status,
    flp.days_past_due,
    flp.collateral_value,
    flp.loan_to_value_ratio,
    flp.ltv_category,
    flp.default_probability
FROM gold.fact_loan_performance flp
JOIN gold.dim_client dc
    ON flp.client_id = dc.client_id
WHERE flp.is_delinquent = true
ORDER BY flp.days_past_due DESC;
