-- View: reporting.v_loan_portfolio
-- Layer: reporting
-- Sources: gold.fact_loan_performance, gold.dim_client
-- Purpose: Loan portfolio overview

CREATE VIEW reporting.v_loan_portfolio AS
SELECT
    flp.loan_id,
    flp.contract_id,
    dc.full_name,
    dc.client_segment,
    flp.loan_amount,
    flp.outstanding_balance,
    flp.paid_amount,
    flp.payment_progress,
    flp.interest_rate,
    flp.delinquency_status,
    flp.is_delinquent,
    flp.days_past_due,
    flp.ltv_category,
    flp.default_probability
FROM gold.fact_loan_performance flp
JOIN gold.dim_client dc
    ON flp.client_id = dc.client_id
WHERE dc.is_current = true;
