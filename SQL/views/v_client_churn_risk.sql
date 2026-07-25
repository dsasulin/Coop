-- View: reporting.v_client_churn_risk
-- Layer: reporting
-- Sources: gold.dim_client, gold.client_360_view
-- Purpose: Identify clients at risk of churning

CREATE VIEW reporting.v_client_churn_risk AS
SELECT
    dc.client_id,
    dc.full_name,
    dc.client_segment,
    c360.risk_score,
    c360.days_since_last_transaction,
    c360.total_balance,
    c360.transactions_30d,
    c360.delinquent_loans
FROM gold.dim_client dc
JOIN gold.client_360_view c360
    ON dc.client_id = c360.client_id
WHERE c360.risk_score > 70
   OR c360.days_since_last_transaction > 90
ORDER BY c360.risk_score DESC;
