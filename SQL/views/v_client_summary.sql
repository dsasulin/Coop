-- View: reporting.v_client_summary
-- Layer: reporting
-- Sources: gold.dim_client, gold.client_360_view
-- Purpose: Client overview for relationship managers

CREATE VIEW reporting.v_client_summary AS
SELECT
    dc.client_id,
    dc.full_name,
    dc.email,
    dc.phone,
    dc.age_group,
    dc.city,
    dc.country,
    dc.client_segment,
    dc.credit_score_category,
    c360.total_accounts,
    c360.total_products,
    c360.total_loans,
    c360.total_balance,
    c360.transaction_volume_30d,
    c360.transactions_30d,
    c360.customer_lifetime_value,
    c360.risk_score,
    c360.profitability_score,
    c360.days_since_last_transaction
FROM gold.dim_client dc
JOIN gold.client_360_view c360
    ON dc.client_id = c360.client_id
WHERE dc.is_current = true;
