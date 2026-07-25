-- View: reporting.v_top_clients
-- Layer: reporting
-- Sources: gold.dim_client, gold.client_360_view
-- Purpose: Top 100 most valuable clients

CREATE VIEW reporting.v_top_clients AS
SELECT
    dc.client_id,
    dc.full_name,
    dc.client_segment,
    c360.customer_lifetime_value,
    c360.total_balance,
    c360.total_products,
    c360.profitability_score,
    c360.risk_score
FROM gold.dim_client dc
JOIN gold.client_360_view c360
    ON dc.client_id = c360.client_id
WHERE dc.is_current = true
ORDER BY c360.customer_lifetime_value DESC
LIMIT 100;
