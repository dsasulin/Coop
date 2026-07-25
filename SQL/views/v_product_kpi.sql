-- View:    reporting.v_product_kpi
-- Layer:   Operational
-- Sources: gold.product_performance_summary
-- Purpose: Product KPI dashboard

CREATE VIEW reporting.v_product_kpi AS
SELECT
    product_id,
    product_name,
    product_type,
    product_category,
    total_clients,
    active_clients,
    retention_rate,
    total_contracts,
    total_contract_value,
    avg_contract_value,
    total_revenue_mtd,
    total_revenue_ytd,
    client_growth_rate,
    revenue_growth_rate,
    market_share,
    report_date
FROM gold.product_performance_summary
ORDER BY total_revenue_ytd DESC;
