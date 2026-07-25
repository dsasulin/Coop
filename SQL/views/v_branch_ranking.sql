-- View:    reporting.v_branch_ranking
-- Layer:   Operational
-- Sources: gold.branch_performance_dashboard
-- Purpose: Branch ranking by performance metrics

CREATE VIEW reporting.v_branch_ranking AS
SELECT
    branch_id,
    branch_code,
    branch_name,
    city,
    state,
    region,
    manager_name,
    total_employees,
    total_clients,
    active_clients,
    total_accounts,
    total_deposits,
    total_revenue_mtd,
    total_fees_collected,
    revenue_rank,
    customer_rank,
    efficiency_rank,
    report_date
FROM gold.branch_performance_dashboard
ORDER BY revenue_rank ASC;
