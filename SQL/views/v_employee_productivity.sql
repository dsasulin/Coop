-- View:    reporting.v_employee_productivity
-- Layer:   Operational
-- Sources: gold.branch_performance_dashboard
-- Purpose: Employee productivity metrics per branch

CREATE VIEW reporting.v_employee_productivity AS
SELECT
    branch_id,
    branch_code,
    branch_name,
    region,
    total_employees,
    active_employees,
    avg_employee_tenure_years,
    total_clients,
    total_accounts,
    total_revenue_mtd,
    total_fees_collected,
    CASE WHEN total_employees > 0 THEN total_clients / total_employees END AS clients_per_employee,
    CASE WHEN total_employees > 0 THEN total_revenue_mtd / total_employees END AS revenue_per_employee,
    CASE WHEN total_employees > 0 THEN total_accounts / total_employees END AS accounts_per_employee,
    report_date
FROM gold.branch_performance_dashboard
ORDER BY revenue_per_employee DESC;
