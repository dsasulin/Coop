-- View: reporting.v_monthly_pnl
-- Layer: reporting
-- Sources: gold.financial_kpi_summary
-- Purpose: Monthly P&L report

CREATE VIEW reporting.v_monthly_pnl AS
SELECT
    report_date,
    metric_category,
    total_revenue_mtd,
    total_revenue_ytd,
    total_assets,
    total_liabilities,
    net_assets,
    total_deposits,
    total_loans,
    revenue_growth_rate,
    loan_growth_rate,
    deposit_growth_rate
FROM gold.financial_kpi_summary;
