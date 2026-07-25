-- View: reporting.v_daily_revenue
-- Layer: reporting
-- Sources: gold.fact_transactions_daily
-- Purpose: Daily revenue aggregation

CREATE VIEW reporting.v_daily_revenue AS
SELECT
    transaction_date,
    currency,
    SUM(transaction_count) AS total_transactions,
    SUM(total_amount) AS total_amount,
    AVG(avg_amount) AS avg_amount,
    SUM(successful_count) AS successful_count,
    SUM(failed_count) AS failed_count,
    AVG(success_rate) AS avg_success_rate
FROM gold.fact_transactions_daily
GROUP BY transaction_date, currency;
