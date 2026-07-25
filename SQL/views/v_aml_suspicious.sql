-- View:    reporting.v_aml_suspicious
-- Layer:   Regulatory
-- Sources: gold.fact_transactions_daily, gold.dim_client
-- Purpose: Anti-money laundering suspicious activity report

CREATE VIEW reporting.v_aml_suspicious AS
SELECT
    ftd.transaction_date,
    dc.client_id,
    dc.full_name,
    dc.client_segment,
    dc.risk_category,
    dc.credit_score_category,
    ftd.account_id,
    ftd.branch_code,
    ftd.transaction_type,
    ftd.transaction_count,
    ftd.total_amount,
    ftd.suspicious_transaction_count
FROM gold.fact_transactions_daily ftd
JOIN gold.dim_client dc
    ON ftd.client_id = dc.client_id
WHERE (ftd.has_suspicious_transactions = true OR ftd.suspicious_transaction_count > 0)
  AND dc.is_current = true
ORDER BY ftd.suspicious_transaction_count DESC;
