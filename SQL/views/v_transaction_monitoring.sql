-- View:    reporting.v_transaction_monitoring
-- Layer:   Operational
-- Sources: gold.fact_transactions_daily
-- Purpose: Suspicious transaction monitoring for compliance

CREATE VIEW reporting.v_transaction_monitoring AS
SELECT
    transaction_date,
    client_id,
    account_id,
    branch_code,
    transaction_type,
    category,
    transaction_count,
    total_amount,
    suspicious_transaction_count
FROM gold.fact_transactions_daily
WHERE has_suspicious_transactions = true
   OR suspicious_transaction_count > 0
ORDER BY suspicious_transaction_count DESC, total_amount DESC;
