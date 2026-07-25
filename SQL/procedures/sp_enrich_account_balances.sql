-- ============================================================================
-- Procedure: sp_enrich_account_balances
-- Layer: Bronze -> Silver
-- Reads: bronze.account_balances, silver.accounts
-- Writes: silver.account_balances
-- Calls: fn_calc_credit_utilization (inline CASE)
-- ============================================================================

INSERT OVERWRITE TABLE silver.account_balances
SELECT
    ab.balance_id,
    ab.account_id,
    ab.current_balance,
    ab.available_balance,
    -- reserved_amount: difference between current and available, floor at 0
    CASE
        WHEN (ab.current_balance - ab.available_balance) < 0 THEN 0
        ELSE ab.current_balance - ab.available_balance
    END AS reserved_amount,
    ab.currency,
    ab.last_updated,
    ab.credit_limit,
    -- credit_utilization: inline fn_calc_credit_utilization
    CASE
        WHEN ab.credit_limit IS NULL OR ab.credit_limit = 0 THEN 0.0000
        WHEN ab.current_balance < 0                         THEN 0.0000
        ELSE CAST(ab.current_balance AS DECIMAL(18,4)) / ab.credit_limit
    END AS credit_utilization,
    -- balance_category
    CASE
        WHEN ab.current_balance < 0      THEN 'NEGATIVE'
        WHEN ab.current_balance < 1000   THEN 'LOW'
        WHEN ab.current_balance < 10000  THEN 'MEDIUM'
        WHEN ab.current_balance < 100000 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS balance_category,
    ab.load_timestamp,
    CURRENT_TIMESTAMP()  AS process_timestamp,
    'CORE_BANKING'       AS source_system
FROM bronze.account_balances ab
LEFT JOIN silver.accounts a
    ON ab.account_id = a.account_id;
