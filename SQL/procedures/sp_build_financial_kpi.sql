-- ============================================================================
-- Procedure: sp_build_financial_kpi
-- Layer: Gold
-- Reads: gold.fact_transactions_daily, gold.fact_loan_performance,
--        gold.fact_account_balance_daily, gold.dim_client
-- Writes: gold.financial_kpi_summary
-- Calls: none
-- ============================================================================

INSERT OVERWRITE TABLE gold.financial_kpi_summary

WITH client_metrics AS (
    SELECT
        COUNT(*)                                           AS total_clients,
        SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS active_clients,
        SUM(total_accounts)                                AS total_accounts,
        SUM(total_active_accounts)                         AS active_accounts
    FROM gold.dim_client
    WHERE is_current = TRUE
),

balance_metrics AS (
    -- Latest snapshot per account
    SELECT
        SUM(CASE WHEN b.current_balance >= 0 THEN b.current_balance ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN b.current_balance < 0  THEN ABS(b.current_balance) ELSE 0 END) AS total_liabilities,
        SUM(b.current_balance)                              AS net_assets,
        SUM(b.current_balance)                              AS total_assets
    FROM gold.fact_account_balance_daily b
    INNER JOIN (
        SELECT account_id, MAX(snapshot_date) AS max_date
        FROM gold.fact_account_balance_daily
        GROUP BY account_id
    ) latest
        ON b.account_id = latest.account_id
        AND b.snapshot_date = latest.max_date
),

loan_metrics AS (
    SELECT
        SUM(lp.outstanding_balance)                        AS total_loans,
        SUM(CASE WHEN lp.is_delinquent = TRUE THEN 1 ELSE 0 END) AS total_delinquent_loans,
        CASE
            WHEN COUNT(*) > 0
            THEN CAST(SUM(CASE WHEN lp.is_delinquent = TRUE THEN 1 ELSE 0 END) AS DECIMAL(10,4))
                 / COUNT(*)
            ELSE 0
        END                                                AS delinquency_rate,
        SUM(CASE WHEN lp.is_delinquent = TRUE THEN lp.outstanding_balance ELSE 0 END) AS total_npl_amount
    FROM gold.fact_loan_performance lp
    INNER JOIN (
        SELECT loan_id, MAX(snapshot_date) AS max_date
        FROM gold.fact_loan_performance
        GROUP BY loan_id
    ) latest
        ON lp.loan_id = latest.loan_id
        AND lp.snapshot_date = latest.max_date
),

txn_metrics_current AS (
    -- Current month transactions
    SELECT
        SUM(transaction_count)                             AS total_transactions,
        SUM(total_amount)                                  AS total_transaction_volume,
        CASE
            WHEN SUM(transaction_count) > 0
            THEN SUM(total_amount) / SUM(transaction_count)
            ELSE 0
        END                                                AS avg_transaction_value,
        CASE
            WHEN SUM(transaction_count) > 0
            THEN CAST(SUM(successful_count) AS DECIMAL(10,4)) / SUM(transaction_count)
            ELSE 0
        END                                                AS transaction_success_rate,
        SUM(total_amount)                                  AS total_revenue_mtd
    FROM gold.fact_transactions_daily
    WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE())
      AND MONTH(transaction_date) = MONTH(CURRENT_DATE())
),

txn_metrics_ytd AS (
    SELECT
        SUM(total_amount)                                  AS total_revenue_ytd
    FROM gold.fact_transactions_daily
    WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE())
),

-- Previous month for growth rates
prev_client_metrics AS (
    SELECT COUNT(*) AS prev_clients
    FROM gold.dim_client
    WHERE is_current = TRUE
      AND effective_date < DATE_SUB(CURRENT_DATE(), 30)
),

prev_txn_metrics AS (
    SELECT SUM(total_amount) AS prev_revenue
    FROM gold.fact_transactions_daily
    WHERE YEAR(transaction_date) = YEAR(DATE_SUB(CURRENT_DATE(), 30))
      AND MONTH(transaction_date) = MONTH(DATE_SUB(CURRENT_DATE(), 30))
),

prev_loan_metrics AS (
    SELECT SUM(lp.outstanding_balance) AS prev_loans
    FROM gold.fact_loan_performance lp
    INNER JOIN (
        SELECT loan_id, MAX(snapshot_date) AS max_date
        FROM gold.fact_loan_performance
        WHERE snapshot_date < DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY loan_id
    ) latest
        ON lp.loan_id = latest.loan_id
        AND lp.snapshot_date = latest.max_date
),

prev_balance_metrics AS (
    SELECT SUM(b.current_balance) AS prev_deposits
    FROM gold.fact_account_balance_daily b
    INNER JOIN (
        SELECT account_id, MAX(snapshot_date) AS max_date
        FROM gold.fact_account_balance_daily
        WHERE snapshot_date < DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY account_id
    ) latest
        ON b.account_id = latest.account_id
        AND b.snapshot_date = latest.max_date
)

SELECT
    -- 1. report_date
    CURRENT_DATE()                              AS report_date,
    -- 2. metric_category
    'MONTHLY'                                   AS metric_category,
    -- 3. total_clients
    cm.total_clients,
    -- 4. active_clients
    cm.active_clients,
    -- 5. total_accounts
    cm.total_accounts,
    -- 6. active_accounts
    cm.active_accounts,
    -- 7. total_assets
    COALESCE(bm.total_assets, 0)                AS total_assets,
    -- 8. total_liabilities
    COALESCE(bm.total_liabilities, 0)           AS total_liabilities,
    -- 9. net_assets
    COALESCE(bm.net_assets, 0)                  AS net_assets,
    -- 10. total_deposits
    COALESCE(bm.total_deposits, 0)              AS total_deposits,
    -- 11. total_loans
    COALESCE(lm.total_loans, 0)                 AS total_loans,
    -- 12. total_revenue_mtd
    COALESCE(tc.total_revenue_mtd, 0)           AS total_revenue_mtd,
    -- 13. total_revenue_ytd
    COALESCE(ty.total_revenue_ytd, 0)           AS total_revenue_ytd,
    -- 14. total_transactions
    COALESCE(tc.total_transactions, 0)          AS total_transactions,
    -- 15. total_transaction_volume
    COALESCE(tc.total_transaction_volume, 0)    AS total_transaction_volume,
    -- 16. avg_transaction_value
    COALESCE(tc.avg_transaction_value, 0)       AS avg_transaction_value,
    -- 17. transaction_success_rate
    COALESCE(tc.transaction_success_rate, 0)    AS transaction_success_rate,
    -- 18. total_delinquent_loans
    COALESCE(lm.total_delinquent_loans, 0)      AS total_delinquent_loans,
    -- 19. delinquency_rate
    COALESCE(lm.delinquency_rate, 0)            AS delinquency_rate,
    -- 20. total_npl_amount
    COALESCE(lm.total_npl_amount, 0)            AS total_npl_amount,
    -- 21. provision_coverage_ratio
    CASE
        WHEN COALESCE(lm.total_npl_amount, 0) > 0
        THEN CAST(COALESCE(lm.total_npl_amount, 0) * 0.5 / lm.total_npl_amount AS DECIMAL(10,4))
        ELSE CAST(1.0 AS DECIMAL(10,4))
    END                                         AS provision_coverage_ratio,
    -- 22. client_growth_rate
    CASE
        WHEN COALESCE(pcm.prev_clients, 0) > 0
        THEN CAST((cm.total_clients - pcm.prev_clients) AS DECIMAL(10,4))
             / pcm.prev_clients
        ELSE CAST(0.0 AS DECIMAL(10,4))
    END                                         AS client_growth_rate,
    -- 23. revenue_growth_rate
    CASE
        WHEN COALESCE(ptm.prev_revenue, 0) > 0
        THEN CAST((COALESCE(tc.total_revenue_mtd, 0) - ptm.prev_revenue) AS DECIMAL(10,4))
             / ptm.prev_revenue
        ELSE CAST(0.0 AS DECIMAL(10,4))
    END                                         AS revenue_growth_rate,
    -- 24. loan_growth_rate
    CASE
        WHEN COALESCE(plm.prev_loans, 0) > 0
        THEN CAST((COALESCE(lm.total_loans, 0) - plm.prev_loans) AS DECIMAL(10,4))
             / plm.prev_loans
        ELSE CAST(0.0 AS DECIMAL(10,4))
    END                                         AS loan_growth_rate,
    -- 25. deposit_growth_rate
    CASE
        WHEN COALESCE(pbm.prev_deposits, 0) > 0
        THEN CAST((COALESCE(bm.total_deposits, 0) - pbm.prev_deposits) AS DECIMAL(10,4))
             / pbm.prev_deposits
        ELSE CAST(0.0 AS DECIMAL(10,4))
    END                                         AS deposit_growth_rate,
    -- 26. created_timestamp
    CURRENT_TIMESTAMP()                         AS created_timestamp,
    -- 27. updated_timestamp
    CURRENT_TIMESTAMP()                         AS updated_timestamp

FROM client_metrics cm
CROSS JOIN balance_metrics bm
CROSS JOIN loan_metrics lm
CROSS JOIN txn_metrics_current tc
CROSS JOIN txn_metrics_ytd ty
CROSS JOIN prev_client_metrics pcm
CROSS JOIN prev_txn_metrics ptm
CROSS JOIN prev_loan_metrics plm
CROSS JOIN prev_balance_metrics pbm;
