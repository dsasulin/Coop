-- ============================================================================
-- Procedure: sp_build_client_360
-- Layer: Gold
-- Reads: gold.dim_client, gold.fact_transactions_daily,
--        gold.fact_loan_performance, gold.fact_account_balance_daily
-- Writes: gold.client_360_view
-- Calls: fn_calc_profitability (inline), fn_calc_risk_score (inline)
-- ============================================================================

INSERT OVERWRITE TABLE gold.client_360_view

WITH txn_summary AS (
    -- 30-day transaction aggregates from fact_transactions_daily
    SELECT
        client_id,
        SUM(transaction_count)                   AS transactions_30d,
        SUM(total_amount)                        AS transaction_volume_30d,
        CASE
            WHEN SUM(transaction_count) > 0
            THEN SUM(total_amount) / SUM(transaction_count)
            ELSE 0
        END                                      AS avg_transaction_30d,
        SUM(CASE WHEN transaction_type = 'DEPOSIT'
            THEN total_amount ELSE 0 END)        AS deposits_30d,
        SUM(CASE WHEN transaction_type = 'WITHDRAWAL'
            THEN total_amount ELSE 0 END)        AS withdrawals_30d,
        SUM(CASE WHEN transaction_type = 'TRANSFER'
            THEN total_amount ELSE 0 END)        AS transfers_30d,
        MAX(transaction_date)                    AS last_transaction_date,
        SUM(CASE WHEN has_suspicious_transactions = TRUE
            THEN suspicious_transaction_count ELSE 0 END) AS suspicious_activities,
        SUM(CASE WHEN has_suspicious_transactions = TRUE
            THEN 1 ELSE 0 END)                   AS fraud_alerts,
        -- channel_preference: most frequent transaction type
        MAX(transaction_type)                    AS most_freq_type,
        SUM(successful_count)                    AS total_successful,
        SUM(total_amount)                        AS total_revenue_raw
    FROM gold.fact_transactions_daily
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY client_id
),

balance_summary AS (
    -- Latest balances from fact_account_balance_daily
    SELECT
        b.client_id,
        SUM(b.current_balance)                   AS total_balance,
        AVG(b.current_balance)                   AS avg_balance,
        SUM(CASE WHEN b.account_type = 'CHECKING' THEN 1 ELSE 0 END) AS checking_accounts,
        SUM(CASE WHEN b.account_type = 'SAVINGS'  THEN 1 ELSE 0 END) AS savings_accounts,
        SUM(CASE WHEN b.account_type = 'LOAN'     THEN 1 ELSE 0 END) AS loan_accounts
    FROM gold.fact_account_balance_daily b
    INNER JOIN (
        SELECT client_id, account_id, MAX(snapshot_date) AS max_date
        FROM gold.fact_account_balance_daily
        GROUP BY client_id, account_id
    ) latest
        ON b.client_id = latest.client_id
        AND b.account_id = latest.account_id
        AND b.snapshot_date = latest.max_date
    GROUP BY b.client_id
),

loan_summary AS (
    -- Loan metrics from fact_loan_performance (latest snapshot per loan)
    SELECT
        lp.client_id,
        SUM(lp.loan_amount)                      AS total_loan_amount,
        SUM(lp.outstanding_balance)              AS total_outstanding_balance,
        SUM(lp.paid_amount)                      AS total_loan_payment,
        SUM(CASE WHEN lp.is_delinquent = TRUE THEN 1 ELSE 0 END) AS delinquent_loans,
        COUNT(*)                                  AS total_loans_snap,
        SUM(CASE WHEN lp.remaining_months > 0 THEN 1 ELSE 0 END) AS active_loans
    FROM gold.fact_loan_performance lp
    INNER JOIN (
        SELECT loan_id, MAX(snapshot_date) AS max_date
        FROM gold.fact_loan_performance
        GROUP BY loan_id
    ) latest
        ON lp.loan_id = latest.loan_id
        AND lp.snapshot_date = latest.max_date
    GROUP BY lp.client_id
)

SELECT
    -- 1. client_id
    dc.client_id,
    -- 2. snapshot_date
    CURRENT_DATE()                              AS snapshot_date,
    -- 3. full_name
    dc.full_name,
    -- 4. email
    dc.email,
    -- 5. phone
    dc.phone,
    -- 6. age
    dc.age,
    -- 7. age_group
    dc.age_group,
    -- 8. city
    dc.city,
    -- 9. country
    dc.country,
    -- 10. risk_category
    dc.risk_category,
    -- 11. credit_score
    dc.credit_score,
    -- 12. credit_score_category
    dc.credit_score_category,
    -- 13. annual_income
    dc.annual_income,
    -- 14. income_category
    dc.income_category,
    -- 15. client_segment
    dc.client_segment,
    -- 16. total_accounts
    dc.total_accounts,
    -- 17. active_accounts
    dc.total_active_accounts                    AS active_accounts,
    -- 18. checking_accounts
    COALESCE(bs.checking_accounts, 0)           AS checking_accounts,
    -- 19. savings_accounts
    COALESCE(bs.savings_accounts, 0)            AS savings_accounts,
    -- 20. loan_accounts
    COALESCE(bs.loan_accounts, 0)               AS loan_accounts,
    -- 21. total_balance
    COALESCE(bs.total_balance, 0)               AS total_balance,
    -- 22. avg_balance
    COALESCE(bs.avg_balance, 0)                 AS avg_balance,
    -- 23. total_products
    dc.total_products,
    -- 24. total_contracts
    dc.total_contracts,
    -- 25. total_cards
    dc.total_cards,
    -- 26. active_cards
    dc.total_cards                              AS active_cards,
    -- 27. total_loans
    dc.total_loans,
    -- 28. active_loans
    COALESCE(ls.active_loans, 0)                AS active_loans,
    -- 29. total_loan_amount
    COALESCE(ls.total_loan_amount, 0)           AS total_loan_amount,
    -- 30. total_outstanding_balance
    COALESCE(ls.total_outstanding_balance, 0)   AS total_outstanding_balance,
    -- 31. total_loan_payment
    COALESCE(ls.total_loan_payment, 0)          AS total_loan_payment,
    -- 32. delinquent_loans
    COALESCE(ls.delinquent_loans, 0)            AS delinquent_loans,
    -- 33. transactions_30d
    COALESCE(ts.transactions_30d, 0)            AS transactions_30d,
    -- 34. transaction_volume_30d
    COALESCE(ts.transaction_volume_30d, 0)      AS transaction_volume_30d,
    -- 35. avg_transaction_30d
    COALESCE(ts.avg_transaction_30d, 0)         AS avg_transaction_30d,
    -- 36. deposits_30d
    COALESCE(ts.deposits_30d, 0)                AS deposits_30d,
    -- 37. withdrawals_30d
    COALESCE(ts.withdrawals_30d, 0)             AS withdrawals_30d,
    -- 38. transfers_30d
    COALESCE(ts.transfers_30d, 0)               AS transfers_30d,
    -- 39. last_transaction_date
    ts.last_transaction_date,
    -- 40. days_since_last_transaction
    CASE
        WHEN ts.last_transaction_date IS NOT NULL
        THEN DATEDIFF(CURRENT_DATE(), ts.last_transaction_date)
        ELSE NULL
    END                                         AS days_since_last_transaction,
    -- 41. transaction_frequency
    CASE
        WHEN COALESCE(ts.transactions_30d, 0) >= 60 THEN 'DAILY'
        WHEN COALESCE(ts.transactions_30d, 0) >= 12 THEN 'WEEKLY'
        WHEN COALESCE(ts.transactions_30d, 0) >= 3  THEN 'MONTHLY'
        WHEN COALESCE(ts.transactions_30d, 0) >= 1  THEN 'RARE'
        ELSE 'INACTIVE'
    END                                         AS transaction_frequency,
    -- 42. channel_preference
    CASE
        WHEN ts.most_freq_type IS NULL THEN 'UNKNOWN'
        WHEN ts.deposits_30d >= ts.withdrawals_30d
             AND ts.deposits_30d >= ts.transfers_30d THEN 'DEPOSIT'
        WHEN ts.withdrawals_30d >= ts.transfers_30d  THEN 'WITHDRAWAL'
        ELSE 'TRANSFER'
    END                                         AS channel_preference,
    -- 43. customer_lifetime_value
    dc.client_lifetime_value                    AS customer_lifetime_value,
    -- 44. total_revenue
    COALESCE(ts.total_revenue_raw, 0)           AS total_revenue,
    -- 45. total_fees_paid
    CAST(COALESCE(ts.total_revenue_raw, 0) * 0.02 AS DECIMAL(18,2)) AS total_fees_paid,
    -- 46. profitability_score: inline fn_calc_profitability
    CASE
        WHEN ts.total_revenue_raw IS NULL THEN CAST(0.00 AS DECIMAL(10,2))
        ELSE CAST(
            LEAST(
                LEAST(
                    (COALESCE(ts.total_revenue_raw, 0)
                     + COALESCE(ts.total_revenue_raw, 0) * 0.02) / 5000.0,
                    60.0
                )
                + LEAST(dc.customer_tenure_years * 4, 40.0),
                100.0
            ) AS DECIMAL(10,2))
    END                                         AS profitability_score,
    -- 47. risk_score: inline fn_calc_risk_score
    CAST(
        LEAST(
            LEAST(COALESCE(ls.delinquent_loans, 0) * 25, 50)
            + LEAST(COALESCE(ts.suspicious_activities, 0) * 10, 30)
            + CASE
                WHEN dc.credit_score >= 740 THEN 0
                WHEN dc.credit_score >= 670 THEN 5
                WHEN dc.credit_score >= 580 THEN 10
                ELSE 20
              END,
            100
        ) AS DECIMAL(5,2)
    )                                           AS risk_score,
    -- 48. fraud_alerts
    COALESCE(ts.fraud_alerts, 0)                AS fraud_alerts,
    -- 49. suspicious_activities
    COALESCE(ts.suspicious_activities, 0)       AS suspicious_activities,
    -- 50. created_timestamp
    CURRENT_TIMESTAMP()                         AS created_timestamp,
    -- 51. updated_timestamp
    CURRENT_TIMESTAMP()                         AS updated_timestamp

FROM gold.dim_client dc
LEFT JOIN txn_summary ts
    ON dc.client_id = ts.client_id
LEFT JOIN balance_summary bs
    ON dc.client_id = bs.client_id
LEFT JOIN loan_summary ls
    ON dc.client_id = ls.client_id
WHERE dc.is_current = TRUE;
