-- ============================================================================
-- Procedure: sp_validate_data_quality
-- Layer: Silver
-- Reads: silver.clients, silver.accounts, silver.transactions,
--        silver.contracts, silver.loans
-- Writes: silver.dq_results
-- Calls: none
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.dq_results (
    check_date         DATE,
    table_name         STRING,
    check_type         STRING,
    check_description  STRING,
    total_records      BIGINT,
    failed_records     BIGINT,
    pass_rate          DECIMAL(5,4),
    severity           STRING,
    created_timestamp  TIMESTAMP
)
STORED AS PARQUET;

INSERT INTO TABLE silver.dq_results

-- 1. NULL check: silver.clients.client_id
SELECT
    CURRENT_DATE()                           AS check_date,
    'silver.clients'                         AS table_name,
    'NULL_CHECK'                             AS check_type,
    'client_id must not be NULL'             AS check_description,
    COUNT(*)                                 AS total_records,
    SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS failed_records,
    1.0 - CAST(SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*)                           AS pass_rate,
    'CRITICAL'                               AS severity,
    CURRENT_TIMESTAMP()                      AS created_timestamp
FROM silver.clients

UNION ALL

-- 2. NULL check: silver.clients.email
SELECT
    CURRENT_DATE(),
    'silver.clients',
    'NULL_CHECK',
    'email must not be NULL',
    COUNT(*),
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.clients

UNION ALL

-- 3. NULL check: silver.clients.credit_score
SELECT
    CURRENT_DATE(),
    'silver.clients',
    'NULL_CHECK',
    'credit_score must not be NULL',
    COUNT(*),
    SUM(CASE WHEN credit_score IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN credit_score IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.clients

UNION ALL

-- 4. Range check: silver.clients.credit_score BETWEEN 300 AND 850
SELECT
    CURRENT_DATE(),
    'silver.clients',
    'RANGE_CHECK',
    'credit_score must be between 300 and 850',
    COUNT(*),
    SUM(CASE WHEN credit_score NOT BETWEEN 300 AND 850 THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN credit_score NOT BETWEEN 300 AND 850 THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'MAJOR',
    CURRENT_TIMESTAMP()
FROM silver.clients

UNION ALL

-- 5. NULL check: silver.accounts.account_id
SELECT
    CURRENT_DATE(),
    'silver.accounts',
    'NULL_CHECK',
    'account_id must not be NULL',
    COUNT(*),
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.accounts

UNION ALL

-- 6. NULL check: silver.accounts.client_id
SELECT
    CURRENT_DATE(),
    'silver.accounts',
    'NULL_CHECK',
    'client_id must not be NULL',
    COUNT(*),
    SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.accounts

UNION ALL

-- 7. Referential integrity: silver.accounts.client_id -> silver.clients
SELECT
    CURRENT_DATE(),
    'silver.accounts',
    'REFERENTIAL_INTEGRITY',
    'accounts.client_id must exist in clients',
    COUNT(*),
    SUM(CASE WHEN c.client_id IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN c.client_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.accounts a
LEFT JOIN silver.clients c
    ON a.client_id = c.client_id

UNION ALL

-- 8. NULL check: silver.transactions.transaction_id
SELECT
    CURRENT_DATE(),
    'silver.transactions',
    'NULL_CHECK',
    'transaction_id must not be NULL',
    COUNT(*),
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.transactions

UNION ALL

-- 9. Range check: silver.transactions.amount > 0
SELECT
    CURRENT_DATE(),
    'silver.transactions',
    'RANGE_CHECK',
    'amount must be greater than 0',
    COUNT(*),
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'MAJOR',
    CURRENT_TIMESTAMP()
FROM silver.transactions

UNION ALL

-- 10. Referential integrity: silver.contracts.client_id -> silver.clients
SELECT
    CURRENT_DATE(),
    'silver.contracts',
    'REFERENTIAL_INTEGRITY',
    'contracts.client_id must exist in clients',
    COUNT(*),
    SUM(CASE WHEN c.client_id IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN c.client_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.contracts ct
LEFT JOIN silver.clients c
    ON ct.client_id = c.client_id

UNION ALL

-- 11. Referential integrity: silver.loans.contract_id -> silver.contracts
SELECT
    CURRENT_DATE(),
    'silver.loans',
    'REFERENTIAL_INTEGRITY',
    'loans.contract_id must exist in contracts',
    COUNT(*),
    SUM(CASE WHEN ct.contract_id IS NULL THEN 1 ELSE 0 END),
    1.0 - CAST(SUM(CASE WHEN ct.contract_id IS NULL THEN 1 ELSE 0 END) AS DECIMAL(18,4))
        / COUNT(*),
    'CRITICAL',
    CURRENT_TIMESTAMP()
FROM silver.loans l
LEFT JOIN silver.contracts ct
    ON l.contract_id = ct.contract_id;
