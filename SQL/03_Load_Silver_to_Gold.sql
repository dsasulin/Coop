-- ============================================================================
-- ETL Job: Silver -> Gold Layer
-- Description: Build dimensions, facts, and analytical data marts
-- Execution: Run in Hue SQL Editor
-- ============================================================================

-- Set dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

-- ============================================================================
-- 1. DIM_CLIENT - Client Dimension Table
-- ============================================================================
TRUNCATE TABLE gold.dim_client;

INSERT INTO TABLE gold.dim_client
SELECT
    -- Surrogate key (using client_id as key)
    c.client_id as client_key,
    c.client_id,

    -- Demographics
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.email_domain,
    c.phone,
    c.birth_date,
    c.age,

    -- Age group
    CASE
        WHEN c.age < 25 THEN '18-24'
        WHEN c.age < 35 THEN '25-34'
        WHEN c.age < 45 THEN '35-44'
        WHEN c.age < 55 THEN '45-54'
        WHEN c.age < 65 THEN '55-64'
        ELSE '65+'
    END as age_group,

    c.registration_date,
    c.customer_tenure_days,
    ROUND(c.customer_tenure_days / 365.25, 2) as customer_tenure_years,

    -- Location
    c.address,
    c.city,
    c.country,

    -- Region (simplified)
    CASE
        WHEN c.country = 'US' THEN 'NORTH_AMERICA'
        WHEN c.country IN ('UK', 'DE', 'FR', 'IT', 'ES') THEN 'EUROPE'
        ELSE 'OTHER'
    END as region,

    -- Financial profile
    c.risk_category,
    c.credit_score,
    c.credit_score_category,
    c.employment_status,
    c.annual_income,
    c.income_category,

    -- Client segment
    CASE
        WHEN c.annual_income > 150000 AND c.credit_score > 750 THEN 'VIP'
        WHEN c.annual_income > 100000 AND c.credit_score > 700 THEN 'PREMIUM'
        WHEN c.annual_income > 50000 AND c.credit_score > 650 THEN 'REGULAR'
        ELSE 'BASIC'
    END as client_segment,

    -- Client lifetime value (placeholder)
    0.0 as client_lifetime_value,

    -- Account summary
    COALESCE(acct.total_accounts, 0) as total_accounts,
    COALESCE(acct.total_active_accounts, 0) as total_active_accounts,

    -- Product summary
    COALESCE(prod.total_products, 0) as total_products,
    0 as total_contracts,  -- Placeholder
    0 as total_cards,      -- Placeholder
    0 as total_loans,      -- Placeholder

    -- SCD Type 2 fields (simplified - not full implementation)
    c.registration_date as effective_date,
    CAST(NULL AS DATE) as end_date,
    TRUE as is_current,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.clients c

LEFT JOIN (
    SELECT
        client_id,
        COUNT(*) as total_accounts,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as total_active_accounts
    FROM silver.accounts
    GROUP BY client_id
) acct ON c.client_id = acct.client_id

LEFT JOIN (
    SELECT
        client_id,
        COUNT(DISTINCT product_id) as total_products
    FROM silver.client_products
    WHERE is_active = TRUE
    GROUP BY client_id
) prod ON c.client_id = prod.client_id;

-- Check results
SELECT
    COUNT(*) as total_clients,
    COUNT(DISTINCT client_segment) as segments,
    ROUND(AVG(annual_income), 2) as avg_income
FROM gold.dim_client;

-- ============================================================================
-- 2. DIM_PRODUCT - Product Dimension Table
-- ============================================================================
TRUNCATE TABLE gold.dim_product;

INSERT INTO TABLE gold.dim_product
SELECT
    product_id as product_key,
    product_id,
    product_name,
    product_type,
    description,
    interest_rate,
    min_amount,
    max_amount,
    term_months,
    currency,
    active,

    -- Product category (derived from type)
    CASE
        WHEN product_type IN ('CHECKING', 'SAVINGS') THEN 'DEPOSIT_ACCOUNTS'
        WHEN product_type IN ('CREDIT_CARD', 'DEBIT_CARD') THEN 'CARD_PRODUCTS'
        WHEN product_type IN ('PERSONAL_LOAN', 'MORTGAGE', 'AUTO_LOAN') THEN 'LENDING_PRODUCTS'
        WHEN product_type IN ('INVESTMENT', 'INSURANCE') THEN 'INVESTMENT_PRODUCTS'
        ELSE 'OTHER'
    END as product_category,

    -- SCD fields
    created_date as effective_date,
    CAST(NULL AS DATE) as end_date,
    active as is_current,

    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.products;

SELECT COUNT(*) as total_products FROM gold.dim_product;

-- ============================================================================
-- 3. DIM_BRANCH - Branch Dimension Table
-- ============================================================================
TRUNCATE TABLE gold.dim_branch;

INSERT INTO TABLE gold.dim_branch
SELECT
    branch_id as branch_key,
    branch_id,
    branch_code,
    branch_name,
    address,
    city,
    state,
    country,

    -- Region
    CASE
        WHEN country = 'US' THEN 'NORTH_AMERICA'
        WHEN country IN ('UK', 'DE', 'FR', 'IT', 'ES') THEN 'EUROPE'
        ELSE 'OTHER'
    END as region,

    postal_code,
    phone,
    email,
    manager_name,
    open_date,
    branch_age_years,

    -- SCD fields
    open_date as effective_date,
    CAST(NULL AS DATE) as end_date,
    TRUE as is_current,

    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.branches;

SELECT COUNT(*) as total_branches FROM gold.dim_branch;

-- ============================================================================
-- 4. DIM_DATE - Date Dimension Table (Generate date range)
-- ============================================================================
-- Note: This is a simplified version. In production, use a proper date dimension generator

TRUNCATE TABLE gold.dim_date;

-- Generate dates for last 5 years and next 2 years
INSERT INTO TABLE gold.dim_date
SELECT
    CAST(DATE_FORMAT(dt, 'yyyyMMdd') AS INT) as date_key,
    dt as date_value,
    YEAR(dt) as year,
    QUARTER(dt) as quarter,
    MONTH(dt) as month,
    DAYOFMONTH(dt) as day,
    DAYOFWEEK(dt) as day_of_week,
    DAYOFYEAR(dt) as day_of_year,
    WEEKOFYEAR(dt) as week_of_year,

    -- Month name
    CASE MONTH(dt)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,

    -- Day name
    CASE DAYOFWEEK(dt)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,

    -- Is weekend
    CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,

    -- Is holiday (simplified - only major US holidays)
    CASE
        WHEN MONTH(dt) = 1 AND DAYOFMONTH(dt) = 1 THEN TRUE  -- New Year
        WHEN MONTH(dt) = 7 AND DAYOFMONTH(dt) = 4 THEN TRUE  -- Independence Day
        WHEN MONTH(dt) = 12 AND DAYOFMONTH(dt) = 25 THEN TRUE  -- Christmas
        ELSE FALSE
    END as is_holiday,

    -- Fiscal period (assuming fiscal year = calendar year)
    CONCAT('FY', YEAR(dt)) as fiscal_year,
    CONCAT('Q', QUARTER(dt), '-', YEAR(dt)) as fiscal_quarter,

    CURRENT_TIMESTAMP as created_timestamp

FROM (
    -- Generate date range from 2020-01-01 to 2027-12-31
    SELECT DATE_ADD('2020-01-01', pos) as dt
    FROM (
        SELECT posexplode(split(space(2921), ' ')) as (pos, val)
    ) x
) dates;

SELECT
    MIN(date_value) as min_date,
    MAX(date_value) as max_date,
    COUNT(*) as total_dates
FROM gold.dim_date;

-- ============================================================================
-- 5. FACT_TRANSACTIONS_DAILY - Daily Transaction Aggregates
-- ============================================================================

INSERT OVERWRITE TABLE gold.fact_transactions_daily
PARTITION (year, month)
SELECT
    t.transaction_date_only as transaction_date,
    COALESCE(a.client_id, -1) as client_id,
    t.from_account_id as account_id,
    COALESCE(a.branch_code, 'UNKNOWN') as branch_code,
    t.transaction_type_normalized as transaction_type,
    t.category_normalized as category,
    t.currency,

    -- Aggregated metrics
    COUNT(*) as transaction_count,
    ROUND(SUM(t.amount), 2) as total_amount,
    ROUND(AVG(t.amount), 2) as avg_amount,
    ROUND(MIN(t.amount), 2) as min_amount,
    ROUND(MAX(t.amount), 2) as max_amount,

    -- Status breakdown
    SUM(CASE WHEN t.status_normalized = 'COMPLETED' THEN 1 ELSE 0 END) as successful_count,
    SUM(CASE WHEN t.status_normalized = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    SUM(CASE WHEN t.status_normalized = 'PENDING' THEN 1 ELSE 0 END) as pending_count,
    SUM(CASE WHEN t.status_normalized = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_count,

    -- Suspicious transactions
    SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) as suspicious_transaction_count,

    -- Success rate
    ROUND((SUM(CASE WHEN t.status_normalized = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as success_rate,

    -- Has suspicious flag
    CASE WHEN SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END as has_suspicious_transactions,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp,

    -- Partitions
    YEAR(t.transaction_date) as year,
    MONTH(t.transaction_date) as month

FROM silver.transactions t
LEFT JOIN silver.accounts a ON t.from_account_id = a.account_id

WHERE t.transaction_date_only IS NOT NULL

GROUP BY
    t.transaction_date_only,
    a.client_id,
    t.from_account_id,
    a.branch_code,
    t.transaction_type_normalized,
    t.category_normalized,
    t.currency,
    YEAR(t.transaction_date),
    MONTH(t.transaction_date);

-- Check results
SELECT
    year,
    month,
    COUNT(*) as daily_aggregates,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(total_amount), 2) as total_volume
FROM gold.fact_transactions_daily
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- ============================================================================
-- 6. FACT_ACCOUNT_BALANCE_DAILY - Daily Account Balance Snapshots
-- ============================================================================
TRUNCATE TABLE gold.fact_account_balance_daily;

INSERT INTO TABLE gold.fact_account_balance_daily
SELECT
    ab.balance_date,
    a.client_id,
    ab.account_id,
    a.branch_code,
    a.account_type_normalized as account_type,
    ab.currency,

    -- Balance metrics
    ROUND(ab.current_balance, 2) as current_balance,
    ROUND(ab.available_balance, 2) as available_balance,
    ROUND(ab.overdraft_limit, 2) as overdraft_limit,
    ROUND(ab.credit_limit, 2) as credit_limit,
    ab.credit_utilization_pct,
    ab.is_overdrawn,

    -- Account status
    a.is_active as is_account_active,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.account_balances ab
INNER JOIN silver.accounts a ON ab.account_id = a.account_id

WHERE ab.balance_date IS NOT NULL;

SELECT
    COUNT(*) as total_balance_records,
    COUNT(DISTINCT account_id) as unique_accounts,
    ROUND(SUM(current_balance), 2) as total_system_balance
FROM gold.fact_account_balance_daily;

-- ============================================================================
-- 7. FACT_LOAN_PERFORMANCE - Loan Performance Metrics
-- ============================================================================
TRUNCATE TABLE gold.fact_loan_performance;

INSERT INTO TABLE gold.fact_loan_performance
SELECT
    l.loan_id,
    c.client_id,
    l.contract_id,
    l.loan_type,
    l.currency,

    -- Loan details
    ROUND(l.loan_amount, 2) as loan_amount,
    l.interest_rate,
    l.term_months,
    ROUND(l.monthly_payment, 2) as monthly_payment,
    l.start_date,
    l.maturity_date,

    -- Performance metrics
    ROUND(l.outstanding_balance, 2) as outstanding_balance,
    ROUND(l.principal_paid, 2) as principal_paid,
    l.outstanding_pct,
    l.days_to_maturity,

    -- Payment metrics
    l.next_payment_date,
    ROUND(l.next_payment_amount, 2) as next_payment_amount,
    l.payment_day,

    -- Delinquency metrics
    l.delinquency_status_normalized as delinquency_status,
    l.days_past_due,
    l.is_delinquent,
    l.loan_risk_category,

    -- Collateral
    l.collateral_type,
    ROUND(l.collateral_value, 2) as collateral_value,
    l.loan_to_value_ratio,

    -- Client credit profile
    c.credit_score as client_credit_score,
    c.annual_income as client_annual_income,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.loans l
INNER JOIN silver.contracts cnt ON l.contract_id = cnt.contract_id
INNER JOIN silver.clients c ON cnt.client_id = c.client_id;

SELECT
    COUNT(*) as total_loans,
    SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) as delinquent_loans,
    ROUND(SUM(loan_amount), 2) as total_loan_amount,
    ROUND(SUM(outstanding_balance), 2) as total_outstanding
FROM gold.fact_loan_performance;

-- ============================================================================
-- 8. CLIENT_360_VIEW - Comprehensive Client Profile
-- ============================================================================

INSERT OVERWRITE TABLE gold.client_360_view
PARTITION (snapshot_year, snapshot_month)
SELECT
    c.client_id,
    CURRENT_DATE as snapshot_date,

    -- Demographics
    c.full_name,
    c.email,
    c.phone,
    c.age,

    -- Age group
    CASE
        WHEN c.age < 25 THEN '18-24'
        WHEN c.age < 35 THEN '25-34'
        WHEN c.age < 45 THEN '35-44'
        WHEN c.age < 55 THEN '45-54'
        WHEN c.age < 65 THEN '55-64'
        ELSE '65+'
    END as age_group,

    c.city,
    c.country,

    -- Financial profile
    c.risk_category,
    c.credit_score,
    c.credit_score_category,
    c.annual_income,
    c.income_category,

    -- Client segment
    CASE
        WHEN c.annual_income > 150000 AND c.credit_score > 750 THEN 'VIP'
        WHEN c.annual_income > 100000 AND c.credit_score > 700 THEN 'PREMIUM'
        ELSE 'REGULAR'
    END as client_segment,

    -- Account summary
    COALESCE(acct.total_accounts, 0) as total_accounts,
    COALESCE(acct.active_accounts, 0) as active_accounts,
    COALESCE(acct.checking_accounts, 0) as checking_accounts,
    COALESCE(acct.savings_accounts, 0) as savings_accounts,
    COALESCE(acct.loan_accounts, 0) as loan_accounts,

    -- Balance summary
    COALESCE(bal.total_balance, 0) as total_balance,
    COALESCE(bal.avg_balance, 0) as avg_balance,

    -- Product holdings
    COALESCE(prod.total_products, 0) as total_products,
    0 as total_contracts,  -- Placeholder
    COALESCE(cards.total_cards, 0) as total_cards,
    COALESCE(cards.active_cards, 0) as active_cards,

    -- Loan summary
    COALESCE(loans.total_loans, 0) as total_loans,
    COALESCE(loans.active_loans, 0) as active_loans,
    COALESCE(loans.total_loan_amount, 0) as total_loan_amount,
    COALESCE(loans.total_outstanding_balance, 0) as total_outstanding_balance,
    COALESCE(loans.total_loan_payment, 0) as total_loan_payment,
    COALESCE(loans.delinquent_loans, 0) as delinquent_loans,

    -- Transaction behavior (last 30 days)
    COALESCE(txn.transactions_30d, 0) as transactions_30d,
    COALESCE(txn.transaction_volume_30d, 0) as transaction_volume_30d,
    COALESCE(txn.avg_transaction_30d, 0) as avg_transaction_30d,
    COALESCE(txn.deposits_30d, 0) as deposits_30d,
    COALESCE(txn.withdrawals_30d, 0) as withdrawals_30d,
    COALESCE(txn.transfers_30d, 0) as transfers_30d,

    -- Engagement metrics
    txn.last_transaction_date,
    COALESCE(DATEDIFF(CURRENT_DATE, txn.last_transaction_date), 999) as days_since_last_transaction,

    -- Transaction frequency
    CASE
        WHEN COALESCE(txn.transactions_30d, 0) >= 20 THEN 'DAILY'
        WHEN COALESCE(txn.transactions_30d, 0) >= 4 THEN 'WEEKLY'
        WHEN COALESCE(txn.transactions_30d, 0) >= 1 THEN 'MONTHLY'
        ELSE 'INACTIVE'
    END as transaction_frequency,

    'MOBILE' as channel_preference,  -- Placeholder

    -- Lifetime value (placeholders)
    0.0 as customer_lifetime_value,
    0.0 as total_revenue,
    0.0 as total_fees_paid,
    0.0 as profitability_score,

    -- Risk indicators (placeholders)
    0.0 as risk_score,
    0 as fraud_alerts,
    0 as suspicious_activities,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp,

    -- Partitions
    YEAR(CURRENT_DATE) as snapshot_year,
    MONTH(CURRENT_DATE) as snapshot_month

FROM silver.clients c

-- Account summary
LEFT JOIN (
    SELECT
        client_id,
        COUNT(*) as total_accounts,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_accounts,
        SUM(CASE WHEN account_type_normalized = 'CHECKING' THEN 1 ELSE 0 END) as checking_accounts,
        SUM(CASE WHEN account_type_normalized = 'SAVINGS' THEN 1 ELSE 0 END) as savings_accounts,
        SUM(CASE WHEN account_type_normalized = 'LOAN' THEN 1 ELSE 0 END) as loan_accounts
    FROM silver.accounts
    GROUP BY client_id
) acct ON c.client_id = acct.client_id

-- Balance summary
LEFT JOIN (
    SELECT
        a.client_id,
        SUM(ab.current_balance) as total_balance,
        AVG(ab.current_balance) as avg_balance
    FROM silver.account_balances ab
    INNER JOIN silver.accounts a ON ab.account_id = a.account_id
    GROUP BY a.client_id
) bal ON c.client_id = bal.client_id

-- Product holdings
LEFT JOIN (
    SELECT
        client_id,
        COUNT(DISTINCT product_id) as total_products
    FROM silver.client_products
    WHERE is_active = TRUE
    GROUP BY client_id
) prod ON c.client_id = prod.client_id

-- Card summary
LEFT JOIN (
    SELECT
        client_id,
        COUNT(*) as total_cards,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_cards
    FROM silver.cards
    GROUP BY client_id
) cards ON c.client_id = cards.client_id

-- Loan summary
LEFT JOIN (
    SELECT
        cnt.client_id,
        COUNT(*) as total_loans,
        SUM(CASE WHEN l.days_past_due = 0 THEN 1 ELSE 0 END) as active_loans,
        SUM(l.loan_amount) as total_loan_amount,
        SUM(l.outstanding_balance) as total_outstanding_balance,
        SUM(l.monthly_payment) as total_loan_payment,
        SUM(CASE WHEN l.is_delinquent THEN 1 ELSE 0 END) as delinquent_loans
    FROM silver.loans l
    INNER JOIN silver.contracts cnt ON l.contract_id = cnt.contract_id
    GROUP BY cnt.client_id
) loans ON c.client_id = loans.client_id

-- Transaction activity (last 30 days)
LEFT JOIN (
    SELECT
        a.client_id,
        COUNT(*) as transactions_30d,
        SUM(t.amount) as transaction_volume_30d,
        AVG(t.amount) as avg_transaction_30d,
        SUM(CASE WHEN t.transaction_type_normalized = 'DEPOSIT' THEN 1 ELSE 0 END) as deposits_30d,
        SUM(CASE WHEN t.transaction_type_normalized = 'WITHDRAWAL' THEN 1 ELSE 0 END) as withdrawals_30d,
        SUM(CASE WHEN t.transaction_type_normalized = 'TRANSFER' THEN 1 ELSE 0 END) as transfers_30d,
        MAX(t.transaction_date) as last_transaction_date
    FROM silver.transactions t
    INNER JOIN silver.accounts a ON t.from_account_id = a.account_id
    WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE, 30)
      AND t.status_normalized = 'COMPLETED'
    GROUP BY a.client_id
) txn ON c.client_id = txn.client_id;

-- Check results
SELECT
    COUNT(*) as total_clients,
    COUNT(DISTINCT client_segment) as segments,
    SUM(total_accounts) as total_accounts,
    ROUND(SUM(total_balance), 2) as total_balance
FROM gold.client_360_view;

-- Segment breakdown
SELECT
    client_segment,
    COUNT(*) as client_count,
    ROUND(AVG(credit_score), 0) as avg_credit_score,
    ROUND(AVG(total_balance), 2) as avg_balance,
    ROUND(AVG(transactions_30d), 1) as avg_transactions_30d
FROM gold.client_360_view
GROUP BY client_segment
ORDER BY client_count DESC;

-- ============================================================================
-- 9. PRODUCT_PERFORMANCE_SUMMARY - Product Performance Dashboard
-- ============================================================================
TRUNCATE TABLE gold.product_performance_summary;

INSERT INTO TABLE gold.product_performance_summary
SELECT
    p.product_id,
    p.product_name,
    p.product_type,

    -- Adoption metrics
    COALESCE(cp.total_clients, 0) as total_clients,
    COALESCE(cp.active_clients, 0) as active_clients,

    -- Contract metrics
    COALESCE(cnt.total_contracts, 0) as total_contracts,
    COALESCE(cnt.active_contracts, 0) as active_contracts,
    COALESCE(cnt.total_contract_value, 0) as total_contract_value,
    COALESCE(cnt.avg_contract_value, 0) as avg_contract_value,

    -- Revenue metrics (simplified)
    COALESCE(cnt.total_monthly_payment, 0) as total_monthly_revenue,
    COALESCE(cnt.total_interest_revenue, 0) as total_interest_revenue,

    -- Growth metrics
    COALESCE(growth.new_clients_30d, 0) as new_clients_last_30_days,
    COALESCE(growth.churned_clients_30d, 0) as churned_clients_last_30_days,

    -- Product rating (placeholder)
    0.0 as average_rating,
    0 as total_reviews,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.products p

-- Client adoption
LEFT JOIN (
    SELECT
        product_id,
        COUNT(DISTINCT client_id) as total_clients,
        COUNT(DISTINCT CASE WHEN is_active THEN client_id END) as active_clients
    FROM silver.client_products
    GROUP BY product_id
) cp ON p.product_id = cp.product_id

-- Contract metrics
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*) as total_contracts,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_contracts,
        SUM(contract_amount) as total_contract_value,
        AVG(contract_amount) as avg_contract_value,
        SUM(monthly_payment) as total_monthly_payment,
        SUM(contract_amount * interest_rate / 100) as total_interest_revenue
    FROM silver.contracts
    GROUP BY product_id
) cnt ON p.product_id = cnt.product_id

-- Growth metrics (last 30 days)
LEFT JOIN (
    SELECT
        product_id,
        COUNT(DISTINCT CASE WHEN start_date >= DATE_SUB(CURRENT_DATE, 30) THEN client_id END) as new_clients_30d,
        COUNT(DISTINCT CASE WHEN end_date >= DATE_SUB(CURRENT_DATE, 30) AND end_date < CURRENT_DATE THEN client_id END) as churned_clients_30d
    FROM silver.client_products
    GROUP BY product_id
) growth ON p.product_id = growth.product_id;

SELECT
    COUNT(*) as total_products,
    SUM(total_clients) as total_product_clients,
    ROUND(SUM(total_contract_value), 2) as total_contract_value
FROM gold.product_performance_summary;

-- ============================================================================
-- 10. BRANCH_PERFORMANCE_DASHBOARD - Branch Performance Metrics
-- ============================================================================
TRUNCATE TABLE gold.branch_performance_dashboard;

INSERT INTO TABLE gold.branch_performance_dashboard
SELECT
    b.branch_id,
    b.branch_code,
    b.branch_name,
    b.city,
    b.state,
    b.country,

    -- Account metrics
    COALESCE(acct.total_accounts, 0) as total_accounts,
    COALESCE(acct.active_accounts, 0) as active_accounts,
    COALESCE(acct.new_accounts_30d, 0) as new_accounts_last_30_days,

    -- Client metrics
    COALESCE(acct.total_clients, 0) as total_clients,

    -- Employee metrics
    COALESCE(emp.total_employees, 0) as total_employees,
    COALESCE(emp.avg_employee_tenure, 0) as avg_employee_tenure_years,

    -- Financial metrics
    COALESCE(bal.total_deposits, 0) as total_deposits,
    COALESCE(bal.avg_account_balance, 0) as avg_account_balance,

    -- Transaction metrics (last 30 days)
    COALESCE(txn.total_transactions_30d, 0) as total_transactions_last_30_days,
    COALESCE(txn.total_transaction_volume_30d, 0) as total_transaction_volume_last_30_days,

    -- Card metrics
    COALESCE(cards.total_cards_issued, 0) as total_cards_issued,
    COALESCE(cards.active_cards, 0) as active_cards,

    -- Performance score (simplified calculation)
    ROUND(
        (COALESCE(acct.active_accounts, 0) * 0.3 +
         COALESCE(bal.total_deposits, 0) / 10000 * 0.4 +
         COALESCE(txn.total_transactions_30d, 0) * 0.3),
        2
    ) as branch_performance_score,

    -- Technical fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_TIMESTAMP as updated_timestamp

FROM silver.branches b

-- Account and client metrics
LEFT JOIN (
    SELECT
        branch_code,
        COUNT(*) as total_accounts,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_accounts,
        COUNT(CASE WHEN open_date >= DATE_SUB(CURRENT_DATE, 30) THEN 1 END) as new_accounts_30d,
        COUNT(DISTINCT client_id) as total_clients
    FROM silver.accounts
    GROUP BY branch_code
) acct ON b.branch_code = acct.branch_code

-- Employee metrics
LEFT JOIN (
    SELECT
        branch_id,
        COUNT(*) as total_employees,
        ROUND(AVG(tenure_years), 1) as avg_employee_tenure
    FROM silver.employees
    GROUP BY branch_id
) emp ON b.branch_id = emp.branch_id

-- Balance metrics
LEFT JOIN (
    SELECT
        a.branch_code,
        SUM(ab.current_balance) as total_deposits,
        AVG(ab.current_balance) as avg_account_balance
    FROM silver.account_balances ab
    INNER JOIN silver.accounts a ON ab.account_id = a.account_id
    GROUP BY a.branch_code
) bal ON b.branch_code = bal.branch_code

-- Transaction metrics (last 30 days)
LEFT JOIN (
    SELECT
        a.branch_code,
        COUNT(*) as total_transactions_30d,
        SUM(t.amount) as total_transaction_volume_30d
    FROM silver.transactions t
    INNER JOIN silver.accounts a ON t.from_account_id = a.account_id
    WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE, 30)
      AND t.status_normalized = 'COMPLETED'
    GROUP BY a.branch_code
) txn ON b.branch_code = txn.branch_code

-- Card metrics
LEFT JOIN (
    SELECT
        a.branch_code,
        COUNT(*) as total_cards_issued,
        SUM(CASE WHEN c.is_active THEN 1 ELSE 0 END) as active_cards
    FROM silver.cards c
    INNER JOIN silver.accounts a ON c.account_id = a.account_id
    GROUP BY a.branch_code
) cards ON b.branch_code = cards.branch_code;

SELECT
    COUNT(*) as total_branches,
    ROUND(AVG(total_accounts), 0) as avg_accounts_per_branch,
    ROUND(SUM(total_deposits), 2) as total_system_deposits
FROM gold.branch_performance_dashboard;

-- ============================================================================
-- FINAL SUMMARY - Gold Layer Statistics
-- ============================================================================
SELECT 'Gold Layer Load Summary' as summary_title;

SELECT
    'dim_client' as table_name,
    COUNT(*) as record_count,
    'Dimension' as table_type
FROM gold.dim_client
UNION ALL
SELECT 'dim_product', COUNT(*), 'Dimension' FROM gold.dim_product
UNION ALL
SELECT 'dim_branch', COUNT(*), 'Dimension' FROM gold.dim_branch
UNION ALL
SELECT 'dim_date', COUNT(*), 'Dimension' FROM gold.dim_date
UNION ALL
SELECT 'fact_transactions_daily', COUNT(*), 'Fact' FROM gold.fact_transactions_daily
UNION ALL
SELECT 'fact_account_balance_daily', COUNT(*), 'Fact' FROM gold.fact_account_balance_daily
UNION ALL
SELECT 'fact_loan_performance', COUNT(*), 'Fact' FROM gold.fact_loan_performance
UNION ALL
SELECT 'client_360_view', COUNT(*), 'Data Mart' FROM gold.client_360_view
UNION ALL
SELECT 'product_performance_summary', COUNT(*), 'Data Mart' FROM gold.product_performance_summary
UNION ALL
SELECT 'branch_performance_dashboard', COUNT(*), 'Data Mart' FROM gold.branch_performance_dashboard
ORDER BY table_type, table_name;

-- ============================================================================
-- END OF SILVER TO GOLD ETL
-- ============================================================================
