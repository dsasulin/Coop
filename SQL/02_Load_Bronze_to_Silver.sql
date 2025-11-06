-- ============================================================================
-- ETL Job: Bronze -> Silver Layer
-- Description: Clean, validate, and transform data from bronze to silver
-- Execution: Run in Hue SQL Editor
-- ============================================================================

-- Set dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

-- ============================================================================
-- 1. CLIENTS - Clean and Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.clients;

INSERT INTO TABLE silver.clients
SELECT
    -- Original fields
    client_id,
    first_name,
    last_name,

    -- Derived: Full name
    CONCAT(first_name, ' ', last_name) as full_name,

    -- Email normalization
    LOWER(TRIM(email)) as email,

    -- Email domain extraction
    CASE
        WHEN email IS NOT NULL AND email LIKE '%@%'
        THEN SUBSTRING(LOWER(email), LOCATE('@', LOWER(email)) + 1)
        ELSE NULL
    END as email_domain,

    -- Phone normalization (remove special characters)
    REGEXP_REPLACE(phone, '[^0-9]', '') as phone,

    birth_date,

    -- Age calculation
    CAST(FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) AS INT) as age,

    registration_date,

    -- Customer tenure
    DATEDIFF(CURRENT_DATE, registration_date) as customer_tenure_days,

    address,
    city,
    state,
    country,
    postal_code,
    occupation,
    employment_status,
    annual_income,

    -- Income category
    CASE
        WHEN annual_income < 30000 THEN 'LOW'
        WHEN annual_income >= 30000 AND annual_income < 60000 THEN 'LOWER_MIDDLE'
        WHEN annual_income >= 60000 AND annual_income < 100000 THEN 'MIDDLE'
        WHEN annual_income >= 100000 AND annual_income < 150000 THEN 'UPPER_MIDDLE'
        WHEN annual_income >= 150000 THEN 'HIGH'
        ELSE 'UNKNOWN'
    END as income_category,

    credit_score,

    -- Credit score category
    CASE
        WHEN credit_score IS NULL THEN 'UNKNOWN'
        WHEN credit_score < 580 THEN 'POOR'
        WHEN credit_score >= 580 AND credit_score < 670 THEN 'FAIR'
        WHEN credit_score >= 670 AND credit_score < 740 THEN 'GOOD'
        WHEN credit_score >= 740 AND credit_score < 800 THEN 'VERY_GOOD'
        WHEN credit_score >= 800 THEN 'EXCELLENT'
        ELSE 'UNKNOWN'
    END as credit_score_category,

    -- Risk category
    CASE
        WHEN credit_score >= 740 AND annual_income >= 75000 THEN 'LOW_RISK'
        WHEN credit_score >= 670 AND annual_income >= 50000 THEN 'MEDIUM_RISK'
        WHEN credit_score >= 580 THEN 'HIGH_RISK'
        ELSE 'VERY_HIGH_RISK'
    END as risk_category,

    -- Data Quality Score (0-1 scale)
    ROUND(
        (CASE WHEN first_name IS NOT NULL THEN 0.10 ELSE 0 END +
         CASE WHEN last_name IS NOT NULL THEN 0.10 ELSE 0 END +
         CASE WHEN email IS NOT NULL THEN 0.15 ELSE 0 END +
         CASE WHEN phone IS NOT NULL THEN 0.15 ELSE 0 END +
         CASE WHEN birth_date IS NOT NULL THEN 0.10 ELSE 0 END +
         CASE WHEN address IS NOT NULL THEN 0.10 ELSE 0 END +
         CASE WHEN city IS NOT NULL THEN 0.10 ELSE 0 END +
         CASE WHEN credit_score IS NOT NULL THEN 0.10 ELSE 0 END +
         CASE WHEN annual_income IS NOT NULL THEN 0.10 ELSE 0 END),
        2
    ) as dq_score,

    -- Data Quality Issues
    CONCAT_WS('; ',
        CASE WHEN first_name IS NULL THEN 'missing_first_name' END,
        CASE WHEN last_name IS NULL THEN 'missing_last_name' END,
        CASE WHEN email IS NULL THEN 'missing_email' END,
        CASE WHEN email IS NOT NULL AND email NOT LIKE '%@%' THEN 'invalid_email_format' END,
        CASE WHEN phone IS NULL THEN 'missing_phone' END,
        CASE WHEN birth_date IS NULL THEN 'missing_birth_date' END,
        CASE WHEN credit_score IS NULL THEN 'missing_credit_score' END,
        CASE WHEN credit_score < 300 OR credit_score > 850 THEN 'invalid_credit_score' END,
        CASE WHEN annual_income IS NULL THEN 'missing_annual_income' END,
        CASE WHEN annual_income < 0 THEN 'negative_income' END
    ) as dq_issues,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.clients' as source_system,

    -- Partition column
    YEAR(registration_date) as registration_year

FROM bronze.clients
WHERE client_id IS NOT NULL;  -- Basic deduplication

-- Check results
SELECT
    COUNT(*) as total_records,
    ROUND(AVG(dq_score), 3) as avg_dq_score,
    MIN(dq_score) as min_dq_score,
    COUNT(CASE WHEN dq_score < 0.8 THEN 1 END) as low_quality_count
FROM silver.clients;

-- ============================================================================
-- 2. PRODUCTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.products;

INSERT INTO TABLE silver.products
SELECT
    product_id,
    TRIM(product_name) as product_name,
    UPPER(TRIM(product_type)) as product_type,
    description,
    interest_rate,
    min_amount,
    max_amount,
    term_months,
    UPPER(currency) as currency,
    active,
    created_date,
    updated_date,
    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.products' as source_system
FROM bronze.products
WHERE product_id IS NOT NULL;

SELECT COUNT(*) as silver_products_count FROM silver.products;

-- ============================================================================
-- 3. CONTRACTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.contracts;

INSERT INTO TABLE silver.contracts
SELECT
    contract_id,
    client_id,
    product_id,
    contract_number,
    contract_date,
    start_date,
    end_date,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED', 'OPEN') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('CLOSED', 'TERMINATED', 'COMPLETED') THEN 'CLOSED'
        WHEN UPPER(TRIM(status)) IN ('SUSPENDED', 'FROZEN') THEN 'SUSPENDED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'PENDING_ACTIVATION') THEN 'PENDING'
        ELSE 'UNKNOWN'
    END as status_normalized,

    contract_amount,
    UPPER(currency) as currency,
    interest_rate,
    term_months,
    monthly_payment,

    -- Derived: Contract duration in days
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) as contract_duration_days,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED', 'OPEN')
             AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END as is_active,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.contracts' as source_system
FROM bronze.contracts
WHERE contract_id IS NOT NULL;

SELECT COUNT(*) as silver_contracts_count FROM silver.contracts;

-- ============================================================================
-- 4. ACCOUNTS - Transform to Silver
-- ============================================================================

INSERT OVERWRITE TABLE silver.accounts
PARTITION (open_year)
SELECT
    account_id,
    client_id,
    contract_id,
    account_number,

    -- Account type normalization
    CASE
        WHEN UPPER(TRIM(account_type)) IN ('CHECKING', 'CURRENT', 'CHK') THEN 'CHECKING'
        WHEN UPPER(TRIM(account_type)) IN ('SAVINGS', 'SAV', 'DEPOSIT') THEN 'SAVINGS'
        WHEN UPPER(TRIM(account_type)) IN ('CREDIT', 'LOAN', 'LENDING') THEN 'LOAN'
        WHEN UPPER(TRIM(account_type)) IN ('INVESTMENT', 'INV') THEN 'INVESTMENT'
        ELSE 'OTHER'
    END as account_type_normalized,

    UPPER(currency) as currency,
    open_date,
    close_date,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'OPEN') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('CLOSED', 'TERMINATED') THEN 'CLOSED'
        WHEN UPPER(TRIM(status)) IN ('SUSPENDED', 'FROZEN') THEN 'SUSPENDED'
        WHEN UPPER(TRIM(status)) IN ('DORMANT', 'INACTIVE') THEN 'DORMANT'
        ELSE 'UNKNOWN'
    END as status_normalized,

    branch_code,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END as is_active,

    -- Account age in days
    DATEDIFF(COALESCE(close_date, CURRENT_DATE), open_date) as account_age_days,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.accounts' as source_system,

    -- Partition
    YEAR(open_date) as open_year

FROM bronze.accounts
WHERE account_id IS NOT NULL;

SELECT COUNT(*) as silver_accounts_count FROM silver.accounts;

-- ============================================================================
-- 5. TRANSACTIONS - Transform to Silver (with partitioning)
-- ============================================================================

INSERT OVERWRITE TABLE silver.transactions
PARTITION (transaction_year, transaction_month)
SELECT
    transaction_id,
    from_account_id,
    to_account_id,
    transaction_date,

    -- Date components
    TO_DATE(transaction_date) as transaction_date_only,
    HOUR(transaction_date) as transaction_hour,

    -- Is weekend flag
    CASE
        WHEN DAYOFWEEK(transaction_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END as is_weekend,

    amount,
    UPPER(currency) as currency,

    -- Transaction type normalization
    CASE
        WHEN UPPER(TRIM(transaction_type)) IN ('DEPOSIT', 'DEP', 'CREDIT') THEN 'DEPOSIT'
        WHEN UPPER(TRIM(transaction_type)) IN ('WITHDRAWAL', 'WD', 'DEBIT') THEN 'WITHDRAWAL'
        WHEN UPPER(TRIM(transaction_type)) IN ('TRANSFER', 'TRF', 'XFER') THEN 'TRANSFER'
        WHEN UPPER(TRIM(transaction_type)) IN ('PAYMENT', 'PAY', 'PMT') THEN 'PAYMENT'
        WHEN UPPER(TRIM(transaction_type)) IN ('FEE', 'CHARGE') THEN 'FEE'
        ELSE 'OTHER'
    END as transaction_type_normalized,

    -- Category normalization
    CASE
        WHEN UPPER(TRIM(category)) IN ('GROCERIES', 'GROCERY', 'FOOD') THEN 'GROCERIES'
        WHEN UPPER(TRIM(category)) IN ('RESTAURANT', 'DINING', 'FOOD_DINING') THEN 'DINING'
        WHEN UPPER(TRIM(category)) IN ('TRANSPORT', 'TRANSPORTATION', 'TRAVEL') THEN 'TRANSPORTATION'
        WHEN UPPER(TRIM(category)) IN ('UTILITIES', 'UTILITY', 'BILLS') THEN 'UTILITIES'
        WHEN UPPER(TRIM(category)) IN ('ENTERTAINMENT', 'LEISURE') THEN 'ENTERTAINMENT'
        WHEN UPPER(TRIM(category)) IN ('SHOPPING', 'RETAIL') THEN 'SHOPPING'
        WHEN UPPER(TRIM(category)) IN ('HEALTHCARE', 'HEALTH', 'MEDICAL') THEN 'HEALTHCARE'
        WHEN UPPER(TRIM(category)) IN ('INSURANCE', 'INS') THEN 'INSURANCE'
        WHEN UPPER(TRIM(category)) IN ('SALARY', 'INCOME', 'PAYROLL') THEN 'SALARY'
        ELSE 'OTHER'
    END as category_normalized,

    description,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('COMPLETED', 'SUCCESS', 'SUCCESSFUL') THEN 'COMPLETED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'PROCESSING', 'IN_PROGRESS') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('FAILED', 'DECLINED', 'REJECTED') THEN 'FAILED'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CANCELED', 'REVERSED') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END as status_normalized,

    UPPER(TRIM(channel)) as channel,

    -- Suspicious transaction flag (simple rules)
    CASE
        WHEN amount > 10000 THEN TRUE  -- Large amount
        WHEN HOUR(transaction_date) BETWEEN 0 AND 4 THEN TRUE  -- Late night
        WHEN amount < 0 THEN TRUE  -- Negative amount
        ELSE FALSE
    END as is_suspicious,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.transactions' as source_system,

    -- Partitions
    YEAR(transaction_date) as transaction_year,
    MONTH(transaction_date) as transaction_month

FROM bronze.transactions
WHERE transaction_id IS NOT NULL
  AND amount IS NOT NULL
  AND transaction_date IS NOT NULL;

-- Check results
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) as txn_count,
    SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) as suspicious_count,
    ROUND(SUM(amount), 2) as total_amount
FROM silver.transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year DESC, transaction_month DESC;

-- ============================================================================
-- 6. ACCOUNT_BALANCES - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.account_balances;

INSERT INTO TABLE silver.account_balances
SELECT
    balance_id,
    account_id,
    balance_date,
    current_balance,
    available_balance,
    UPPER(currency) as currency,
    COALESCE(overdraft_limit, 0) as overdraft_limit,
    COALESCE(credit_limit, 0) as credit_limit,

    -- Derived: Utilization rate
    CASE
        WHEN credit_limit > 0
        THEN ROUND((current_balance / credit_limit) * 100, 2)
        ELSE 0
    END as credit_utilization_pct,

    -- Is overdrawn flag
    CASE
        WHEN current_balance < 0 THEN TRUE
        ELSE FALSE
    END as is_overdrawn,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.account_balances' as source_system
FROM bronze.account_balances
WHERE balance_id IS NOT NULL;

SELECT COUNT(*) as silver_balances_count FROM silver.account_balances;

-- ============================================================================
-- 7. CARDS - Transform to Silver (with masking)
-- ============================================================================
TRUNCATE TABLE silver.cards;

INSERT INTO TABLE silver.cards
SELECT
    card_id,
    client_id,
    account_id,

    -- Masked card number (show only last 4 digits)
    CONCAT('****-****-****-', SUBSTR(card_number, -4)) as card_number_masked,

    UPPER(TRIM(card_type)) as card_type,
    UPPER(TRIM(card_level)) as card_level,
    issue_date,
    expiry_date,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('BLOCKED', 'FROZEN', 'SUSPENDED') THEN 'BLOCKED'
        WHEN UPPER(TRIM(status)) IN ('EXPIRED', 'INACTIVE') THEN 'EXPIRED'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CLOSED') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END as status_normalized,

    COALESCE(credit_limit, 0) as credit_limit,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND expiry_date >= CURRENT_DATE
        THEN TRUE
        ELSE FALSE
    END as is_active,

    -- Is expired flag
    CASE
        WHEN expiry_date < CURRENT_DATE THEN TRUE
        ELSE FALSE
    END as is_expired,

    -- Days until expiry
    DATEDIFF(expiry_date, CURRENT_DATE) as days_until_expiry,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.cards' as source_system
FROM bronze.cards
WHERE card_id IS NOT NULL;

SELECT COUNT(*) as silver_cards_count FROM silver.cards;

-- ============================================================================
-- 8. BRANCHES - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.branches;

INSERT INTO TABLE silver.branches
SELECT
    branch_id,
    UPPER(TRIM(branch_code)) as branch_code,
    TRIM(branch_name) as branch_name,
    address,
    city,
    state,
    country,
    postal_code,
    phone,
    LOWER(TRIM(email)) as email,
    manager_name,
    open_date,

    -- Branch age in years
    ROUND(DATEDIFF(CURRENT_DATE, open_date) / 365.25, 1) as branch_age_years,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.branches' as source_system
FROM bronze.branches
WHERE branch_id IS NOT NULL;

SELECT COUNT(*) as silver_branches_count FROM silver.branches;

-- ============================================================================
-- 9. EMPLOYEES - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.employees;

INSERT INTO TABLE silver.employees
SELECT
    employee_id,
    branch_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) as full_name,
    LOWER(TRIM(email)) as email,
    phone,
    TRIM(position) as position,
    TRIM(department) as department,
    hire_date,

    -- Tenure calculation
    DATEDIFF(CURRENT_DATE, hire_date) as tenure_days,
    ROUND(DATEDIFF(CURRENT_DATE, hire_date) / 365.25, 1) as tenure_years,

    salary,
    manager_id,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.employees' as source_system
FROM bronze.employees
WHERE employee_id IS NOT NULL;

SELECT COUNT(*) as silver_employees_count FROM silver.employees;

-- ============================================================================
-- 10. LOANS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.loans;

INSERT INTO TABLE silver.loans
SELECT
    loan_id,
    contract_id,
    UPPER(TRIM(loan_type)) as loan_type,
    loan_amount,
    UPPER(currency) as currency,
    interest_rate,
    term_months,
    monthly_payment,
    start_date,
    maturity_date,
    outstanding_balance,
    next_payment_date,
    next_payment_amount,
    payment_day,

    -- Delinquency status normalization
    CASE
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%CURRENT%' THEN 'CURRENT'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%30%' THEN 'DELINQUENT_30'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%60%' THEN 'DELINQUENT_60'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%90%' THEN 'DELINQUENT_90'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%120%' THEN 'DELINQUENT_120_PLUS'
        ELSE 'UNKNOWN'
    END as delinquency_status_normalized,

    COALESCE(days_past_due, 0) as days_past_due,
    collateral_type,
    collateral_value,
    loan_to_value_ratio,

    -- Derived metrics
    ROUND((outstanding_balance / loan_amount) * 100, 2) as outstanding_pct,
    ROUND(loan_amount - outstanding_balance, 2) as principal_paid,
    DATEDIFF(maturity_date, CURRENT_DATE) as days_to_maturity,

    -- Is delinquent flag
    CASE
        WHEN days_past_due > 30 THEN TRUE
        ELSE FALSE
    END as is_delinquent,

    -- Risk score (simplified)
    CASE
        WHEN days_past_due = 0 THEN 'LOW'
        WHEN days_past_due <= 30 THEN 'MEDIUM'
        WHEN days_past_due <= 90 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END as loan_risk_category,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.loans' as source_system
FROM bronze.loans
WHERE loan_id IS NOT NULL;

SELECT COUNT(*) as silver_loans_count FROM silver.loans;

-- ============================================================================
-- 11. CREDIT_APPLICATIONS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.credit_applications;

INSERT INTO TABLE silver.credit_applications
SELECT
    application_id,
    client_id,
    application_date,
    UPPER(TRIM(product_type)) as product_type,
    requested_amount,
    requested_term_months,
    TRIM(purpose) as purpose,
    TRIM(employment_status) as employment_status,
    annual_income,
    COALESCE(existing_debt, 0) as existing_debt,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('APPROVED', 'ACCEPTED') THEN 'APPROVED'
        WHEN UPPER(TRIM(status)) IN ('REJECTED', 'DECLINED', 'DENIED') THEN 'REJECTED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'UNDER_REVIEW', 'IN_REVIEW') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('WITHDRAWN', 'CANCELLED') THEN 'WITHDRAWN'
        ELSE 'UNKNOWN'
    END as status_normalized,

    decision_date,
    COALESCE(approved_amount, 0) as approved_amount,
    approved_term_months,
    interest_rate_proposed,
    rejection_reason,

    -- Derived metrics
    DATEDIFF(COALESCE(decision_date, CURRENT_DATE), application_date) as processing_days,

    CASE
        WHEN approved_amount > 0
        THEN ROUND((approved_amount / requested_amount) * 100, 2)
        ELSE 0
    END as approval_pct,

    CASE
        WHEN annual_income > 0
        THEN ROUND((requested_amount / annual_income) * 100, 2)
        ELSE 0
    END as debt_to_income_ratio,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.credit_applications' as source_system
FROM bronze.credit_applications
WHERE application_id IS NOT NULL;

SELECT COUNT(*) as silver_credit_applications_count FROM silver.credit_applications;

-- ============================================================================
-- 12. CLIENT_PRODUCTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.client_products;

INSERT INTO TABLE silver.client_products
SELECT
    client_id,
    product_id,
    start_date,
    end_date,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE' AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) = 'INACTIVE' OR end_date < CURRENT_DATE
        THEN 'INACTIVE'
        ELSE 'UNKNOWN'
    END as status_normalized,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END as is_active,

    -- Product holding duration
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) as holding_days,

    -- Technical fields
    CURRENT_TIMESTAMP as process_timestamp,
    'bronze.client_products' as source_system
FROM bronze.client_products
WHERE client_id IS NOT NULL
  AND product_id IS NOT NULL;

SELECT COUNT(*) as silver_client_products_count FROM silver.client_products;

-- ============================================================================
-- FINAL SUMMARY - Silver Layer Load Statistics
-- ============================================================================
SELECT
    'clients' as table_name,
    COUNT(*) as record_count,
    ROUND(AVG(dq_score), 3) as avg_dq_score
FROM silver.clients
UNION ALL
SELECT 'products', COUNT(*), NULL FROM silver.products
UNION ALL
SELECT 'contracts', COUNT(*), NULL FROM silver.contracts
UNION ALL
SELECT 'accounts', COUNT(*), NULL FROM silver.accounts
UNION ALL
SELECT 'transactions', COUNT(*), NULL FROM silver.transactions
UNION ALL
SELECT 'account_balances', COUNT(*), NULL FROM silver.account_balances
UNION ALL
SELECT 'cards', COUNT(*), NULL FROM silver.cards
UNION ALL
SELECT 'branches', COUNT(*), NULL FROM silver.branches
UNION ALL
SELECT 'employees', COUNT(*), NULL FROM silver.employees
UNION ALL
SELECT 'loans', COUNT(*), NULL FROM silver.loans
UNION ALL
SELECT 'credit_applications', COUNT(*), NULL FROM silver.credit_applications
UNION ALL
SELECT 'client_products', COUNT(*), NULL FROM silver.client_products
ORDER BY table_name;

-- Data Quality Report
SELECT
    'Silver Layer Data Quality Summary' as report_title;

SELECT
    CASE
        WHEN dq_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN dq_score >= 0.8 THEN 'Good (0.8-0.9)'
        WHEN dq_score >= 0.7 THEN 'Fair (0.7-0.8)'
        ELSE 'Poor (<0.7)'
    END as quality_category,
    COUNT(*) as client_count,
    ROUND(AVG(dq_score), 3) as avg_score
FROM silver.clients
GROUP BY
    CASE
        WHEN dq_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN dq_score >= 0.8 THEN 'Good (0.8-0.9)'
        WHEN dq_score >= 0.7 THEN 'Fair (0.7-0.8)'
        ELSE 'Poor (<0.7)'
    END
ORDER BY avg_score DESC;

-- ============================================================================
-- END OF BRONZE TO SILVER ETL
-- ============================================================================
