-- ============================================================================
-- Script: 02_Load_Bronze_to_Silver.sql
-- Description: Transform and load data from Bronze to Silver layer
-- Author: Data Engineering Team
-- Date: 2025-11-06
-- ============================================================================

-- This script transforms raw data from the Bronze layer into cleaned,
-- validated, and enriched data in the Silver layer for the banking data warehouse.

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
    -- Primary Key
    client_id,

    -- Original fields
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,

    -- Derived: Full name
    CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,

    -- Email normalization
    LOWER(TRIM(email)) AS email,

    -- Email domain extraction
    CASE
        WHEN email IS NOT NULL AND email LIKE '%@%'
        THEN SUBSTRING(LOWER(email), LOCATE('@', LOWER(email)) + 1)
        ELSE NULL
    END AS email_domain,

    -- Phone (original)
    phone,

    -- Phone normalized (remove special characters)
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone_normalized,

    birth_date,

    -- Age calculation
    CAST(FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) AS INT) AS age,

    registration_date,

    -- Address fields
    TRIM(address) AS address,
    TRIM(city) AS city,
    UPPER(TRIM(country)) AS country,

    -- Risk category
    UPPER(TRIM(risk_category)) AS risk_category,

    -- Credit score
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
    END AS credit_score_category,

    -- Employment and income
    UPPER(TRIM(employment_status)) AS employment_status,
    annual_income,

    -- Income category
    CASE
        WHEN annual_income < 30000 THEN 'LOW'
        WHEN annual_income >= 30000 AND annual_income < 60000 THEN 'LOWER_MIDDLE'
        WHEN annual_income >= 60000 AND annual_income < 100000 THEN 'MIDDLE'
        WHEN annual_income >= 100000 AND annual_income < 150000 THEN 'UPPER_MIDDLE'
        WHEN annual_income >= 150000 THEN 'HIGH'
        ELSE 'UNKNOWN'
    END AS income_category,

    -- Is active (client has recent activity)
    CASE
        WHEN registration_date IS NOT NULL
             AND DATEDIFF(CURRENT_DATE, registration_date) <= 365
        THEN TRUE
        ELSE FALSE
    END AS is_active,

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
    ) AS dq_score,

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
    ) AS dq_issues,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.clients' AS source_system,

    -- Partition column
    YEAR(registration_date) AS registration_year

FROM bronze.clients
WHERE client_id IS NOT NULL;

-- Check results
SELECT
    COUNT(*) AS total_records,
    ROUND(AVG(dq_score), 3) AS avg_dq_score,
    MIN(dq_score) AS min_dq_score,
    COUNT(CASE WHEN dq_score < 0.8 THEN 1 END) AS low_quality_count
FROM silver.clients;

-- ============================================================================
-- 2. PRODUCTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.products;

INSERT INTO TABLE silver.products
SELECT
    -- Primary Key
    product_id,

    -- Product information
    TRIM(product_name) AS product_name,
    UPPER(TRIM(product_type)) AS product_type,

    -- Product category grouping
    CASE
        WHEN UPPER(TRIM(product_type)) IN ('LOAN', 'MORTGAGE', 'CREDIT_CARD', 'CREDIT_LINE') THEN 'CREDIT'
        WHEN UPPER(TRIM(product_type)) IN ('CHECKING', 'SAVINGS', 'DEPOSIT') THEN 'DEPOSIT'
        WHEN UPPER(TRIM(product_type)) IN ('INVESTMENT', 'MUTUAL_FUND', 'RETIREMENT') THEN 'INVESTMENT'
        ELSE 'OTHER'
    END AS product_category,

    UPPER(TRIM(currency)) AS currency,
    COALESCE(active, FALSE) AS active,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.products' AS source_system

FROM bronze.products
WHERE product_id IS NOT NULL;

SELECT COUNT(*) AS silver_products_count FROM silver.products;

-- ============================================================================
-- 3. CONTRACTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.contracts;

INSERT INTO TABLE silver.contracts
PARTITION (start_year)
SELECT
    -- Primary Key
    contract_id,

    -- Foreign Keys
    client_id,
    product_id,

    -- Contract information
    TRIM(contract_number) AS contract_number,
    start_date,
    end_date,

    -- Derived: Contract duration in days
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) AS contract_duration_days,

    contract_amount,
    ROUND(interest_rate, 4) AS interest_rate,

    -- Original status
    status,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED', 'OPEN') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('CLOSED', 'TERMINATED', 'COMPLETED') THEN 'CLOSED'
        WHEN UPPER(TRIM(status)) IN ('SUSPENDED', 'FROZEN') THEN 'SUSPENDED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'PENDING_ACTIVATION') THEN 'PENDING'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    monthly_payment,
    created_date,

    -- Is expired flag
    CASE
        WHEN end_date IS NOT NULL AND end_date < CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_expired,

    -- Days to expiry
    CASE
        WHEN end_date IS NOT NULL THEN DATEDIFF(end_date, CURRENT_DATE)
        ELSE NULL
    END AS days_to_expiry,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.contracts' AS source_system,

    -- Partition
    YEAR(start_date) AS start_year

FROM bronze.contracts
WHERE contract_id IS NOT NULL;

SELECT COUNT(*) AS silver_contracts_count FROM silver.contracts;

-- ============================================================================
-- 4. ACCOUNTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.accounts;

INSERT INTO TABLE silver.accounts
PARTITION (open_year)
SELECT
    -- Primary Key
    account_id,

    -- Foreign Keys
    client_id,
    contract_id,

    TRIM(account_number) AS account_number,

    -- Original account_type
    account_type,

    -- Account type normalization
    CASE
        WHEN UPPER(TRIM(account_type)) IN ('CHECKING', 'CURRENT', 'CHK') THEN 'CHECKING'
        WHEN UPPER(TRIM(account_type)) IN ('SAVINGS', 'SAV', 'DEPOSIT') THEN 'SAVINGS'
        WHEN UPPER(TRIM(account_type)) IN ('CREDIT', 'LOAN', 'LENDING') THEN 'LOAN'
        WHEN UPPER(TRIM(account_type)) IN ('INVESTMENT', 'INV') THEN 'INVESTMENT'
        ELSE 'OTHER'
    END AS account_type_normalized,

    UPPER(TRIM(currency)) AS currency,
    open_date,
    close_date,

    -- Account age in days
    DATEDIFF(COALESCE(close_date, CURRENT_DATE), open_date) AS account_age_days,

    -- Original status
    status,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'OPEN') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('CLOSED', 'TERMINATED') THEN 'CLOSED'
        WHEN UPPER(TRIM(status)) IN ('SUSPENDED', 'FROZEN') THEN 'SUSPENDED'
        WHEN UPPER(TRIM(status)) IN ('DORMANT', 'INACTIVE') THEN 'DORMANT'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    branch_code,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND (close_date IS NULL OR close_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.accounts' AS source_system,

    -- Partition
    YEAR(open_date) AS open_year

FROM bronze.accounts
WHERE account_id IS NOT NULL;

SELECT COUNT(*) AS silver_accounts_count FROM silver.accounts;

-- ============================================================================
-- 5. CLIENT_PRODUCTS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.client_products;

INSERT INTO TABLE silver.client_products
SELECT
    -- Primary Key
    relationship_id,

    -- Foreign Keys
    client_id,
    product_id,

    -- Original relationship_type
    relationship_type,

    -- Relationship type normalization
    UPPER(TRIM(relationship_type)) AS relationship_type_normalized,

    start_date,
    end_date,

    -- Relationship duration
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) AS relationship_duration_days,

    -- Original status
    status,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.client_products' AS source_system

FROM bronze.client_products
WHERE relationship_id IS NOT NULL
  AND client_id IS NOT NULL
  AND product_id IS NOT NULL;

SELECT COUNT(*) AS silver_client_products_count FROM silver.client_products;

-- ============================================================================
-- 6. TRANSACTIONS - Transform to Silver (with partitioning)
-- ============================================================================
TRUNCATE TABLE silver.transactions;

INSERT INTO TABLE silver.transactions
PARTITION (transaction_year, transaction_month)
SELECT
    -- Primary Key (BIGINT in silver)
    CAST(transaction_id AS BIGINT) AS transaction_id,
    TRIM(transaction_uuid) AS transaction_uuid,

    -- Account references
    from_account_id,
    to_account_id,
    TRIM(from_account_number) AS from_account_number,
    TRIM(to_account_number) AS to_account_number,

    -- Original transaction_type
    transaction_type,

    -- Transaction type normalization
    CASE
        WHEN UPPER(TRIM(transaction_type)) IN ('DEPOSIT', 'DEP', 'CREDIT') THEN 'DEPOSIT'
        WHEN UPPER(TRIM(transaction_type)) IN ('WITHDRAWAL', 'WD', 'DEBIT') THEN 'WITHDRAWAL'
        WHEN UPPER(TRIM(transaction_type)) IN ('TRANSFER', 'TRF', 'XFER') THEN 'TRANSFER'
        WHEN UPPER(TRIM(transaction_type)) IN ('PAYMENT', 'PAY', 'PMT') THEN 'PAYMENT'
        WHEN UPPER(TRIM(transaction_type)) IN ('FEE', 'CHARGE') THEN 'FEE'
        ELSE 'OTHER'
    END AS transaction_type_normalized,

    amount,
    ABS(amount) AS amount_abs,
    UPPER(TRIM(currency)) AS currency,

    transaction_date,

    -- Date components
    TO_DATE(transaction_date) AS transaction_date_only,
    HOUR(transaction_date) AS transaction_hour,

    TRIM(description) AS description,

    -- Original status
    status,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('COMPLETED', 'SUCCESS', 'SUCCESSFUL') THEN 'COMPLETED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'PROCESSING', 'IN_PROGRESS') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('FAILED', 'DECLINED', 'REJECTED') THEN 'FAILED'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CANCELED', 'REVERSED') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    -- Original category
    category,

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
    END AS category_normalized,

    TRIM(merchant_name) AS merchant_name,

    -- Is internal transfer flag
    CASE
        WHEN from_account_id IS NOT NULL AND to_account_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_internal_transfer,

    -- Suspicious transaction flag (simple rules)
    CASE
        WHEN ABS(amount) > 10000 THEN TRUE
        WHEN HOUR(transaction_date) BETWEEN 0 AND 4 THEN TRUE
        ELSE FALSE
    END AS is_suspicious,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.transactions' AS source_system,

    -- Partitions
    YEAR(transaction_date) AS transaction_year,
    MONTH(transaction_date) AS transaction_month

FROM bronze.transactions
WHERE transaction_id IS NOT NULL
  AND amount IS NOT NULL
  AND transaction_date IS NOT NULL;

-- Check results
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) AS txn_count,
    SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) AS suspicious_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM silver.transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year DESC, transaction_month DESC;

-- ============================================================================
-- 7. ACCOUNT_BALANCES - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.account_balances;

INSERT INTO TABLE silver.account_balances
SELECT
    -- Primary Key
    balance_id,

    -- Foreign Key
    account_id,

    -- Balance information
    current_balance,
    available_balance,

    -- Reserved amount (derived)
    current_balance - available_balance AS reserved_amount,

    UPPER(TRIM(currency)) AS currency,
    last_updated,
    COALESCE(credit_limit, 0) AS credit_limit,

    -- Credit utilization (derived)
    CASE
        WHEN credit_limit > 0
        THEN ROUND((current_balance / credit_limit) * 100, 2)
        ELSE 0
    END AS credit_utilization,

    -- Balance category
    CASE
        WHEN current_balance < 0 THEN 'NEGATIVE'
        WHEN current_balance = 0 THEN 'ZERO'
        WHEN current_balance < 1000 THEN 'LOW'
        WHEN current_balance < 10000 THEN 'MEDIUM'
        WHEN current_balance < 100000 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS balance_category,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.account_balances' AS source_system

FROM bronze.account_balances
WHERE balance_id IS NOT NULL;

SELECT COUNT(*) AS silver_balances_count FROM silver.account_balances;

-- ============================================================================
-- 8. CARDS - Transform to Silver (with masking)
-- ============================================================================
TRUNCATE TABLE silver.cards;

INSERT INTO TABLE silver.cards
SELECT
    -- Primary Key
    card_id,

    -- Foreign Keys
    client_id,
    account_id,

    -- Masked card number (show only last 4 digits)
    CONCAT('****-****-****-', SUBSTR(card_number, -4)) AS card_number_masked,

    TRIM(card_holder_name) AS card_holder_name,
    expiry_date,

    -- Original card_type
    card_type,

    -- Card type normalization
    UPPER(TRIM(card_type)) AS card_type_normalized,

    UPPER(TRIM(card_level)) AS card_level,

    -- Original status
    status,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('BLOCKED', 'FROZEN', 'SUSPENDED') THEN 'BLOCKED'
        WHEN UPPER(TRIM(status)) IN ('EXPIRED', 'INACTIVE') THEN 'EXPIRED'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CLOSED') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    issue_date,

    -- Card age
    DATEDIFF(CURRENT_DATE, issue_date) AS card_age_days,

    -- Is expired flag
    CASE
        WHEN expiry_date < CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_expired,

    -- Days to expiry
    DATEDIFF(expiry_date, CURRENT_DATE) AS days_to_expiry,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.cards' AS source_system

FROM bronze.cards
WHERE card_id IS NOT NULL;

SELECT COUNT(*) AS silver_cards_count FROM silver.cards;

-- ============================================================================
-- 9. BRANCHES - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.branches;

INSERT INTO TABLE silver.branches
SELECT
    -- Primary Key
    branch_id,

    -- Branch information
    UPPER(TRIM(branch_code)) AS branch_code,
    TRIM(branch_name) AS branch_name,

    -- Location
    TRIM(address) AS address,
    TRIM(city) AS city,
    TRIM(state) AS state,

    -- State code (derived from state)
    CASE
        WHEN UPPER(TRIM(state)) IN ('CALIFORNIA', 'CA') THEN 'CA'
        WHEN UPPER(TRIM(state)) IN ('TEXAS', 'TX') THEN 'TX'
        WHEN UPPER(TRIM(state)) IN ('NEW YORK', 'NY') THEN 'NY'
        WHEN UPPER(TRIM(state)) IN ('FLORIDA', 'FL') THEN 'FL'
        WHEN UPPER(TRIM(state)) IN ('ILLINOIS', 'IL') THEN 'IL'
        ELSE UPPER(TRIM(state))
    END AS state_code,

    TRIM(zip_code) AS zip_code,

    -- Contact (original)
    phone,

    -- Phone normalized
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone_normalized,

    TRIM(manager_name) AS manager_name,

    -- Dates
    opening_date,

    -- Branch age in days
    DATEDIFF(CURRENT_DATE, opening_date) AS branch_age_days,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.branches' AS source_system

FROM bronze.branches
WHERE branch_id IS NOT NULL;

SELECT COUNT(*) AS silver_branches_count FROM silver.branches;

-- ============================================================================
-- 10. EMPLOYEES - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.employees;

INSERT INTO TABLE silver.employees
SELECT
    -- Primary Key
    employee_id,

    -- Foreign Key
    branch_id,

    -- Personal information
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,

    -- Contact
    LOWER(TRIM(email)) AS email,

    -- Email domain extraction
    CASE
        WHEN email IS NOT NULL AND email LIKE '%@%'
        THEN SUBSTRING(LOWER(email), LOCATE('@', LOWER(email)) + 1)
        ELSE NULL
    END AS email_domain,

    -- Phone (original)
    phone,

    -- Phone normalized
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone_normalized,

    -- Original position
    position,

    -- Position normalization
    UPPER(TRIM(position)) AS position_normalized,

    -- Original department
    department,

    -- Department normalization
    UPPER(TRIM(department)) AS department_normalized,

    -- Employment
    hire_date,

    -- Tenure calculation
    DATEDIFF(CURRENT_DATE, hire_date) AS tenure_days,
    ROUND(DATEDIFF(CURRENT_DATE, hire_date) / 365.25, 2) AS tenure_years,

    salary,

    -- Salary category
    CASE
        WHEN salary < 40000 THEN 'LOW'
        WHEN salary >= 40000 AND salary < 70000 THEN 'MEDIUM'
        WHEN salary >= 70000 AND salary < 100000 THEN 'HIGH'
        WHEN salary >= 100000 THEN 'VERY_HIGH'
        ELSE 'UNKNOWN'
    END AS salary_category,

    -- Original status
    status,

    -- Status normalization
    UPPER(TRIM(status)) AS status_normalized,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE' THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.employees' AS source_system

FROM bronze.employees
WHERE employee_id IS NOT NULL;

SELECT COUNT(*) AS silver_employees_count FROM silver.employees;

-- ============================================================================
-- 11. LOANS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.loans;

INSERT INTO TABLE silver.loans
SELECT
    -- Primary Key
    loan_id,

    -- Foreign Key
    contract_id,

    -- Loan amounts
    loan_amount,
    outstanding_balance,

    -- Paid amount (derived)
    loan_amount - outstanding_balance AS paid_amount,

    -- Payment progress (derived)
    CASE
        WHEN loan_amount > 0
        THEN ROUND(((loan_amount - outstanding_balance) / loan_amount) * 100, 2)
        ELSE 0
    END AS payment_progress,

    ROUND(interest_rate, 4) AS interest_rate,

    -- Term information
    term_months,
    remaining_months,

    -- Elapsed months (derived)
    term_months - remaining_months AS elapsed_months,

    -- Payment information
    next_payment_date,
    next_payment_amount,

    -- Original delinquency_status
    delinquency_status,

    -- Delinquency status normalization
    CASE
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%CURRENT%' THEN 'CURRENT'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%30%' THEN 'DELINQUENT_30'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%60%' THEN 'DELINQUENT_60'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%90%' THEN 'DELINQUENT_90'
        WHEN UPPER(TRIM(delinquency_status)) LIKE '%120%' THEN 'DELINQUENT_120_PLUS'
        ELSE 'UNKNOWN'
    END AS delinquency_status_normalized,

    -- Is delinquent flag
    CASE
        WHEN UPPER(TRIM(delinquency_status)) IN ('CURRENT', 'NONE') THEN FALSE
        ELSE TRUE
    END AS is_delinquent,

    -- Collateral and LTV
    collateral_value,
    ROUND(loan_to_value_ratio, 4) AS loan_to_value_ratio,

    -- LTV risk category
    CASE
        WHEN loan_to_value_ratio <= 0.60 THEN 'LOW_RISK'
        WHEN loan_to_value_ratio <= 0.80 THEN 'MEDIUM_RISK'
        WHEN loan_to_value_ratio <= 0.95 THEN 'HIGH_RISK'
        ELSE 'VERY_HIGH_RISK'
    END AS ltv_category,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.loans' AS source_system

FROM bronze.loans
WHERE loan_id IS NOT NULL;

SELECT COUNT(*) AS silver_loans_count FROM silver.loans;

-- ============================================================================
-- 12. CREDIT_APPLICATIONS - Transform to Silver
-- ============================================================================
TRUNCATE TABLE silver.credit_applications;

INSERT INTO TABLE silver.credit_applications
PARTITION (application_year)
SELECT
    -- Primary Key
    application_id,

    -- Foreign Keys
    client_id,

    -- Application details
    application_date,
    requested_amount,
    COALESCE(approved_amount, 0) AS approved_amount,

    -- Approval rate (derived)
    CASE
        WHEN requested_amount > 0
        THEN ROUND((COALESCE(approved_amount, 0) / requested_amount) * 100, 2)
        ELSE 0
    END AS approval_rate,

    -- Original purpose
    purpose,

    -- Purpose normalization
    UPPER(TRIM(purpose)) AS purpose_normalized,

    -- Original status
    status,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('APPROVED', 'ACCEPTED') THEN 'APPROVED'
        WHEN UPPER(TRIM(status)) IN ('REJECTED', 'DECLINED', 'DENIED') THEN 'REJECTED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'UNDER_REVIEW', 'IN_REVIEW') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('WITHDRAWN', 'CANCELLED') THEN 'WITHDRAWN'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    decision_date,

    -- Processing days (derived)
    CASE
        WHEN decision_date IS NOT NULL AND application_date IS NOT NULL
        THEN DATEDIFF(decision_date, application_date)
        ELSE NULL
    END AS processing_days,

    ROUND(interest_rate_proposed, 4) AS interest_rate_proposed,

    TRIM(reason_for_rejection) AS reason_for_rejection,

    -- Rejection category (derived)
    CASE
        WHEN reason_for_rejection IS NULL THEN NULL
        WHEN UPPER(reason_for_rejection) LIKE '%CREDIT%SCORE%' THEN 'LOW_CREDIT_SCORE'
        WHEN UPPER(reason_for_rejection) LIKE '%INCOME%' THEN 'INSUFFICIENT_INCOME'
        WHEN UPPER(reason_for_rejection) LIKE '%DEBT%' THEN 'HIGH_DEBT_RATIO'
        WHEN UPPER(reason_for_rejection) LIKE '%EMPLOYMENT%' THEN 'EMPLOYMENT_ISSUES'
        ELSE 'OTHER'
    END AS rejection_category,

    officer_id,

    -- Is approved flag
    CASE
        WHEN UPPER(TRIM(status)) IN ('APPROVED', 'ACCEPTED') THEN TRUE
        ELSE FALSE
    END AS is_approved,

    -- Is rejected flag
    CASE
        WHEN UPPER(TRIM(status)) IN ('REJECTED', 'DECLINED', 'DENIED') THEN TRUE
        ELSE FALSE
    END AS is_rejected,

    -- Metadata
    load_timestamp,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.credit_applications' AS source_system,

    -- Partition
    YEAR(application_date) AS application_year

FROM bronze.credit_applications
WHERE application_id IS NOT NULL;

SELECT COUNT(*) AS silver_credit_applications_count FROM silver.credit_applications;

-- ============================================================================
-- FINAL SUMMARY - Silver Layer Load Statistics
-- ============================================================================
SELECT
    'clients' AS table_name,
    COUNT(*) AS record_count,
    ROUND(AVG(dq_score), 3) AS avg_dq_score
FROM silver.clients
UNION ALL
SELECT 'products', COUNT(*), NULL FROM silver.products
UNION ALL
SELECT 'contracts', COUNT(*), NULL FROM silver.contracts
UNION ALL
SELECT 'accounts', COUNT(*), NULL FROM silver.accounts
UNION ALL
SELECT 'client_products', COUNT(*), NULL FROM silver.client_products
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
ORDER BY table_name;

-- Data Quality Report for Clients
SELECT
    'Silver Layer Data Quality Summary' AS report_title;

SELECT
    CASE
        WHEN dq_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN dq_score >= 0.8 THEN 'Good (0.8-0.9)'
        WHEN dq_score >= 0.7 THEN 'Fair (0.7-0.8)'
        ELSE 'Poor (<0.7)'
    END AS quality_category,
    COUNT(*) AS client_count,
    ROUND(AVG(dq_score), 3) AS avg_score
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
