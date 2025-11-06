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

    -- Phone normalization (remove special characters)
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,

    birth_date,

    -- Age calculation
    CAST(FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) AS INT) AS age,

    -- Age group
    CASE
        WHEN DATEDIFF(CURRENT_DATE, birth_date) / 365.25 < 25 THEN '18-24'
        WHEN DATEDIFF(CURRENT_DATE, birth_date) / 365.25 < 35 THEN '25-34'
        WHEN DATEDIFF(CURRENT_DATE, birth_date) / 365.25 < 45 THEN '35-44'
        WHEN DATEDIFF(CURRENT_DATE, birth_date) / 365.25 < 55 THEN '45-54'
        WHEN DATEDIFF(CURRENT_DATE, birth_date) / 365.25 < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,

    registration_date,

    -- Customer tenure
    DATEDIFF(CURRENT_DATE, registration_date) AS customer_tenure_days,

    -- Address fields
    TRIM(address) AS address,
    TRIM(city) AS city,
    UPPER(TRIM(country)) AS country,

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

    -- Risk category (derived from credit score and income)
    UPPER(TRIM(risk_category)) AS risk_category,

    -- Enhanced risk scoring
    CASE
        WHEN credit_score >= 740 AND annual_income >= 75000 THEN 'LOW_RISK'
        WHEN credit_score >= 670 AND annual_income >= 50000 THEN 'MEDIUM_RISK'
        WHEN credit_score >= 580 THEN 'HIGH_RISK'
        ELSE 'VERY_HIGH_RISK'
    END AS calculated_risk_category,

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
    source_file,
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

    -- Status based on active flag
    CASE
        WHEN COALESCE(active, FALSE) = TRUE THEN 'ACTIVE'
        ELSE 'INACTIVE'
    END AS status,

    -- Metadata
    load_timestamp,
    source_file,
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
    created_date,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED', 'OPEN') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('CLOSED', 'TERMINATED', 'COMPLETED') THEN 'CLOSED'
        WHEN UPPER(TRIM(status)) IN ('SUSPENDED', 'FROZEN') THEN 'SUSPENDED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'PENDING_ACTIVATION') THEN 'PENDING'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    contract_amount,
    ROUND(interest_rate, 4) AS interest_rate,

    -- Interest rate category
    CASE
        WHEN interest_rate < 3 THEN 'LOW'
        WHEN interest_rate >= 3 AND interest_rate < 6 THEN 'MEDIUM'
        WHEN interest_rate >= 6 AND interest_rate < 10 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS interest_rate_category,

    monthly_payment,

    -- Derived: Contract duration in days
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) AS contract_duration_days,

    -- Days since created
    DATEDIFF(CURRENT_DATE, created_date) AS days_since_created,

    -- Estimated payment count
    CASE
        WHEN monthly_payment IS NOT NULL AND monthly_payment > 0
        THEN ROUND(contract_amount / monthly_payment, 2)
        ELSE NULL
    END AS estimated_payments_count,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED', 'OPEN')
             AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Metadata
    load_timestamp,
    source_file,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.contracts' AS source_system

FROM bronze.contracts
WHERE contract_id IS NOT NULL;

SELECT COUNT(*) AS silver_contracts_count FROM silver.contracts;

-- ============================================================================
-- 4. ACCOUNTS - Transform to Silver
-- ============================================================================

INSERT OVERWRITE TABLE silver.accounts
PARTITION (open_year)
SELECT
    -- Primary Key
    account_id,

    -- Foreign Keys
    client_id,
    contract_id,

    TRIM(account_number) AS account_number,

    -- Account type normalization
    CASE
        WHEN UPPER(TRIM(account_type)) IN ('CHECKING', 'CURRENT', 'CHK') THEN 'CHECKING'
        WHEN UPPER(TRIM(account_type)) IN ('SAVINGS', 'SAV', 'DEPOSIT') THEN 'SAVINGS'
        WHEN UPPER(TRIM(account_type)) IN ('CREDIT', 'LOAN', 'LENDING') THEN 'LOAN'
        WHEN UPPER(TRIM(account_type)) IN ('INVESTMENT', 'INV') THEN 'INVESTMENT'
        ELSE 'OTHER'
    END AS account_type_normalized,

    -- Account category
    CASE
        WHEN UPPER(TRIM(account_type)) IN ('CHECKING', 'CURRENT') THEN 'TRANSACTIONAL'
        WHEN UPPER(TRIM(account_type)) IN ('SAVINGS', 'DEPOSIT') THEN 'SAVINGS'
        WHEN UPPER(TRIM(account_type)) IN ('INVESTMENT', 'BROKERAGE') THEN 'INVESTMENT'
        WHEN UPPER(TRIM(account_type)) IN ('LOAN', 'CREDIT') THEN 'CREDIT'
        ELSE 'OTHER'
    END AS account_category,

    UPPER(TRIM(currency)) AS currency,
    open_date,
    close_date,

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

    -- Is open flag
    CASE
        WHEN close_date IS NULL OR close_date > CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_open,

    -- Account age in days
    DATEDIFF(COALESCE(close_date, CURRENT_DATE), open_date) AS account_age_days,

    -- Metadata
    load_timestamp,
    source_file,
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

    -- Relationship information
    UPPER(TRIM(relationship_type)) AS relationship_type,
    start_date,
    end_date,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE' AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) = 'INACTIVE' OR end_date < CURRENT_DATE
        THEN 'INACTIVE'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND (end_date IS NULL OR end_date >= CURRENT_DATE)
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Product holding duration
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) AS holding_days,

    -- Relationship duration
    DATEDIFF(COALESCE(end_date, CURRENT_DATE), start_date) AS relationship_duration_days,

    -- Metadata
    load_timestamp,
    source_file,
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

INSERT OVERWRITE TABLE silver.transactions
PARTITION (transaction_year, transaction_month)
SELECT
    -- Primary Key
    transaction_id,
    TRIM(transaction_uuid) AS transaction_uuid,

    -- Account references
    from_account_id,
    to_account_id,
    TRIM(from_account_number) AS from_account_number,
    TRIM(to_account_number) AS to_account_number,

    transaction_date,

    -- Date components
    TO_DATE(transaction_date) AS transaction_date_only,
    HOUR(transaction_date) AS transaction_hour,
    DAYOFWEEK(transaction_date) AS day_of_week,

    -- Is weekend flag
    CASE
        WHEN DAYOFWEEK(transaction_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    -- Time of day category
    CASE
        WHEN HOUR(transaction_date) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(transaction_date) BETWEEN 12 AND 17 THEN 'AFTERNOON'
        WHEN HOUR(transaction_date) BETWEEN 18 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS time_of_day,

    amount,
    ABS(amount) AS absolute_amount,
    UPPER(TRIM(currency)) AS currency,

    -- Amount category
    CASE
        WHEN ABS(amount) < 100 THEN 'SMALL'
        WHEN ABS(amount) < 1000 THEN 'MEDIUM'
        WHEN ABS(amount) < 10000 THEN 'LARGE'
        ELSE 'VERY_LARGE'
    END AS amount_category,

    -- Transaction type normalization
    CASE
        WHEN UPPER(TRIM(transaction_type)) IN ('DEPOSIT', 'DEP', 'CREDIT') THEN 'DEPOSIT'
        WHEN UPPER(TRIM(transaction_type)) IN ('WITHDRAWAL', 'WD', 'DEBIT') THEN 'WITHDRAWAL'
        WHEN UPPER(TRIM(transaction_type)) IN ('TRANSFER', 'TRF', 'XFER') THEN 'TRANSFER'
        WHEN UPPER(TRIM(transaction_type)) IN ('PAYMENT', 'PAY', 'PMT') THEN 'PAYMENT'
        WHEN UPPER(TRIM(transaction_type)) IN ('FEE', 'CHARGE') THEN 'FEE'
        ELSE 'OTHER'
    END AS transaction_type_normalized,

    -- Transaction direction
    CASE
        WHEN UPPER(TRIM(transaction_type)) IN ('DEPOSIT', 'CREDIT', 'REFUND') THEN 'INBOUND'
        WHEN UPPER(TRIM(transaction_type)) IN ('WITHDRAWAL', 'DEBIT', 'PAYMENT', 'PURCHASE') THEN 'OUTBOUND'
        WHEN UPPER(TRIM(transaction_type)) IN ('TRANSFER', 'INTERNAL_TRANSFER') THEN 'TRANSFER'
        ELSE 'OTHER'
    END AS transaction_direction,

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

    TRIM(description) AS description,
    TRIM(merchant_name) AS merchant_name,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('COMPLETED', 'SUCCESS', 'SUCCESSFUL') THEN 'COMPLETED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'PROCESSING', 'IN_PROGRESS') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('FAILED', 'DECLINED', 'REJECTED') THEN 'FAILED'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CANCELED', 'REVERSED') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    -- Is completed flag
    CASE
        WHEN UPPER(TRIM(status)) IN ('COMPLETED', 'SUCCESS', 'SUCCESSFUL') THEN TRUE
        ELSE FALSE
    END AS is_completed,

    -- Suspicious transaction flag (simple rules)
    CASE
        WHEN amount > 10000 THEN TRUE  -- Large amount
        WHEN HOUR(transaction_date) BETWEEN 0 AND 4 THEN TRUE  -- Late night
        WHEN amount < 0 THEN TRUE  -- Negative amount
        ELSE FALSE
    END AS is_suspicious,

    -- Metadata
    load_timestamp,
    source_file,
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
    UPPER(TRIM(currency)) AS currency,
    COALESCE(credit_limit, 0) AS credit_limit,

    -- Held amount
    current_balance - available_balance AS held_amount,

    -- Derived: Utilization rate
    CASE
        WHEN credit_limit > 0
        THEN ROUND((credit_limit - available_balance) / credit_limit * 100, 2)
        ELSE 0
    END AS credit_utilization_pct,

    -- Balance category
    CASE
        WHEN current_balance < 0 THEN 'NEGATIVE'
        WHEN current_balance = 0 THEN 'ZERO'
        WHEN current_balance < 1000 THEN 'LOW'
        WHEN current_balance < 10000 THEN 'MEDIUM'
        WHEN current_balance < 100000 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS balance_category,

    -- Liquidity status
    CASE
        WHEN available_balance < current_balance * 0.1 THEN 'CRITICAL'
        WHEN available_balance < current_balance * 0.25 THEN 'LOW'
        WHEN available_balance < current_balance * 0.5 THEN 'MEDIUM'
        ELSE 'HEALTHY'
    END AS liquidity_status,

    -- Is overdrawn flag
    CASE
        WHEN current_balance < 0 THEN TRUE
        ELSE FALSE
    END AS is_overdrawn,

    -- Dates
    last_updated,
    DATEDIFF(CURRENT_DATE, last_updated) AS days_since_update,

    -- Metadata
    load_timestamp,
    source_file,
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

    UPPER(TRIM(card_type)) AS card_type,
    UPPER(TRIM(card_level)) AS card_level,

    -- Card tier
    CASE
        WHEN UPPER(TRIM(card_level)) IN ('PLATINUM', 'PREMIUM', 'BLACK') THEN 'PREMIUM'
        WHEN UPPER(TRIM(card_level)) IN ('GOLD') THEN 'GOLD'
        WHEN UPPER(TRIM(card_level)) IN ('SILVER', 'STANDARD') THEN 'STANDARD'
        ELSE 'BASIC'
    END AS card_tier,

    issue_date,
    expiry_date,

    -- Card age
    DATEDIFF(CURRENT_DATE, issue_date) AS card_age_days,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'ACTIVATED') THEN 'ACTIVE'
        WHEN UPPER(TRIM(status)) IN ('BLOCKED', 'FROZEN', 'SUSPENDED') THEN 'BLOCKED'
        WHEN UPPER(TRIM(status)) IN ('EXPIRED', 'INACTIVE') THEN 'EXPIRED'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CLOSED') THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'
             AND expiry_date >= CURRENT_DATE
        THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Is expired flag
    CASE
        WHEN expiry_date < CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_expired,

    -- Expiry status
    CASE
        WHEN expiry_date < CURRENT_DATE THEN 'EXPIRED'
        WHEN expiry_date < DATE_ADD(CURRENT_DATE, 30) THEN 'EXPIRING_SOON'
        ELSE 'VALID'
    END AS expiry_status,

    -- Days until expiry
    DATEDIFF(expiry_date, CURRENT_DATE) AS days_until_expiry,

    -- Metadata
    load_timestamp,
    source_file,
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
    TRIM(zip_code) AS zip_code,

    -- Contact
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,
    TRIM(manager_name) AS manager_name,

    -- Dates
    opening_date,
    DATEDIFF(CURRENT_DATE, opening_date) AS days_since_opening,

    -- Branch age in years
    ROUND(DATEDIFF(CURRENT_DATE, opening_date) / 365.25, 1) AS branch_age_years,

    -- Branch maturity category
    CASE
        WHEN DATEDIFF(CURRENT_DATE, opening_date) < 365 THEN 'NEW'
        WHEN DATEDIFF(CURRENT_DATE, opening_date) < 1825 THEN 'ESTABLISHED'
        ELSE 'VETERAN'
    END AS branch_maturity,

    -- Metadata
    load_timestamp,
    source_file,
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
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,

    -- Position and department
    TRIM(position) AS position,
    UPPER(TRIM(department)) AS department,

    -- Employment
    hire_date,

    -- Tenure calculation
    DATEDIFF(CURRENT_DATE, hire_date) AS tenure_days,
    ROUND(DATEDIFF(CURRENT_DATE, hire_date) / 365.25, 1) AS tenure_years,
    FLOOR(DATEDIFF(CURRENT_DATE, hire_date) / 365.25) AS years_of_service,

    -- Tenure category
    CASE
        WHEN DATEDIFF(CURRENT_DATE, hire_date) < 365 THEN 'LESS_THAN_1_YEAR'
        WHEN DATEDIFF(CURRENT_DATE, hire_date) < 1095 THEN '1_3_YEARS'
        WHEN DATEDIFF(CURRENT_DATE, hire_date) < 1825 THEN '3_5_YEARS'
        WHEN DATEDIFF(CURRENT_DATE, hire_date) < 3650 THEN '5_10_YEARS'
        ELSE '10_PLUS_YEARS'
    END AS tenure_category,

    salary,

    -- Compensation band
    CASE
        WHEN salary < 40000 THEN 'ENTRY_LEVEL'
        WHEN salary < 70000 THEN 'MID_LEVEL'
        WHEN salary < 100000 THEN 'SENIOR_LEVEL'
        ELSE 'EXECUTIVE_LEVEL'
    END AS compensation_band,

    -- Status
    UPPER(TRIM(status)) AS status,

    -- Is active flag
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE' THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Metadata
    load_timestamp,
    source_file,
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

    -- Amount paid
    loan_amount - outstanding_balance AS amount_paid,

    -- Outstanding balance percentage
    CASE
        WHEN loan_amount > 0
        THEN ROUND((outstanding_balance / loan_amount) * 100, 2)
        ELSE 0
    END AS outstanding_balance_pct,

    -- Amount paid percentage
    CASE
        WHEN loan_amount > 0
        THEN ROUND(((loan_amount - outstanding_balance) / loan_amount) * 100, 2)
        ELSE 0
    END AS amount_paid_pct,

    ROUND(interest_rate, 4) AS interest_rate,

    -- Interest rate category
    CASE
        WHEN interest_rate < 3 THEN 'LOW'
        WHEN interest_rate >= 3 AND interest_rate < 6 THEN 'MEDIUM'
        WHEN interest_rate >= 6 AND interest_rate < 10 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS interest_rate_category,

    -- Term information
    term_months,
    remaining_months,
    term_months - remaining_months AS months_elapsed,

    -- Remaining term percentage
    CASE
        WHEN term_months > 0
        THEN ROUND((remaining_months / term_months) * 100, 2)
        ELSE 0
    END AS remaining_term_pct,

    -- Payment information
    next_payment_date,
    DATEDIFF(next_payment_date, CURRENT_DATE) AS days_until_next_payment,
    next_payment_amount,

    -- Payment status
    CASE
        WHEN next_payment_date < CURRENT_DATE THEN 'OVERDUE'
        WHEN next_payment_date <= DATE_ADD(CURRENT_DATE, 7) THEN 'DUE_SOON'
        ELSE 'CURRENT'
    END AS payment_status,

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
    END AS ltv_risk_category,

    -- Metadata
    load_timestamp,
    source_file,
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
SELECT
    -- Primary Key
    application_id,

    -- Foreign Keys
    client_id,
    officer_id,

    -- Application details
    application_date,
    DATEDIFF(CURRENT_DATE, application_date) AS days_since_application,

    -- Amounts
    requested_amount,
    COALESCE(approved_amount, 0) AS approved_amount,
    requested_amount - COALESCE(approved_amount, 0) AS amount_difference,

    -- Approval percentage
    CASE
        WHEN approved_amount IS NOT NULL AND approved_amount > 0
        THEN ROUND((approved_amount / requested_amount) * 100, 2)
        ELSE 0
    END AS approval_percentage,

    -- Purpose
    UPPER(TRIM(purpose)) AS purpose,

    -- Status normalization
    CASE
        WHEN UPPER(TRIM(status)) IN ('APPROVED', 'ACCEPTED') THEN 'APPROVED'
        WHEN UPPER(TRIM(status)) IN ('REJECTED', 'DECLINED', 'DENIED') THEN 'REJECTED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'UNDER_REVIEW', 'IN_REVIEW') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('WITHDRAWN', 'CANCELLED') THEN 'WITHDRAWN'
        ELSE 'UNKNOWN'
    END AS status_normalized,

    -- Decision category
    CASE
        WHEN UPPER(TRIM(status)) IN ('APPROVED', 'ACCEPTED') THEN 'APPROVED'
        WHEN UPPER(TRIM(status)) IN ('REJECTED', 'DECLINED', 'DENIED') THEN 'REJECTED'
        WHEN UPPER(TRIM(status)) IN ('PENDING', 'UNDER_REVIEW', 'IN_REVIEW') THEN 'PENDING'
        WHEN UPPER(TRIM(status)) IN ('WITHDRAWN', 'CANCELLED') THEN 'WITHDRAWN'
        ELSE 'OTHER'
    END AS decision_category,

    decision_date,

    -- Days to decision
    CASE
        WHEN decision_date IS NOT NULL AND application_date IS NOT NULL
        THEN DATEDIFF(decision_date, application_date)
        ELSE NULL
    END AS days_to_decision,

    ROUND(interest_rate_proposed, 4) AS interest_rate_proposed,

    -- Interest rate category
    CASE
        WHEN interest_rate_proposed < 3 THEN 'LOW'
        WHEN interest_rate_proposed >= 3 AND interest_rate_proposed < 6 THEN 'MEDIUM'
        WHEN interest_rate_proposed >= 6 AND interest_rate_proposed < 10 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS interest_rate_category,

    TRIM(reason_for_rejection) AS reason_for_rejection,

    -- Has rejection reason flag
    CASE
        WHEN reason_for_rejection IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_rejection_reason,

    -- Metadata
    load_timestamp,
    source_file,
    CURRENT_TIMESTAMP AS process_timestamp,
    'bronze.credit_applications' AS source_system

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
