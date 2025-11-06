-- ============================================================================
-- ETL Job: Stage (test) -> Bronze Layer
-- Description: Load raw data from stage tables to bronze layer
-- Execution: Run in Hue SQL Editor
-- ============================================================================

-- Set dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

-- ============================================================================
-- 1. CLIENTS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.clients;

INSERT INTO TABLE bronze.clients
SELECT
    client_id,
    first_name,
    last_name,
    email,
    phone,
    birth_date,
    registration_date,
    address,
    city,
    state,
    country,
    postal_code,
    occupation,
    employment_status,
    annual_income,
    credit_score,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.clients' as source_file
FROM test.clients;

-- Check results
SELECT COUNT(*) as bronze_clients_count FROM bronze.clients;

-- ============================================================================
-- 2. PRODUCTS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.products;

INSERT INTO TABLE bronze.products
SELECT
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
    created_date,
    updated_date,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.products' as source_file
FROM test.products;

SELECT COUNT(*) as bronze_products_count FROM bronze.products;

-- ============================================================================
-- 3. CONTRACTS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.contracts;

INSERT INTO TABLE bronze.contracts
SELECT
    contract_id,
    client_id,
    product_id,
    contract_number,
    contract_date,
    start_date,
    end_date,
    status,
    contract_amount,
    currency,
    interest_rate,
    term_months,
    monthly_payment,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.contracts' as source_file
FROM test.contracts;

SELECT COUNT(*) as bronze_contracts_count FROM bronze.contracts;

-- ============================================================================
-- 4. ACCOUNTS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.accounts;

INSERT INTO TABLE bronze.accounts
SELECT
    account_id,
    client_id,
    contract_id,
    account_number,
    account_type,
    currency,
    open_date,
    close_date,
    status,
    branch_code,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.accounts' as source_file
FROM test.accounts;

SELECT COUNT(*) as bronze_accounts_count FROM bronze.accounts;

-- ============================================================================
-- 5. TRANSACTIONS - Load to Bronze (with partitioning)
-- ============================================================================
-- Note: Using dynamic partitioning by year and month

INSERT OVERWRITE TABLE bronze.transactions
PARTITION (transaction_year, transaction_month)
SELECT
    transaction_id,
    from_account_id,
    to_account_id,
    transaction_date,
    amount,
    currency,
    transaction_type,
    category,
    description,
    status,
    channel,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.transactions' as source_file,
    -- Partition columns
    YEAR(transaction_date) as transaction_year,
    MONTH(transaction_date) as transaction_month
FROM test.transactions;

-- Check partitions
SHOW PARTITIONS bronze.transactions;

-- Check counts
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) as transaction_count
FROM bronze.transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year DESC, transaction_month DESC;

-- ============================================================================
-- 6. ACCOUNT_BALANCES - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.account_balances;

INSERT INTO TABLE bronze.account_balances
SELECT
    balance_id,
    account_id,
    balance_date,
    current_balance,
    available_balance,
    currency,
    overdraft_limit,
    credit_limit,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.account_balances' as source_file
FROM test.account_balances;

SELECT COUNT(*) as bronze_balances_count FROM bronze.account_balances;

-- ============================================================================
-- 7. CARDS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.cards;

INSERT INTO TABLE bronze.cards
SELECT
    card_id,
    client_id,
    account_id,
    card_number,
    card_type,
    card_level,
    issue_date,
    expiry_date,
    status,
    credit_limit,
    cvv,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.cards' as source_file
FROM test.cards;

SELECT COUNT(*) as bronze_cards_count FROM bronze.cards;

-- ============================================================================
-- 8. BRANCHES - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.branches;

INSERT INTO TABLE bronze.branches
SELECT
    branch_id,
    branch_code,
    branch_name,
    address,
    city,
    state,
    country,
    postal_code,
    phone,
    email,
    manager_name,
    open_date,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.branches' as source_file
FROM test.branches;

SELECT COUNT(*) as bronze_branches_count FROM bronze.branches;

-- ============================================================================
-- 9. EMPLOYEES - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.employees;

INSERT INTO TABLE bronze.employees
SELECT
    employee_id,
    branch_id,
    first_name,
    last_name,
    email,
    phone,
    position,
    department,
    hire_date,
    salary,
    manager_id,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.employees' as source_file
FROM test.employees;

SELECT COUNT(*) as bronze_employees_count FROM bronze.employees;

-- ============================================================================
-- 10. LOANS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.loans;

INSERT INTO TABLE bronze.loans
SELECT
    loan_id,
    contract_id,
    loan_type,
    loan_amount,
    currency,
    interest_rate,
    term_months,
    monthly_payment,
    start_date,
    maturity_date,
    outstanding_balance,
    next_payment_date,
    next_payment_amount,
    payment_day,
    delinquency_status,
    days_past_due,
    collateral_type,
    collateral_value,
    loan_to_value_ratio,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.loans' as source_file
FROM test.loans;

SELECT COUNT(*) as bronze_loans_count FROM bronze.loans;

-- ============================================================================
-- 11. CREDIT_APPLICATIONS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.credit_applications;

INSERT INTO TABLE bronze.credit_applications
SELECT
    application_id,
    client_id,
    application_date,
    product_type,
    requested_amount,
    requested_term_months,
    purpose,
    employment_status,
    annual_income,
    existing_debt,
    status,
    decision_date,
    approved_amount,
    approved_term_months,
    interest_rate_proposed,
    rejection_reason,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.credit_applications' as source_file
FROM test.credit_applications;

SELECT COUNT(*) as bronze_credit_applications_count FROM bronze.credit_applications;

-- ============================================================================
-- 12. CLIENT_PRODUCTS - Load to Bronze
-- ============================================================================
TRUNCATE TABLE bronze.client_products;

INSERT INTO TABLE bronze.client_products
SELECT
    client_id,
    product_id,
    start_date,
    end_date,
    status,
    -- Technical fields
    CURRENT_TIMESTAMP as load_timestamp,
    'test.client_products' as source_file
FROM test.client_products;

SELECT COUNT(*) as bronze_client_products_count FROM bronze.client_products;

-- ============================================================================
-- FINAL SUMMARY - Bronze Layer Load Statistics
-- ============================================================================
SELECT
    'clients' as table_name,
    COUNT(*) as record_count
FROM bronze.clients
UNION ALL
SELECT 'products', COUNT(*) FROM bronze.products
UNION ALL
SELECT 'contracts', COUNT(*) FROM bronze.contracts
UNION ALL
SELECT 'accounts', COUNT(*) FROM bronze.accounts
UNION ALL
SELECT 'transactions', COUNT(*) FROM bronze.transactions
UNION ALL
SELECT 'account_balances', COUNT(*) FROM bronze.account_balances
UNION ALL
SELECT 'cards', COUNT(*) FROM bronze.cards
UNION ALL
SELECT 'branches', COUNT(*) FROM bronze.branches
UNION ALL
SELECT 'employees', COUNT(*) FROM bronze.employees
UNION ALL
SELECT 'loans', COUNT(*) FROM bronze.loans
UNION ALL
SELECT 'credit_applications', COUNT(*) FROM bronze.credit_applications
UNION ALL
SELECT 'client_products', COUNT(*) FROM bronze.client_products
ORDER BY table_name;

-- ============================================================================
-- END OF STAGE TO BRONZE ETL
-- ============================================================================
