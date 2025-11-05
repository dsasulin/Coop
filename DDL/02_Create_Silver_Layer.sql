-- =====================================================
-- SILVER LAYER - Cleaned and Validated Data
-- =====================================================
-- Silver layer contains cleaned and validated data
-- Business rules applied, data types corrected, duplicates removed

-- Create Silver database
CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Silver layer - cleaned and validated data'
LOCATION '/user/hive/warehouse/silver.db';

USE silver;

-- Clients (cleaned client data)
CREATE TABLE IF NOT EXISTS clients (
    client_id INT,
    first_name STRING,
    last_name STRING,
    full_name STRING,                    -- Derived field
    email STRING,
    email_domain STRING,                 -- Derived field
    phone STRING,
    phone_normalized STRING,             -- Normalized phone format
    birth_date DATE,
    age INT,                             -- Derived field
    registration_date DATE,
    address STRING,
    city STRING,
    country STRING,
    risk_category STRING,
    credit_score INT,
    credit_score_category STRING,        -- Derived: POOR/FAIR/GOOD/EXCELLENT
    employment_status STRING,
    annual_income DECIMAL(10,2),
    income_category STRING,              -- Derived: LOW/MEDIUM/HIGH
    is_active BOOLEAN,                   -- Derived from various checks
    -- Data Quality flags
    dq_score DECIMAL(3,2),              -- Data quality score 0-1
    dq_issues STRING,                    -- List of quality issues
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
PARTITIONED BY (registration_year INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Products (cleaned product data)
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    product_type STRING,
    product_category STRING,            -- Derived: standardized categories
    currency STRING,
    active BOOLEAN,
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Contracts (cleaned contract data)
CREATE TABLE IF NOT EXISTS contracts (
    contract_id INT,
    client_id INT,
    product_id INT,
    contract_number STRING,
    start_date DATE,
    end_date DATE,
    contract_duration_days INT,          -- Derived
    contract_amount DECIMAL(12,2),
    interest_rate DECIMAL(5,2),
    status STRING,
    status_normalized STRING,            -- Standardized status
    monthly_payment DECIMAL(10,2),
    created_date DATE,
    is_expired BOOLEAN,                  -- Derived
    days_to_expiry INT,                  -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
PARTITIONED BY (start_year INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Accounts (cleaned account data)
CREATE TABLE IF NOT EXISTS accounts (
    account_id INT,
    client_id INT,
    contract_id INT,
    account_number STRING,
    account_type STRING,
    account_type_normalized STRING,      -- Standardized account type
    currency STRING,
    open_date DATE,
    close_date DATE,
    account_age_days INT,                -- Derived
    status STRING,
    status_normalized STRING,            -- Standardized status
    branch_code STRING,
    is_active BOOLEAN,                   -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
PARTITIONED BY (open_year INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Client Products (cleaned relationship data)
CREATE TABLE IF NOT EXISTS client_products (
    relationship_id INT,
    client_id INT,
    product_id INT,
    relationship_type STRING,
    relationship_type_normalized STRING,  -- Standardized
    start_date DATE,
    end_date DATE,
    relationship_duration_days INT,       -- Derived
    status STRING,
    is_active BOOLEAN,                    -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Transactions (cleaned transaction data)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGINT,
    transaction_uuid STRING,
    from_account_id INT,
    to_account_id INT,
    from_account_number STRING,
    to_account_number STRING,
    transaction_type STRING,
    transaction_type_normalized STRING,    -- Standardized
    amount DECIMAL(10,2),
    amount_abs DECIMAL(10,2),             -- Absolute value
    currency STRING,
    transaction_date TIMESTAMP,
    transaction_date_only DATE,           -- Date part
    transaction_hour INT,                  -- Hour of transaction
    description STRING,
    status STRING,
    status_normalized STRING,              -- Standardized
    category STRING,
    category_normalized STRING,            -- Standardized
    merchant_name STRING,
    is_internal_transfer BOOLEAN,         -- Derived
    is_suspicious BOOLEAN,                -- Fraud flag (derived)
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
PARTITIONED BY (transaction_year INT, transaction_month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Account Balances (cleaned balance data)
CREATE TABLE IF NOT EXISTS account_balances (
    balance_id INT,
    account_id INT,
    current_balance DECIMAL(12,2),
    available_balance DECIMAL(12,2),
    reserved_amount DECIMAL(12,2),        -- Derived: current - available
    currency STRING,
    last_updated TIMESTAMP,
    credit_limit DECIMAL(10,2),
    credit_utilization DECIMAL(5,2),      -- Derived: balance/limit ratio
    balance_category STRING,               -- Derived: NEGATIVE/LOW/MEDIUM/HIGH
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Cards (cleaned card data - masked)
CREATE TABLE IF NOT EXISTS cards (
    card_id INT,
    client_id INT,
    account_id INT,
    card_number_masked STRING,            -- Masked: ****-****-****-1234
    card_holder_name STRING,
    expiry_date DATE,
    card_type STRING,
    card_type_normalized STRING,          -- Standardized
    card_level STRING,
    status STRING,
    status_normalized STRING,             -- Standardized
    issue_date DATE,
    card_age_days INT,                    -- Derived
    is_expired BOOLEAN,                   -- Derived
    days_to_expiry INT,                   -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Branches (cleaned branch data)
CREATE TABLE IF NOT EXISTS branches (
    branch_id INT,
    branch_code STRING,
    branch_name STRING,
    address STRING,
    city STRING,
    state STRING,
    state_code STRING,                   -- Standardized state code
    zip_code STRING,
    phone STRING,
    phone_normalized STRING,             -- Normalized format
    manager_name STRING,
    opening_date DATE,
    branch_age_days INT,                 -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Employees (cleaned employee data)
CREATE TABLE IF NOT EXISTS employees (
    employee_id INT,
    branch_id INT,
    first_name STRING,
    last_name STRING,
    full_name STRING,                    -- Derived
    email STRING,
    email_domain STRING,                 -- Derived
    phone STRING,
    phone_normalized STRING,             -- Normalized
    position STRING,
    position_normalized STRING,          -- Standardized
    department STRING,
    department_normalized STRING,        -- Standardized
    hire_date DATE,
    tenure_days INT,                     -- Derived
    tenure_years DECIMAL(4,2),          -- Derived
    salary DECIMAL(10,2),
    salary_category STRING,              -- Derived: LOW/MEDIUM/HIGH
    status STRING,
    status_normalized STRING,            -- Standardized
    is_active BOOLEAN,                   -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Loans (cleaned loan data)
CREATE TABLE IF NOT EXISTS loans (
    loan_id INT,
    contract_id INT,
    loan_amount DECIMAL(12,2),
    outstanding_balance DECIMAL(12,2),
    paid_amount DECIMAL(12,2),           -- Derived
    payment_progress DECIMAL(5,2),       -- Derived: % paid
    interest_rate DECIMAL(5,2),
    term_months INT,
    remaining_months INT,
    elapsed_months INT,                  -- Derived
    next_payment_date DATE,
    next_payment_amount DECIMAL(10,2),
    delinquency_status STRING,
    delinquency_status_normalized STRING, -- Standardized
    is_delinquent BOOLEAN,               -- Derived
    collateral_value DECIMAL(12,2),
    loan_to_value_ratio DECIMAL(4,3),
    ltv_category STRING,                 -- Derived: LOW/MEDIUM/HIGH risk
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Credit Applications (cleaned application data)
CREATE TABLE IF NOT EXISTS credit_applications (
    application_id INT,
    client_id INT,
    application_date DATE,
    requested_amount INT,
    approved_amount INT,
    approval_rate DECIMAL(5,2),          -- Derived: approved/requested
    purpose STRING,
    purpose_normalized STRING,           -- Standardized
    status STRING,
    status_normalized STRING,            -- Standardized
    decision_date DATE,
    processing_days INT,                 -- Derived
    interest_rate_proposed DECIMAL(5,2),
    reason_for_rejection STRING,
    rejection_category STRING,           -- Derived: categorized reason
    officer_id INT,
    is_approved BOOLEAN,                 -- Derived
    is_rejected BOOLEAN,                 -- Derived
    -- Technical fields
    load_timestamp TIMESTAMP,
    process_timestamp TIMESTAMP,
    source_system STRING
)
PARTITIONED BY (application_year INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");
