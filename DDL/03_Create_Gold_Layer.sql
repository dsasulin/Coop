-- =====================================================
-- GOLD LAYER - Business Aggregates and Analytics
-- =====================================================
-- Gold layer contains aggregated data and business metrics
-- Ready-to-use data marts for analytics and reporting

-- Create Gold database
CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Gold layer - business aggregates and analytics-ready data'
LOCATION '/user/hive/warehouse/gold.db';

USE gold;

-- =====================================================
-- DIMENSION TABLES (SCD Type 2 ready)
-- =====================================================

-- Client Dimension (360-degree view)
CREATE TABLE IF NOT EXISTS dim_client (
    client_key BIGINT,                   -- Surrogate key
    client_id INT,                       -- Business key
    first_name STRING,
    last_name STRING,
    full_name STRING,
    email STRING,
    email_domain STRING,
    phone STRING,
    birth_date DATE,
    age INT,
    age_group STRING,                    -- 18-25, 26-35, 36-45, etc.
    registration_date DATE,
    customer_tenure_days INT,
    customer_tenure_years DECIMAL(4,2),
    address STRING,
    city STRING,
    country STRING,
    region STRING,                       -- Derived geographic region
    risk_category STRING,
    credit_score INT,
    credit_score_category STRING,
    employment_status STRING,
    annual_income DECIMAL(10,2),
    income_category STRING,
    -- Client Segment
    client_segment STRING,               -- VIP/PREMIUM/REGULAR/BASIC
    client_lifetime_value DECIMAL(12,2), -- CLV
    -- Aggregated metrics
    total_accounts INT,
    total_active_accounts INT,
    total_products INT,
    total_contracts INT,
    total_cards INT,
    total_loans INT,
    -- SCD Type 2 fields
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGINT,                  -- Surrogate key
    product_id INT,                      -- Business key
    product_name STRING,
    product_type STRING,
    product_category STRING,
    currency STRING,
    active BOOLEAN,
    -- Aggregated metrics
    total_clients INT,
    total_contracts INT,
    total_revenue DECIMAL(12,2),
    -- SCD Type 2 fields
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Branch Dimension
CREATE TABLE IF NOT EXISTS dim_branch (
    branch_key BIGINT,                   -- Surrogate key
    branch_id INT,                       -- Business key
    branch_code STRING,
    branch_name STRING,
    address STRING,
    city STRING,
    state STRING,
    state_code STRING,
    zip_code STRING,
    region STRING,                       -- Derived
    phone STRING,
    manager_name STRING,
    opening_date DATE,
    branch_age_days INT,
    -- Aggregated metrics
    total_employees INT,
    total_accounts INT,
    total_clients INT,
    -- SCD Type 2 fields
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Date Dimension (pre-calculated calendar table)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT,                        -- YYYYMMDD format
    date_value DATE,
    year INT,
    quarter INT,
    quarter_name STRING,                 -- Q1 2025
    month INT,
    month_name STRING,                   -- January
    month_short STRING,                  -- Jan
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name STRING,                     -- Monday
    day_short STRING,                    -- Mon
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    is_business_day BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- =====================================================
-- FACT TABLES
-- =====================================================

-- Transaction Facts (Daily aggregates)
CREATE TABLE IF NOT EXISTS fact_transactions_daily (
    transaction_date DATE,
    client_id INT,
    account_id INT,
    branch_code STRING,
    transaction_type STRING,
    category STRING,
    currency STRING,
    -- Metrics
    transaction_count BIGINT,
    total_amount DECIMAL(15,2),
    avg_amount DECIMAL(12,2),
    min_amount DECIMAL(12,2),
    max_amount DECIMAL(12,2),
    successful_count BIGINT,
    failed_count BIGINT,
    pending_count BIGINT,
    cancelled_count BIGINT,
    success_rate DECIMAL(5,2),
    -- Flags
    has_suspicious_transactions BOOLEAN,
    suspicious_transaction_count INT,
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Account Balance Facts (Daily snapshots)
CREATE TABLE IF NOT EXISTS fact_account_balance_daily (
    snapshot_date DATE,
    account_id INT,
    client_id INT,
    account_type STRING,
    branch_code STRING,
    currency STRING,
    -- Balance metrics
    current_balance DECIMAL(12,2),
    available_balance DECIMAL(12,2),
    reserved_amount DECIMAL(12,2),
    credit_limit DECIMAL(10,2),
    credit_utilization DECIMAL(5,2),
    balance_category STRING,
    -- Change metrics
    balance_change_daily DECIMAL(12,2),
    balance_change_weekly DECIMAL(12,2),
    balance_change_monthly DECIMAL(12,2),
    -- Technical fields
    created_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Loan Performance Facts
CREATE TABLE IF NOT EXISTS fact_loan_performance (
    loan_id INT,
    contract_id INT,
    client_id INT,
    snapshot_date DATE,
    -- Loan details
    loan_amount DECIMAL(12,2),
    outstanding_balance DECIMAL(12,2),
    paid_amount DECIMAL(12,2),
    payment_progress DECIMAL(5,2),
    interest_rate DECIMAL(5,2),
    term_months INT,
    remaining_months INT,
    elapsed_months INT,
    -- Payment metrics
    next_payment_date DATE,
    next_payment_amount DECIMAL(10,2),
    days_to_next_payment INT,
    total_payments_made INT,
    missed_payments INT,
    late_payments INT,
    on_time_payment_rate DECIMAL(5,2),
    -- Risk metrics
    delinquency_status STRING,
    is_delinquent BOOLEAN,
    days_past_due INT,
    collateral_value DECIMAL(12,2),
    loan_to_value_ratio DECIMAL(4,3),
    ltv_category STRING,
    default_probability DECIMAL(5,4),    -- Predictive score
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- =====================================================
-- BUSINESS AGGREGATES (Wide tables)
-- =====================================================

-- Client 360 View (Complete client profile)
CREATE TABLE IF NOT EXISTS client_360_view (
    client_id INT,
    snapshot_date DATE,
    -- Basic info
    full_name STRING,
    email STRING,
    phone STRING,
    age INT,
    age_group STRING,
    city STRING,
    country STRING,
    -- Financial profile
    risk_category STRING,
    credit_score INT,
    credit_score_category STRING,
    annual_income DECIMAL(10,2),
    income_category STRING,
    client_segment STRING,
    -- Account summary
    total_accounts INT,
    active_accounts INT,
    checking_accounts INT,
    savings_accounts INT,
    loan_accounts INT,
    total_balance DECIMAL(15,2),
    avg_balance DECIMAL(12,2),
    -- Product holdings
    total_products INT,
    total_contracts INT,
    total_cards INT,
    active_cards INT,
    -- Loan summary
    total_loans INT,
    active_loans INT,
    total_loan_amount DECIMAL(15,2),
    total_outstanding_balance DECIMAL(15,2),
    total_loan_payment DECIMAL(12,2),
    delinquent_loans INT,
    -- Transaction behavior (Last 30 days)
    transactions_30d INT,
    transaction_volume_30d DECIMAL(15,2),
    avg_transaction_30d DECIMAL(12,2),
    deposits_30d INT,
    withdrawals_30d INT,
    transfers_30d INT,
    -- Engagement metrics
    last_transaction_date DATE,
    days_since_last_transaction INT,
    transaction_frequency STRING,        -- DAILY/WEEKLY/MONTHLY/INACTIVE
    channel_preference STRING,           -- Most used channel
    -- Lifetime value
    customer_lifetime_value DECIMAL(12,2),
    total_revenue DECIMAL(12,2),
    total_fees_paid DECIMAL(10,2),
    profitability_score DECIMAL(5,2),
    -- Risk indicators
    risk_score DECIMAL(5,2),
    fraud_alerts INT,
    suspicious_activities INT,
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
PARTITIONED BY (snapshot_year INT, snapshot_month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Product Performance Summary
CREATE TABLE IF NOT EXISTS product_performance_summary (
    product_id INT,
    report_date DATE,
    -- Product details
    product_name STRING,
    product_type STRING,
    product_category STRING,
    -- Customer metrics
    total_clients INT,
    new_clients_mtd INT,
    churned_clients_mtd INT,
    active_clients INT,
    retention_rate DECIMAL(5,2),
    -- Financial metrics
    total_contracts INT,
    active_contracts INT,
    total_contract_value DECIMAL(15,2),
    avg_contract_value DECIMAL(12,2),
    total_revenue_mtd DECIMAL(15,2),
    total_revenue_ytd DECIMAL(15,2),
    -- Growth metrics
    client_growth_rate DECIMAL(5,2),
    revenue_growth_rate DECIMAL(5,2),
    market_share DECIMAL(5,2),
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Branch Performance Dashboard
CREATE TABLE IF NOT EXISTS branch_performance_dashboard (
    branch_id INT,
    branch_code STRING,
    report_date DATE,
    -- Branch details
    branch_name STRING,
    city STRING,
    state STRING,
    region STRING,
    manager_name STRING,
    -- Staff metrics
    total_employees INT,
    active_employees INT,
    avg_employee_tenure_years DECIMAL(4,2),
    -- Customer metrics
    total_clients INT,
    new_clients_mtd INT,
    active_clients INT,
    total_accounts INT,
    active_accounts INT,
    -- Financial metrics
    total_deposits DECIMAL(15,2),
    total_loans_issued DECIMAL(15,2),
    total_transaction_volume DECIMAL(15,2),
    total_revenue_mtd DECIMAL(15,2),
    total_fees_collected DECIMAL(12,2),
    -- Performance KPIs
    avg_account_balance DECIMAL(12,2),
    loan_approval_rate DECIMAL(5,2),
    customer_satisfaction_score DECIMAL(3,2),
    nps_score DECIMAL(5,2),              -- Net Promoter Score
    -- Rankings
    revenue_rank INT,
    customer_rank INT,
    efficiency_rank INT,
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Financial KPI Summary (Top-level metrics)
CREATE TABLE IF NOT EXISTS financial_kpi_summary (
    report_date DATE,
    metric_category STRING,              -- DEPOSITS/LOANS/TRANSACTIONS/REVENUE
    -- Overall metrics
    total_clients INT,
    active_clients INT,
    total_accounts INT,
    active_accounts INT,
    -- Financial metrics
    total_assets DECIMAL(18,2),
    total_liabilities DECIMAL(18,2),
    net_assets DECIMAL(18,2),
    total_deposits DECIMAL(18,2),
    total_loans DECIMAL(18,2),
    total_revenue_mtd DECIMAL(15,2),
    total_revenue_ytd DECIMAL(15,2),
    -- Transaction metrics
    total_transactions BIGINT,
    total_transaction_volume DECIMAL(18,2),
    avg_transaction_value DECIMAL(12,2),
    transaction_success_rate DECIMAL(5,2),
    -- Risk metrics
    total_delinquent_loans INT,
    delinquency_rate DECIMAL(5,2),
    total_npl_amount DECIMAL(15,2),      -- Non-performing loans
    provision_coverage_ratio DECIMAL(5,2),
    -- Growth metrics
    client_growth_rate DECIMAL(5,2),
    revenue_growth_rate DECIMAL(5,2),
    loan_growth_rate DECIMAL(5,2),
    deposit_growth_rate DECIMAL(5,2),
    -- Technical fields
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Data Quality Dashboard
CREATE TABLE IF NOT EXISTS data_quality_dashboard (
    report_date DATE,
    table_name STRING,
    database_name STRING,
    -- Volume metrics
    total_records BIGINT,
    new_records_today BIGINT,
    updated_records_today BIGINT,
    deleted_records_today BIGINT,
    -- Quality metrics
    complete_records BIGINT,
    incomplete_records BIGINT,
    completeness_rate DECIMAL(5,2),
    duplicate_records BIGINT,
    duplicate_rate DECIMAL(5,2),
    null_critical_fields BIGINT,
    invalid_format_records BIGINT,
    -- Overall quality score
    data_quality_score DECIMAL(5,2),     -- 0-100
    data_quality_grade STRING,           -- A/B/C/D/F
    -- Issue tracking
    critical_issues INT,
    major_issues INT,
    minor_issues INT,
    issue_details STRING,                -- JSON with details
    -- Technical fields
    created_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");
