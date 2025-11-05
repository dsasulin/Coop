CREATE TABLE clients (
    client_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    birth_date DATE,
    registration_date DATE,
    address STRING,
    city STRING,
    country STRING,
    risk_category STRING,
    credit_score INT,
    employment_status STRING,
    annual_income DECIMAL(10,2)
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE products (
    product_id INT,
    product_name STRING,
    product_type STRING,
    currency STRING,
    active BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");


CREATE TABLE contracts (
    contract_id INT,
    client_id INT,
    product_id INT,
    contract_number STRING,
    start_date DATE,
    end_date DATE,
    contract_amount DECIMAL(12,2),
    interest_rate DECIMAL(5,2),
    status STRING,
    monthly_payment DECIMAL(10,2),
    created_date DATE
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE accounts (
    account_id INT,
    client_id INT,
    contract_id INT,
    account_number STRING,
    account_type STRING,
    currency STRING,
    open_date DATE,
    close_date DATE,
    status STRING,
    branch_code STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE client_products (
    relationship_id INT,
    client_id INT,
    product_id INT,
    relationship_type STRING,
    start_date DATE,
    end_date DATE,
    status STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE transactions (
    transaction_id INT,
    transaction_uuid STRING,
    from_account_id INT,
    to_account_id INT,
    from_account_number STRING,
    to_account_number STRING,
    transaction_type STRING,
    amount DECIMAL(10,2),
    currency STRING,
    transaction_date TIMESTAMP,
    description STRING,
    status STRING,
    category STRING,
    merchant_name STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE account_balances (
    balance_id INT,
    account_id INT,
    current_balance DECIMAL(12,2),
    available_balance DECIMAL(12,2),
    currency STRING,
    last_updated TIMESTAMP,
    credit_limit DECIMAL(10,2)
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE cards (
    card_id INT,
    client_id INT,
    account_id INT,
    card_number STRING,
    card_holder_name STRING,
    expiry_date DATE,
    cvv INT,
    card_type STRING,
    card_level STRING,
    status STRING,
    issue_date DATE
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE branches (
    branch_id INT,
    branch_code STRING,
    branch_name STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    phone STRING,
    manager_name STRING,
    opening_date DATE
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE employees (
    employee_id INT,
    branch_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    position STRING,
    department STRING,
    hire_date DATE,
    salary DECIMAL(10,2),
    status STRING
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE loans (
    loan_id INT,
    contract_id INT,
    loan_amount DECIMAL(12,2),
    outstanding_balance DECIMAL(12,2),
    interest_rate DECIMAL(5,2),
    term_months INT,
    remaining_months INT,
    next_payment_date DATE,
    next_payment_amount DECIMAL(10,2),
    delinquency_status STRING,
    collateral_value DECIMAL(12,2),
    loan_to_value_ratio DECIMAL(4,3)
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE TABLE credit_applications (
    application_id INT,
    client_id INT,
    application_date DATE,
    requested_amount INT,
    approved_amount INT,
    purpose STRING,
    status STRING,
    decision_date DATE,
    interest_rate_proposed DECIMAL(5,2),
    reason_for_rejection STRING,
    officer_id INT
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");



