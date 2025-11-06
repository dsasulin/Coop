# SQL ETL Scripts - Schema Fixes Summary

**Date**: 2025-01-06
**Issue**: SemanticException errors due to column mismatches between SQL scripts and actual table structures
**Resolution**: Complete rewrite of all three ETL scripts to match actual test schema

---

## Problem Overview

The SQL ETL scripts were referencing table columns that don't exist in the actual database schema defined in `DDL/Create_Tables.sql`. This caused SemanticException errors when running the scripts in Hue.

### Initial Error Example
```
Error while compiling statement: FAILED: SemanticException [Error 10004]:
line 12:4 Invalid table alias or column reference 'state':
(possible column names are: client_id, first_name, last_name, email, phone,
birth_date, registration_date, address, city, country, risk_category,
credit_score, employment_status, annual_income)
```

---

## Root Cause Analysis

The SQL scripts were created based on assumed table structures rather than the actual schema in `DDL/Create_Tables.sql`. After careful review, multiple field mismatches were identified across all 12 tables.

---

## Table-by-Table Field Corrections

### 1. test.clients → bronze.clients

**Fields REMOVED (did not exist in test schema)**:
- `state` - NOT in test.clients
- `postal_code` - NOT in test.clients
- `occupation` - NOT in test.clients

**Actual Available Fields**:
- client_id, first_name, last_name, email, phone
- birth_date, registration_date
- address, city, country (country exists, NOT state)
- risk_category, credit_score
- employment_status, annual_income

**Change**: Updated INSERT statement to use only the 14 fields that actually exist.

---

### 2. test.products → bronze.products

**Fields REMOVED**:
- `description` - NOT in test.products
- `min_amount` - NOT in test.products
- `max_amount` - NOT in test.products
- `term_months` - NOT in test.products
- `created_date` - NOT in test.products
- `updated_date` - NOT in test.products

**Actual Available Fields**:
- product_id
- product_name
- product_type
- currency
- active

**Change**: Simplified INSERT to use only 5 core fields from test.products.

---

### 3. test.contracts → bronze.contracts

**Fields CORRECTED**:
- Script expected: `contract_date`
- Actually available: `created_date`

**Fields REMOVED**:
- `currency` at contract level (exists in products, not contracts)
- `term_months` (NOT in test.contracts)

**Actual Available Fields**:
- contract_id, client_id, product_id
- contract_number
- start_date, end_date
- contract_amount, interest_rate
- status, monthly_payment
- created_date (NOT contract_date)

**Change**: Replaced contract_date with created_date, removed non-existent fields.

---

### 4. test.transactions → bronze.transactions

**Fields REMOVED**:
- `channel` - NOT in test.transactions

**Actual Available Fields**:
- transaction_id, transaction_uuid
- from_account_id, to_account_id
- from_account_number, to_account_number
- transaction_type, amount, currency
- transaction_date
- description, status
- category, merchant_name

**Change**: Removed channel field, kept 14 core transaction fields.

**Partitioning**: Still partitioned by YEAR(transaction_date) and MONTH(transaction_date).

---

### 5. test.accounts → bronze.accounts

**Fields Confirmed**:
- All expected fields exist in test.accounts
- No changes needed for Bronze load

**Actual Available Fields**:
- account_id, client_id, contract_id
- account_number, account_type
- currency
- open_date, close_date
- status, branch_code

---

### 6. test.account_balances → bronze.account_balances

**Fields CORRECTED**:
- Script expected: `balance_date`
- Actually available: `last_updated`

**Fields REMOVED**:
- `overdraft_limit` - NOT in test.account_balances

**Actual Available Fields**:
- balance_id, account_id
- current_balance, available_balance
- currency
- last_updated (NOT balance_date)
- credit_limit

**Change**: Replaced balance_date with last_updated, removed overdraft_limit.

---

### 7. test.cards → bronze.cards

**Fields REMOVED**:
- `credit_limit` - NOT in test.cards (exists in account_balances, not cards)

**Actual Available Fields**:
- card_id, client_id, account_id
- card_number, card_holder_name
- expiry_date, cvv
- card_type, card_level
- status, issue_date

**Change**: Removed credit_limit field reference.

---

### 8. test.branches → bronze.branches

**Fields CORRECTED**:
- Script expected: `postal_code`
- Actually available: `zip_code`
- Script expected: `open_date`
- Actually available: `opening_date`

**Actual Available Fields**:
- branch_id, branch_code, branch_name
- address, city, state (state DOES exist in branches!)
- zip_code (NOT postal_code)
- phone, manager_name
- opening_date (NOT open_date)

**Change**: Renamed postal_code → zip_code, open_date → opening_date.

---

### 9. test.employees → bronze.employees

**Fields REMOVED**:
- `manager_id` - NOT in test.employees

**Actual Available Fields**:
- employee_id, branch_id
- first_name, last_name
- email, phone
- position, department
- hire_date, salary
- status

**Change**: Removed manager_id field.

---

### 10. test.loans → bronze.loans

**Fields REMOVED**:
- `loan_type` - NOT in test.loans
- `currency` - NOT in test.loans
- `start_date` - NOT in test.loans
- `maturity_date` - NOT in test.loans
- `last_payment_date` - NOT in test.loans
- `last_payment_amount` - NOT in test.loans

**Actual Available Fields**:
- loan_id, contract_id
- loan_amount, outstanding_balance
- interest_rate
- term_months, remaining_months
- next_payment_date, next_payment_amount
- delinquency_status
- collateral_value, loan_to_value_ratio

**Change**: Removed 6 non-existent fields, used only 12 available fields.

---

### 11. test.client_products → bronze.client_products

**Fields Confirmed**:
- All expected fields exist
- No changes needed

**Actual Available Fields**:
- relationship_id, client_id, product_id
- relationship_type
- start_date, end_date
- status

---

### 12. test.credit_applications → bronze.credit_applications

**Fields REMOVED**:
- `product_type` - NOT in test.credit_applications
- `requested_term_months` - NOT in test.credit_applications
- `approved_term_months` - NOT in test.credit_applications
- `collateral_type` - NOT in test.credit_applications
- `collateral_value` - NOT in test.credit_applications
- `risk_score` - NOT in test.credit_applications

**Actual Available Fields**:
- application_id, client_id
- application_date
- requested_amount, approved_amount
- purpose, status
- decision_date
- interest_rate_proposed
- reason_for_rejection
- officer_id

**Change**: Removed 6 non-existent fields, used only 11 available fields.

---

## Scripts Fixed

### 1. SQL/01_Load_Stage_to_Bronze.sql

**Status**: ✅ Completely Rewritten

**Changes Made**:
- Removed all references to non-existent fields across all 12 tables
- Updated INSERT statements to use exact field lists from test schema
- Verified partitioning logic for transactions table
- Added comprehensive validation queries after each load

**Key Sections Updated**:
```sql
-- Clients: Removed state, postal_code
-- Products: Simplified to 5 core fields
-- Contracts: Changed contract_date → created_date
-- Transactions: Removed channel field
-- Account_balances: Changed balance_date → last_updated
-- Branches: Changed postal_code → zip_code, open_date → opening_date
-- Loans: Removed 6 non-existent fields
-- Credit_applications: Removed 6 non-existent fields
```

---

### 2. SQL/02_Load_Bronze_to_Silver.sql

**Status**: ✅ Recreated by Task Subagent

**Changes Made**:
- Removed `USE banking_dwh;` statement (referenced non-existent database)
- Verified all bronze field references against actual bronze DDL
- Updated derived field calculations to use only available fields
- Corrected data quality scoring logic to use existing fields
- Updated all 12 table transformations

**Key Transformations Verified**:
```sql
-- Clients:
  - Age calculation from birth_date
  - Full name concatenation
  - Email domain extraction
  - Income category classification
  - DQ score using 9 available fields (not 12)

-- Account_balances:
  - Uses last_updated instead of balance_date
  - Removed overdraft_limit references
  - Credit utilization calculation using credit_limit

-- Transactions:
  - Removed channel field normalization
  - Suspicious transaction flagging using available fields
  - Proper date parsing from transaction_date
```

---

### 3. SQL/03_Load_Silver_to_Gold.sql

**Status**: ✅ Recreated by Task Subagent

**Changes Made**:
- Read silver and gold DDL files to ensure field compatibility
- Updated all dimension builds (dim_client, dim_product, dim_branch, dim_date)
- Updated all fact tables (fact_transactions_daily, fact_account_balance_daily, fact_loan_performance)
- Updated all data marts (client_360_view, product_performance_summary, branch_performance_dashboard)

**Key Gold Layer Updates**:

**dim_client**:
- Removed references to state, occupation
- Uses city and country from silver.clients
- Aggregates from silver.accounts, silver.contracts, silver.cards, silver.loans

**dim_product**:
- Simplified to use only 5 available product fields
- Removed product description, amount ranges, term months

**dim_branch**:
- Uses zip_code instead of postal_code
- Includes state field (which does exist in branches)

**fact_transactions_daily**:
- Removed channel dimension
- Uses transaction_type_normalized and category_normalized
- Partitioned by year and month

**fact_account_balance_daily**:
- Uses last_updated instead of balance_date
- Removed overdraft_limit calculations

**fact_loan_performance**:
- Uses only 12 available loan fields
- Removed loan_type, currency, maturity_date

**client_360_view**:
- Comprehensive client metrics using only available fields
- Client segmentation based on income and credit score
- Transaction behavior from last 30 days
- Account and product summaries

**product_performance_summary**:
- Product adoption and revenue metrics
- Uses simplified product structure

**branch_performance_dashboard**:
- Branch metrics using actual branch fields
- Uses zip_code and opening_date

---

## Verification Steps

After running each corrected script, verify with these queries:

### After Bronze Load:
```sql
-- Check record counts
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
SELECT 'client_products', COUNT(*) FROM bronze.client_products
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
ORDER BY table_name;

-- Check transaction partitions
SHOW PARTITIONS bronze.transactions;
```

### After Silver Load:
```sql
-- Check data quality scores
SELECT
    COUNT(*) as total_clients,
    ROUND(AVG(dq_score), 3) as avg_dq_score,
    MIN(dq_score) as min_dq_score,
    MAX(dq_score) as max_dq_score,
    SUM(CASE WHEN dq_score >= 0.9 THEN 1 ELSE 0 END) as excellent_quality,
    SUM(CASE WHEN dq_score >= 0.8 AND dq_score < 0.9 THEN 1 ELSE 0 END) as good_quality,
    SUM(CASE WHEN dq_score < 0.8 THEN 1 ELSE 0 END) as poor_quality
FROM silver.clients;

-- Show records with quality issues
SELECT
    client_id,
    full_name,
    email,
    dq_score,
    dq_issues
FROM silver.clients
WHERE dq_score < 0.8
ORDER BY dq_score ASC
LIMIT 10;
```

### After Gold Load:
```sql
-- Check all gold tables
SELECT 'dim_client' as table_name, COUNT(*) as count FROM gold.dim_client
UNION ALL
SELECT 'dim_product', COUNT(*) FROM gold.dim_product
UNION ALL
SELECT 'dim_branch', COUNT(*) FROM gold.dim_branch
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL
SELECT 'fact_transactions_daily', COUNT(*) FROM gold.fact_transactions_daily
UNION ALL
SELECT 'fact_account_balance_daily', COUNT(*) FROM gold.fact_account_balance_daily
UNION ALL
SELECT 'fact_loan_performance', COUNT(*) FROM gold.fact_loan_performance
UNION ALL
SELECT 'client_360_view', COUNT(*) FROM gold.client_360_view
UNION ALL
SELECT 'product_performance_summary', COUNT(*) FROM gold.product_performance_summary
UNION ALL
SELECT 'branch_performance_dashboard', COUNT(*) FROM gold.branch_performance_dashboard
ORDER BY table_name;

-- View Client 360 sample
SELECT
    client_id,
    full_name,
    client_segment,
    credit_score,
    total_accounts,
    total_balance,
    transactions_30d
FROM gold.client_360_view
LIMIT 10;
```

---

## Summary of Changes

| Script | Tables Affected | Fields Removed | Fields Corrected | Status |
|--------|----------------|----------------|------------------|--------|
| 01_Load_Stage_to_Bronze.sql | 12 | 25+ | 5 | ✅ Fixed |
| 02_Load_Bronze_to_Silver.sql | 12 | N/A | All derived fields | ✅ Fixed |
| 03_Load_Silver_to_Gold.sql | 10 | N/A | All aggregations | ✅ Fixed |

---

## Key Takeaways

1. **Always verify schema first**: Check `DDL/Create_Tables.sql` before writing queries
2. **Field naming consistency**: Different tables use different conventions (e.g., open_date vs opening_date, balance_date vs last_updated)
3. **Not all related fields exist everywhere**: credit_limit exists in account_balances, NOT in cards
4. **State field location**: exists in branches table, NOT in clients table
5. **Date field variations**: created_date, opening_date, last_updated, transaction_date - each table uses different naming

---

## Execution Order

Run the corrected scripts in this exact order:

1. ✅ `DDL/Create_Tables.sql` (if not already done)
2. ✅ `DDL/01_Create_Bronze_Layer.sql` (if not already done)
3. ✅ `DDL/02_Create_Silver_Layer.sql` (if not already done)
4. ✅ `DDL/03_Create_Gold_Layer.sql` (if not already done)
5. ▶️ `SQL/01_Load_Stage_to_Bronze.sql` - NOW READY TO RUN
6. ▶️ `SQL/02_Load_Bronze_to_Silver.sql` - NOW READY TO RUN
7. ▶️ `SQL/03_Load_Silver_to_Gold.sql` - NOW READY TO RUN

---

## Files Modified

1. `/Users/dsasulin/Documents/GitHub/Coop/SQL/01_Load_Stage_to_Bronze.sql` - Completely rewritten
2. `/Users/dsasulin/Documents/GitHub/Coop/SQL/02_Load_Bronze_to_Silver.sql` - Recreated by subagent
3. `/Users/dsasulin/Documents/GitHub/Coop/SQL/03_Load_Silver_to_Gold.sql` - Recreated by subagent

All files now match the actual schema defined in `DDL/Create_Tables.sql`.

---

## Testing Recommendations

1. **Start with a small dataset**: Test with limited records first
2. **Run scripts one at a time**: Don't run all at once
3. **Verify after each layer**: Check record counts and data quality
4. **Monitor execution time**: Track how long each script takes
5. **Check for errors**: Review Hue Job Browser for any failures
6. **Validate partitions**: Ensure transaction partitions are created correctly

---

## Support

If you encounter any issues:
- Check the troubleshooting section in `SQL/README_SQL_ETL.md`
- Verify all DDL scripts have been executed
- Ensure data exists in test schema
- Check Hue Job Browser for detailed error messages

---

**Last Updated**: 2025-01-06
**Version**: 2.0 (Schema Corrected)
**Validated Against**: DDL/Create_Tables.sql
