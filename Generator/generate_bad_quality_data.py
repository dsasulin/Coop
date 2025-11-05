"""
Data quality issues generator for testing DQ processes
Creates ~100 records with various data quality problems
"""

import csv
import random
from datetime import datetime, timedelta

# Quality issues to generate:
# 1. NULL/empty values in critical fields
# 2. Invalid formats (email, phone)
# 3. Invalid values (negative amounts where shouldn't be, future dates)
# 4. Duplicates
# 5. Data inconsistencies (references to non-existent IDs)
# 6. Out of bounds values
# 7. Invalid data types

def generate_bad_clients():
    """Generate clients with quality issues"""
    clients = []

    issues = [
        # 1-10: NULL in required fields
        {'client_id': 1, 'first_name': None, 'last_name': 'Smith', 'email': 'john.smith@example.com', 'phone': '555-0101', 'birth_date': '1980-01-15', 'registration_date': '2020-01-01', 'address': '123 Main St', 'city': 'New York', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 750, 'employment_status': 'EMPLOYED', 'annual_income': 75000.00},
        {'client_id': 2, 'first_name': 'Jane', 'last_name': None, 'email': 'jane@example.com', 'phone': '555-0102', 'birth_date': '1985-03-20', 'registration_date': '2020-02-01', 'address': '456 Oak Ave', 'city': 'Boston', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': 680, 'employment_status': 'EMPLOYED', 'annual_income': 65000.00},
        {'client_id': 3, 'first_name': 'Bob', 'last_name': 'Wilson', 'email': None, 'phone': '555-0103', 'birth_date': '1990-06-10', 'registration_date': '2020-03-01', 'address': '789 Elm St', 'city': 'Chicago', 'country': 'US', 'risk_category': 'HIGH', 'credit_score': 620, 'employment_status': 'SELF_EMPLOYED', 'annual_income': 45000.00},
        {'client_id': 4, 'first_name': 'Alice', 'last_name': 'Brown', 'email': 'alice@example.com', 'phone': None, 'birth_date': '1975-09-25', 'registration_date': '2020-04-01', 'address': '321 Pine Rd', 'city': 'Seattle', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 800, 'employment_status': 'EMPLOYED', 'annual_income': 95000.00},
        {'client_id': 5, 'first_name': 'Charlie', 'last_name': 'Davis', 'email': 'charlie@example.com', 'phone': '555-0105', 'birth_date': None, 'registration_date': '2020-05-01', 'address': '654 Maple Dr', 'city': 'Portland', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': 700, 'employment_status': 'EMPLOYED', 'annual_income': 70000.00},

        # 6-15: Invalid email and phone formats
        {'client_id': 6, 'first_name': 'David', 'last_name': 'Miller', 'email': 'invalid-email', 'phone': '555-0106', 'birth_date': '1982-11-05', 'registration_date': '2020-06-01', 'address': '987 Cedar Ln', 'city': 'Denver', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 720, 'employment_status': 'EMPLOYED', 'annual_income': 80000.00},
        {'client_id': 7, 'first_name': 'Emma', 'last_name': 'Garcia', 'email': 'emma@@example.com', 'phone': '555-0107', 'birth_date': '1988-02-14', 'registration_date': '2020-07-01', 'address': '147 Birch St', 'city': 'Austin', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': 690, 'employment_status': 'EMPLOYED', 'annual_income': 62000.00},
        {'client_id': 8, 'first_name': 'Frank', 'last_name': 'Martinez', 'email': 'frank@', 'phone': '555-0108', 'birth_date': '1979-07-30', 'registration_date': '2020-08-01', 'address': '258 Spruce Ave', 'city': 'Phoenix', 'country': 'US', 'risk_category': 'HIGH', 'credit_score': 640, 'employment_status': 'UNEMPLOYED', 'annual_income': 0.00},
        {'client_id': 9, 'first_name': 'Grace', 'last_name': 'Rodriguez', 'email': 'grace@example.com', 'phone': 'NOT-A-PHONE', 'birth_date': '1992-04-18', 'registration_date': '2020-09-01', 'address': '369 Willow Ct', 'city': 'Miami', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': 710, 'employment_status': 'EMPLOYED', 'annual_income': 68000.00},
        {'client_id': 10, 'first_name': 'Henry', 'last_name': 'Lopez', 'email': 'henry@example.com', 'phone': '12345', 'birth_date': '1986-12-22', 'registration_date': '2020-10-01', 'address': '741 Aspen Way', 'city': 'Dallas', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 760, 'employment_status': 'EMPLOYED', 'annual_income': 85000.00},

        # 16-25: Invalid values
        {'client_id': 11, 'first_name': 'Ivy', 'last_name': 'Hernandez', 'email': 'ivy@example.com', 'phone': '555-0111', 'birth_date': '2030-01-01', 'registration_date': '2020-11-01', 'address': '852 Oak Park', 'city': 'Houston', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 730, 'employment_status': 'EMPLOYED', 'annual_income': 72000.00},  # Будущая дата рождения
        {'client_id': 12, 'first_name': 'Jack', 'last_name': 'Gonzalez', 'email': 'jack@example.com', 'phone': '555-0112', 'birth_date': '1985-05-15', 'registration_date': '2019-01-01', 'address': '963 Pine Plaza', 'city': 'Atlanta', 'country': 'US', 'risk_category': 'INVALID', 'credit_score': 740, 'employment_status': 'EMPLOYED', 'annual_income': 78000.00},  # Неверная категория риска
        {'client_id': 13, 'first_name': 'Kate', 'last_name': 'Wilson', 'email': 'kate@example.com', 'phone': '555-0113', 'birth_date': '1990-08-20', 'registration_date': '2021-03-01', 'address': '159 Elm Circle', 'city': 'San Diego', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': -50, 'employment_status': 'EMPLOYED', 'annual_income': 66000.00},  # Отрицательный credit score
        {'client_id': 14, 'first_name': 'Leo', 'last_name': 'Anderson', 'email': 'leo@example.com', 'phone': '555-0114', 'birth_date': '1983-03-12', 'registration_date': '2021-04-01', 'address': '357 Maple Heights', 'city': 'Las Vegas', 'country': 'US', 'risk_category': 'HIGH', 'credit_score': 1200, 'employment_status': 'EMPLOYED', 'annual_income': 55000.00},  # Слишком высокий credit score
        {'client_id': 15, 'first_name': 'Mia', 'last_name': 'Thomas', 'email': 'mia@example.com', 'phone': '555-0115', 'birth_date': '1987-10-08', 'registration_date': '2021-05-01', 'address': '486 Cedar Ridge', 'city': 'Nashville', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 750, 'employment_status': 'UNKNOWN_STATUS', 'annual_income': -10000.00},  # Отрицательный доход

        # 26-30: Duplicates
        {'client_id': 16, 'first_name': 'Noah', 'last_name': 'Taylor', 'email': 'noah@example.com', 'phone': '555-0116', 'birth_date': '1991-06-30', 'registration_date': '2021-06-01', 'address': '597 Birch Valley', 'city': 'Detroit', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': 695, 'employment_status': 'EMPLOYED', 'annual_income': 64000.00},
        {'client_id': 16, 'first_name': 'Noah', 'last_name': 'Taylor', 'email': 'noah@example.com', 'phone': '555-0116', 'birth_date': '1991-06-30', 'registration_date': '2021-06-01', 'address': '597 Birch Valley', 'city': 'Detroit', 'country': 'US', 'risk_category': 'MEDIUM', 'credit_score': 695, 'employment_status': 'EMPLOYED', 'annual_income': 64000.00},  # Exact duplicate
        {'client_id': 17, 'first_name': 'Olivia', 'last_name': 'Moore', 'email': 'olivia@example.com', 'phone': '555-0117', 'birth_date': '1984-09-17', 'registration_date': '2021-07-01', 'address': '681 Spruce Point', 'city': 'Memphis', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 780, 'employment_status': 'EMPLOYED', 'annual_income': 88000.00},
        {'client_id': 17, 'first_name': 'Olivia', 'last_name': 'Moore', 'email': 'olivia.moore@example.com', 'phone': '555-9999', 'birth_date': '1984-09-17', 'registration_date': '2021-07-05', 'address': '681 Spruce Point', 'city': 'Memphis', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 780, 'employment_status': 'EMPLOYED', 'annual_income': 88000.00},  # Duplicate with slight difference

        # 31-40: Edge cases and strange values
        {'client_id': 18, 'first_name': 'Peter', 'last_name': 'Jackson', 'email': 'peter@example.com', 'phone': '555-0118', 'birth_date': '1920-01-01', 'registration_date': '2021-08-01', 'address': '792 Willow Creek', 'city': 'Baltimore', 'country': 'US', 'risk_category': 'LOW', 'credit_score': 800, 'employment_status': 'RETIRED', 'annual_income': 35000.00},  # Очень старый возраст (105 лет)
        {'client_id': 19, 'first_name': '', 'last_name': '', 'email': '', 'phone': '', 'birth_date': '', 'registration_date': '2021-09-01', 'address': '', 'city': '', 'country': '', 'risk_category': '', 'credit_score': None, 'employment_status': '', 'annual_income': None},  # Почти все пустые
        {'client_id': 20, 'first_name': 'A', 'last_name': 'B', 'email': 'a@b.c', 'phone': '1', 'birth_date': '2000-01-01', 'registration_date': '2021-10-01', 'address': 'X', 'city': 'Y', 'country': 'ZZ', 'risk_category': 'LOW', 'credit_score': 300, 'employment_status': 'E', 'annual_income': 1.00},  # Минималистичные значения
    ]

    return issues

def generate_bad_transactions():
    """Generate transactions with quality issues"""
    transactions = []

    issues = [
        # Negative amounts where they shouldn't be
        {'transaction_id': 1, 'transaction_uuid': 'BAD-UUID-001', 'from_account_id': 1, 'to_account_id': 2, 'from_account_number': 'ACC0001', 'to_account_number': 'ACC0002', 'transaction_type': 'DEPOSIT', 'amount': -500.00, 'currency': 'USD', 'transaction_date': '2025-01-15 10:30:00', 'description': 'Invalid negative deposit', 'status': 'COMPLETED', 'category': 'SALARY', 'merchant_name': ''},

        # NULL in required fields
        {'transaction_id': 2, 'transaction_uuid': 'BAD-UUID-002', 'from_account_id': None, 'to_account_id': 3, 'from_account_number': None, 'to_account_number': 'ACC0003', 'transaction_type': 'TRANSFER', 'amount': 1000.00, 'currency': 'USD', 'transaction_date': '2025-01-16 14:20:00', 'description': 'Missing from account', 'status': 'COMPLETED', 'category': 'OTHER', 'merchant_name': ''},

        # Future dates
        {'transaction_id': 3, 'transaction_uuid': 'BAD-UUID-003', 'from_account_id': 4, 'to_account_id': 5, 'from_account_number': 'ACC0004', 'to_account_number': 'ACC0005', 'transaction_type': 'PAYMENT', 'amount': 250.00, 'currency': 'USD', 'transaction_date': '2030-12-31 23:59:59', 'description': 'Future dated transaction', 'status': 'COMPLETED', 'category': 'SHOPPING', 'merchant_name': 'Future Store'},

        # Non-existent accounts (large IDs)
        {'transaction_id': 4, 'transaction_uuid': 'BAD-UUID-004', 'from_account_id': 999999, 'to_account_id': 888888, 'from_account_number': 'ACC999999', 'to_account_number': 'ACC888888', 'transaction_type': 'TRANSFER', 'amount': 5000.00, 'currency': 'USD', 'transaction_date': '2025-01-17 09:15:00', 'description': 'Non-existent accounts', 'status': 'COMPLETED', 'category': 'OTHER', 'merchant_name': ''},

        # Invalid statuses
        {'transaction_id': 5, 'transaction_uuid': 'BAD-UUID-005', 'from_account_id': 6, 'to_account_id': 7, 'from_account_number': 'ACC0006', 'to_account_number': 'ACC0007', 'transaction_type': 'WITHDRAWAL', 'amount': 300.00, 'currency': 'USD', 'transaction_date': '2025-01-18 16:45:00', 'description': 'Invalid status', 'status': 'UNKNOWN_STATUS', 'category': 'CASH', 'merchant_name': ''},

        # Duplicate UUIDs
        {'transaction_id': 6, 'transaction_uuid': 'DUPLICATE-UUID', 'from_account_id': 8, 'to_account_id': 9, 'from_account_number': 'ACC0008', 'to_account_number': 'ACC0009', 'transaction_type': 'TRANSFER', 'amount': 1500.00, 'currency': 'USD', 'transaction_date': '2025-01-19 11:00:00', 'description': 'Duplicate UUID 1', 'status': 'COMPLETED', 'category': 'TRANSFER', 'merchant_name': ''},
        {'transaction_id': 7, 'transaction_uuid': 'DUPLICATE-UUID', 'from_account_id': 10, 'to_account_id': 11, 'from_account_number': 'ACC0010', 'to_account_number': 'ACC0011', 'transaction_type': 'PAYMENT', 'amount': 800.00, 'currency': 'USD', 'transaction_date': '2025-01-19 11:05:00', 'description': 'Duplicate UUID 2', 'status': 'COMPLETED', 'category': 'SHOPPING', 'merchant_name': 'Store ABC'},

        # Huge amounts (suspicious)
        {'transaction_id': 8, 'transaction_uuid': 'BAD-UUID-008', 'from_account_id': 12, 'to_account_id': 13, 'from_account_number': 'ACC0012', 'to_account_number': 'ACC0013', 'transaction_type': 'TRANSFER', 'amount': 99999999.99, 'currency': 'USD', 'transaction_date': '2025-01-20 03:00:00', 'description': 'Suspicious large amount', 'status': 'COMPLETED', 'category': 'OTHER', 'merchant_name': ''},

        # Zero amounts
        {'transaction_id': 9, 'transaction_uuid': 'BAD-UUID-009', 'from_account_id': 14, 'to_account_id': 15, 'from_account_number': 'ACC0014', 'to_account_number': 'ACC0015', 'transaction_type': 'PAYMENT', 'amount': 0.00, 'currency': 'USD', 'transaction_date': '2025-01-21 12:30:00', 'description': 'Zero amount transaction', 'status': 'COMPLETED', 'category': 'OTHER', 'merchant_name': ''},

        # Invalid currency
        {'transaction_id': 10, 'transaction_uuid': 'BAD-UUID-010', 'from_account_id': 16, 'to_account_id': 17, 'from_account_number': 'ACC0016', 'to_account_number': 'ACC0017', 'transaction_type': 'TRANSFER', 'amount': 500.00, 'currency': 'XXX', 'transaction_date': '2025-01-22 15:20:00', 'description': 'Invalid currency', 'status': 'COMPLETED', 'category': 'OTHER', 'merchant_name': ''},
    ]

    return issues

def generate_bad_accounts():
    """Generate accounts with quality issues"""
    accounts = []

    issues = [
        # Close dates before open dates
        {'account_id': 1, 'client_id': 1, 'contract_id': 1, 'account_number': 'BAD_ACC001', 'account_type': 'CHECKING', 'currency': 'USD', 'open_date': '2025-01-01', 'close_date': '2024-12-31', 'status': 'CLOSED', 'branch_code': 'BR001'},

        # NULL in required fields
        {'account_id': 2, 'client_id': None, 'contract_id': 2, 'account_number': 'BAD_ACC002', 'account_type': 'SAVINGS', 'currency': 'USD', 'open_date': '2024-06-15', 'close_date': None, 'status': 'ACTIVE', 'branch_code': 'BR002'},

        # Non-existent client
        {'account_id': 3, 'client_id': 999999, 'contract_id': 3, 'account_number': 'BAD_ACC003', 'account_type': 'LOAN', 'currency': 'USD', 'open_date': '2024-03-10', 'close_date': None, 'status': 'ACTIVE', 'branch_code': 'BR003'},

        # Duplicate account numbers
        {'account_id': 4, 'client_id': 4, 'contract_id': 4, 'account_number': 'DUPLICATE_ACC', 'account_type': 'CHECKING', 'currency': 'USD', 'open_date': '2024-02-20', 'close_date': None, 'status': 'ACTIVE', 'branch_code': 'BR004'},
        {'account_id': 5, 'client_id': 5, 'contract_id': 5, 'account_number': 'DUPLICATE_ACC', 'account_type': 'SAVINGS', 'currency': 'USD', 'open_date': '2024-05-15', 'close_date': None, 'status': 'ACTIVE', 'branch_code': 'BR005'},

        # Invalid status
        {'account_id': 6, 'client_id': 6, 'contract_id': 6, 'account_number': 'BAD_ACC006', 'account_type': 'CHECKING', 'currency': 'USD', 'open_date': '2024-07-01', 'close_date': None, 'status': 'INVALID_STATUS', 'branch_code': 'BR006'},

        # Empty account type
        {'account_id': 7, 'client_id': 7, 'contract_id': 7, 'account_number': 'BAD_ACC007', 'account_type': '', 'currency': 'USD', 'open_date': '2024-08-12', 'close_date': None, 'status': 'ACTIVE', 'branch_code': 'BR007'},

        # Future open date
        {'account_id': 8, 'client_id': 8, 'contract_id': 8, 'account_number': 'BAD_ACC008', 'account_type': 'SAVINGS', 'currency': 'USD', 'open_date': '2030-01-01', 'close_date': None, 'status': 'ACTIVE', 'branch_code': 'BR008'},
    ]

    return accounts

def save_to_csv(data, filename, fieldnames):
    """Save data to CSV"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Created {filename} with {len(data)} records")

def main():
    """Main function"""
    print("=" * 80)
    print("Generating Bad Quality Data for Testing")
    print("=" * 80)

    # Generate data
    bad_clients = generate_bad_clients()
    bad_transactions = generate_bad_transactions()
    bad_accounts = generate_bad_accounts()

    # Save to CSV
    save_to_csv(
        bad_clients,
        'Data/quality_test/clients_bad_quality.csv',
        ['client_id', 'first_name', 'last_name', 'email', 'phone', 'birth_date',
         'registration_date', 'address', 'city', 'country', 'risk_category',
         'credit_score', 'employment_status', 'annual_income']
    )

    save_to_csv(
        bad_transactions,
        'Data/quality_test/transactions_bad_quality.csv',
        ['transaction_id', 'transaction_uuid', 'from_account_id', 'to_account_id',
         'from_account_number', 'to_account_number', 'transaction_type', 'amount',
         'currency', 'transaction_date', 'description', 'status', 'category', 'merchant_name']
    )

    save_to_csv(
        bad_accounts,
        'Data/quality_test/accounts_bad_quality.csv',
        ['account_id', 'client_id', 'contract_id', 'account_number', 'account_type',
         'currency', 'open_date', 'close_date', 'status', 'branch_code']
    )

    print("\n" + "=" * 80)
    print("Summary of Data Quality Issues:")
    print("=" * 80)
    print("\nClients Issues:")
    print("  - NULL values in critical fields (first_name, last_name, email, phone, birth_date)")
    print("  - Invalid email formats")
    print("  - Invalid phone formats")
    print("  - Future birth dates")
    print("  - Invalid risk categories")
    print("  - Negative credit scores")
    print("  - Credit scores > 850")
    print("  - Negative annual income")
    print("  - Exact duplicates")
    print("  - Near duplicates")
    print("  - Very old age (105+ years)")
    print("  - Empty strings in multiple fields")
    print("  - Minimal/suspicious values")

    print("\nTransactions Issues:")
    print("  - Negative amounts in deposits")
    print("  - NULL in required fields (from_account_id)")
    print("  - Future transaction dates")
    print("  - References to non-existent accounts")
    print("  - Invalid status values")
    print("  - Duplicate transaction UUIDs")
    print("  - Suspiciously large amounts")
    print("  - Zero amount transactions")
    print("  - Invalid currency codes")

    print("\nAccounts Issues:")
    print("  - Close date before open date")
    print("  - NULL client_id")
    print("  - References to non-existent clients")
    print("  - Duplicate account numbers")
    print("  - Invalid status values")
    print("  - Empty account type")
    print("  - Future open dates")

    print("\n" + "=" * 80)
    print("Files created in Data/quality_test/")
    print("=" * 80)

if __name__ == "__main__":
    main()
