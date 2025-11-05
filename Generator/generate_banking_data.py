import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid

# Initialize Faker and set random seeds for reproducibility
fake = Faker()
np.random.seed(42)
random.seed(42)

print("Generating banking data...")

# 1. Clients table
def generate_clients(n=3000):
    """Generate client data with personal information"""
    clients = []
    for i in range(1, n + 1):
        clients.append({
            'client_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'registration_date': fake.date_between(start_date='-5y', end_date='today'),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'country': 'US',
            'risk_category': random.choice(['LOW', 'MEDIUM', 'HIGH']),
            'credit_score': random.randint(300, 850),
            'employment_status': random.choice(['EMPLOYED', 'SELF_EMPLOYED', 'UNEMPLOYED', 'RETIRED']),
            'annual_income': round(random.uniform(30000, 250000), 2)
        })
    return pd.DataFrame(clients)

# 2. Products table
def generate_products():
    """Generate banking products"""
    products = [
        {'product_id': 1, 'product_name': 'Debit Card', 'product_type': 'CARD', 'currency': 'USD', 'active': True},
        {'product_id': 2, 'product_name': 'Credit Card Standard', 'product_type': 'CARD', 'currency': 'USD', 'active': True},
        {'product_id': 3, 'product_name': 'Credit Card Premium', 'product_type': 'CARD', 'currency': 'USD', 'active': True},
        {'product_id': 4, 'product_name': 'Personal Loan', 'product_type': 'LOAN', 'currency': 'USD', 'active': True},
        {'product_id': 5, 'product_name': 'Mortgage', 'product_type': 'LOAN', 'currency': 'USD', 'active': True},
        {'product_id': 6, 'product_name': 'Auto Loan', 'product_type': 'LOAN', 'currency': 'USD', 'active': True},
        {'product_id': 7, 'product_name': 'Savings Account', 'product_type': 'DEPOSIT', 'currency': 'USD', 'active': True},
        {'product_id': 8, 'product_name': 'Checking Account', 'product_type': 'DEPOSIT', 'currency': 'USD', 'active': True},
        {'product_id': 9, 'product_name': 'Time Deposit', 'product_type': 'DEPOSIT', 'currency': 'USD', 'active': True},
        {'product_id': 10, 'product_name': 'Investment Account', 'product_type': 'INVESTMENT', 'currency': 'USD', 'active': True},
        {'product_id': 11, 'product_name': 'Business Account', 'product_type': 'ACCOUNT', 'currency': 'USD', 'active': True},
        {'product_id': 12, 'product_name': 'Student Loan', 'product_type': 'LOAN', 'currency': 'USD', 'active': True}
    ]
    return pd.DataFrame(products)

# 3. Contracts table
def generate_contracts(clients_df, products_df, n=6000):
    """Generate contracts linking clients and products"""
    contracts = []
    
    for i in range(1, n + 1):
        client = clients_df.sample(1).iloc[0]
        product = products_df.sample(1).iloc[0]
        
        start_date = fake.date_between(
            start_date=client['registration_date'], 
            end_date='today'
        )
        
        # Set end date based on product type
        if product['product_type'] == 'LOAN':
            end_date = start_date + timedelta(days=random.randint(365, 3650))
            contract_amount = random.randint(5000, 200000)
            interest_rate = round(random.uniform(5.0, 15.0), 2)
        elif product['product_type'] == 'DEPOSIT':
            end_date = start_date + timedelta(days=random.randint(180, 1825))
            contract_amount = random.randint(1000, 100000)
            interest_rate = round(random.uniform(1.0, 5.0), 2)
        else:
            end_date = start_date + timedelta(days=random.randint(365, 3650))
            contract_amount = random.randint(100, 50000)
            interest_rate = round(random.uniform(0.0, 10.0), 2)
        
        contracts.append({
            'contract_id': i,
            'client_id': client['client_id'],
            'product_id': product['product_id'],
            'contract_number': f"CNT{str(i).zfill(8)}",
            'start_date': start_date,
            'end_date': end_date,
            'contract_amount': contract_amount,
            'interest_rate': interest_rate,
            'status': random.choice(['ACTIVE', 'CLOSED', 'SUSPENDED', 'PENDING']),
            'monthly_payment': round(contract_amount * interest_rate / 100 / 12, 2),
            'created_date': start_date
        })
    
    return pd.DataFrame(contracts)

# 4. Accounts table
def generate_accounts(clients_df, contracts_df, n=8000):
    """Generate bank accounts"""
    accounts = []
    
    for i in range(1, n + 1):
        client = clients_df.sample(1).iloc[0]
        
        # Some accounts are linked to contracts, some are standalone
        if random.random() < 0.7 and len(contracts_df) > 0:
            contract = contracts_df.sample(1).iloc[0]
            contract_id = contract['contract_id']
        else:
            contract_id = None
        
        open_date = fake.date_between(
            start_date=client['registration_date'], 
            end_date='today'
        )
        
        accounts.append({
            'account_id': i,
            'client_id': client['client_id'],
            'contract_id': contract_id,
            'account_number': f"ACC{str(i).zfill(10)}",
            'account_type': random.choice(['SAVINGS', 'CHECKING', 'CREDIT', 'LOAN', 'DEPOSIT']),
            'currency': 'USD',
            'open_date': open_date,
            'close_date': open_date + timedelta(days=random.randint(365, 3650)) if random.random() < 0.2 else None,
            'status': random.choice(['ACTIVE', 'CLOSED', 'BLOCKED', 'DORMANT']),
            'branch_code': f"BR{random.randint(100, 999)}"
        })
    
    return pd.DataFrame(accounts)

# 5. Client-Product relationships
def generate_client_products(clients_df, products_df, n=10000):
    """Generate many-to-many relationships between clients and products"""
    relationships = []
    
    for i in range(1, n + 1):
        client = clients_df.sample(1).iloc[0]
        product = products_df.sample(1).iloc[0]
        
        relationships.append({
            'relationship_id': i,
            'client_id': client['client_id'],
            'product_id': product['product_id'],
            'relationship_type': random.choice(['OWNER', 'CO_OWNER', 'AUTHORIZED_USER', 'BENEFICIARY']),
            'start_date': fake.date_between(
                start_date=client['registration_date'], 
                end_date='today'
            ),
            'end_date': fake.date_between(start_date='today', end_date='+2y') if random.random() < 0.3 else None,
            'status': random.choice(['ACTIVE', 'INACTIVE', 'PENDING'])
        })
    
    return pd.DataFrame(relationships)

# 6. Transactions table
def generate_transactions(accounts_df, n=25000):
    """Generate transaction data"""
    transactions = []
    transaction_types = ['TRANSFER', 'PAYMENT', 'DEPOSIT', 'WITHDRAWAL', 'PURCHASE', 'FEE', 'INTEREST']
    
    for i in range(1, n + 1):
        from_account = accounts_df.sample(1).iloc[0]
        
        # Determine if it's internal or external transaction
        if random.random() < 0.6:
            to_account = accounts_df[accounts_df['account_id'] != from_account['account_id']].sample(1).iloc[0]
            to_account_id = to_account['account_id']
            to_account_number = to_account['account_number']
        else:
            to_account_id = None
            to_account_number = f"EXT{random.randint(1000000000, 9999999999)}"
        
        transaction_date = fake.date_time_between(
            start_date=from_account['open_date'], 
            end_date='now'
        )
        
        amount = round(random.uniform(1, 5000), 2)
        
        transactions.append({
            'transaction_id': i,
            'transaction_uuid': str(uuid.uuid4()),
            'from_account_id': from_account['account_id'],
            'to_account_id': to_account_id,
            'from_account_number': from_account['account_number'],
            'to_account_number': to_account_number,
            'transaction_type': random.choice(transaction_types),
            'amount': amount,
            'currency': 'USD',
            'transaction_date': transaction_date,
            'description': fake.sentence(nb_words=6),
            'status': random.choice(['COMPLETED', 'PENDING', 'FAILED', 'CANCELLED']),
            'category': random.choice(['FOOD', 'TRANSPORT', 'ENTERTAINMENT', 'BILLS', 'SHOPPING', 'SALARY', 'OTHER']),
            'merchant_name': fake.company() if random.random() < 0.7 else None
        })
    
    return pd.DataFrame(transactions)

# 7. Account balances table
def generate_account_balances(accounts_df, transactions_df):
    """Generate current balances for accounts based on transactions"""
    balances = []
    
    for account_id in accounts_df['account_id'].unique():
        account = accounts_df[accounts_df['account_id'] == account_id].iloc[0]
        
        # Calculate approximate balance based on transaction history
        account_transactions = transactions_df[
            (transactions_df['from_account_id'] == account_id) | 
            (transactions_df['to_account_id'] == account_id)
        ]
        
        # Start with initial balance
        initial_balance = random.uniform(100, 50000)
        current_balance = initial_balance
        
        # Simple balance calculation (in reality would need proper accounting)
        for _, tx in account_transactions.iterrows():
            if tx['from_account_id'] == account_id and tx['status'] == 'COMPLETED':
                current_balance -= tx['amount']
            elif tx['to_account_id'] == account_id and tx['status'] == 'COMPLETED':
                current_balance += tx['amount']
        
        # Ensure balance is not negative for non-credit accounts
        if account['account_type'] in ['SAVINGS', 'CHECKING', 'DEPOSIT'] and current_balance < 0:
            current_balance = abs(current_balance)
        
        balances.append({
            'balance_id': len(balances) + 1,
            'account_id': account_id,
            'current_balance': round(current_balance, 2),
            'available_balance': round(current_balance * random.uniform(0.8, 1.0), 2),
            'currency': 'USD',
            'last_updated': datetime.now(),
            'credit_limit': round(random.uniform(1000, 25000), 2) if account['account_type'] == 'CREDIT' else 0
        })
    
    return pd.DataFrame(balances)

# 8. Cards table
def generate_cards(clients_df, accounts_df, n=4000):
    """Generate card data"""
    cards = []
    
    for i in range(1, n + 1):
        client = clients_df.sample(1).iloc[0]
        account = accounts_df.sample(1).iloc[0]
        
        issue_date = fake.date_between(
            start_date=account['open_date'], 
            end_date='today'
        )
        expiry_date = issue_date + timedelta(days=365 * 3)
        
        cards.append({
            'card_id': i,
            'client_id': client['client_id'],
            'account_id': account['account_id'],
            'card_number': f"{random.randint(4000, 4999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)}",
            'card_holder_name': f"{client['first_name']} {client['last_name']}",
            'expiry_date': expiry_date,
            'cvv': random.randint(100, 999),
            'card_type': random.choice(['DEBIT', 'CREDIT']),
            'card_level': random.choice(['STANDARD', 'GOLD', 'PLATINUM']),
            'status': random.choice(['ACTIVE', 'BLOCKED', 'EXPIRED', 'LOST']),
            'issue_date': issue_date
        })
    
    return pd.DataFrame(cards)

# 9. Branches table
def generate_branches(n=50):
    """Generate bank branches"""
    branches = []
    
    for i in range(1, n + 1):
        branches.append({
            'branch_id': i,
            'branch_code': f"BR{str(i).zfill(3)}",
            'branch_name': f"{fake.city()} Branch",
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'phone': fake.phone_number(),
            'manager_name': fake.name(),
            'opening_date': fake.date_between(start_date='-20y', end_date='today')
        })
    
    return pd.DataFrame(branches)

# 10. Employees table
def generate_employees(branches_df, n=500):
    """Generate employee data"""
    employees = []
    
    for i in range(1, n + 1):
        branch = branches_df.sample(1).iloc[0]
        
        employees.append({
            'employee_id': i,
            'branch_id': branch['branch_id'],
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'position': random.choice(['TELLER', 'MANAGER', 'ADVISOR', 'ANALYST', 'SUPERVISOR']),
            'department': random.choice(['RETAIL', 'OPERATIONS', 'IT', 'RISK', 'COMPLIANCE']),
            'hire_date': fake.date_between(start_date='-10y', end_date='today'),
            'salary': round(random.uniform(30000, 120000), 2),
            'status': random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED'])
        })
    
    return pd.DataFrame(employees)

# 11. Loans table
def generate_loans(contracts_df, n=2000):
    """Generate detailed loan information"""
    loans = []
    
    # Filter only loan contracts
    loan_contracts = contracts_df[contracts_df['product_id'].isin([4, 5, 6, 12])]
    
    if len(loan_contracts) > n:
        loan_contracts = loan_contracts.sample(n)
    
    for i, (_, contract) in enumerate(loan_contracts.iterrows(), 1):
        loans.append({
            'loan_id': i,
            'contract_id': contract['contract_id'],
            'loan_amount': contract['contract_amount'],
            'outstanding_balance': round(contract['contract_amount'] * random.uniform(0.1, 0.9), 2),
            'interest_rate': contract['interest_rate'],
            'term_months': random.randint(12, 360),
            'remaining_months': random.randint(1, 60),
            'next_payment_date': fake.date_between(start_date='today', end_date='+30d'),
            'next_payment_amount': contract['monthly_payment'],
            'delinquency_status': random.choice(['CURRENT', 'DELINQUENT_30', 'DELINQUENT_60', 'DELINQUENT_90+']),
            'collateral_value': round(contract['contract_amount'] * random.uniform(1.1, 1.5), 2),
            'loan_to_value_ratio': round(random.uniform(0.6, 0.9), 2)
        })
    
    return pd.DataFrame(loans)

# 12. Credit applications table
def generate_credit_applications(clients_df, n=1500):
    """Generate credit application data"""
    applications = []
    
    for i in range(1, n + 1):
        client = clients_df.sample(1).iloc[0]
        
        application_date = fake.date_between(
            start_date=client['registration_date'], 
            end_date='today'
        )
        
        applications.append({
            'application_id': i,
            'client_id': client['client_id'],
            'application_date': application_date,
            'requested_amount': random.randint(1000, 50000),
            'approved_amount': random.randint(0, 50000) if random.random() < 0.7 else 0,
            'purpose': random.choice(['CAR', 'HOME', 'EDUCATION', 'BUSINESS', 'PERSONAL', 'MEDICAL']),
            'status': random.choice(['APPROVED', 'REJECTED', 'PENDING', 'CANCELLED']),
            'decision_date': application_date + timedelta(days=random.randint(1, 30)),
            'interest_rate_proposed': round(random.uniform(5.0, 25.0), 2),
            'reason_for_rejection': fake.sentence() if random.random() < 0.3 else None,
            'officer_id': random.randint(1, 100)
        })
    
    return pd.DataFrame(applications)

# Generate all tables
print("Generating clients...")
clients_df = generate_clients(3000)

print("Generating products...")
products_df = generate_products()

print("Generating contracts...")
contracts_df = generate_contracts(clients_df, products_df, 6000)

print("Generating accounts...")
accounts_df = generate_accounts(clients_df, contracts_df, 8000)

print("Generating client-product relationships...")
client_products_df = generate_client_products(clients_df, products_df, 10000)

print("Generating transactions...")
transactions_df = generate_transactions(accounts_df, 25000)

print("Generating account balances...")
balances_df = generate_account_balances(accounts_df, transactions_df)

print("Generating cards...")
cards_df = generate_cards(clients_df, accounts_df, 4000)

print("Generating branches...")
branches_df = generate_branches(50)

print("Generating employees...")
employees_df = generate_employees(branches_df, 500)

print("Generating loans...")
loans_df = generate_loans(contracts_df, 2000)

print("Generating credit applications...")
applications_df = generate_credit_applications(clients_df, 1500)

# Save to CSV files
print("Saving data to CSV files...")
clients_df.to_csv('clients.csv', index=False)
products_df.to_csv('products.csv', index=False)
contracts_df.to_csv('contracts.csv', index=False)
accounts_df.to_csv('accounts.csv', index=False)
client_products_df.to_csv('client_products.csv', index=False)
transactions_df.to_csv('transactions.csv', index=False)
balances_df.to_csv('account_balances.csv', index=False)
cards_df.to_csv('cards.csv', index=False)
branches_df.to_csv('branches.csv', index=False)
employees_df.to_csv('employees.csv', index=False)
loans_df.to_csv('loans.csv', index=False)
applications_df.to_csv('credit_applications.csv', index=False)

print("Data generation completed!")
print("\nGenerated tables with row counts:")
print(f"  Clients: {len(clients_df)}")
print(f"  Products: {len(products_df)}")
print(f"  Contracts: {len(contracts_df)}")
print(f"  Accounts: {len(accounts_df)}")
print(f"  Client-Products: {len(client_products_df)}")
print(f"  Transactions: {len(transactions_df)}")
print(f"  Account Balances: {len(balances_df)}")
print(f"  Cards: {len(cards_df)}")
print(f"  Branches: {len(branches_df)}")
print(f"  Employees: {len(employees_df)}")
print(f"  Loans: {len(loans_df)}")
print(f"  Credit Applications: {len(applications_df)}")