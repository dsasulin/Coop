-- Анализ клиентской базы и финансовых показателей
SELECT 
    risk_category,
    employment_status,
    COUNT(*) as client_count,
    ROUND(AVG(credit_score), 2) as avg_credit_score,
    ROUND(AVG(annual_income), 2) as avg_annual_income,
    ROUND(SUM(annual_income), 2) as total_annual_income,
    COUNT(CASE WHEN credit_score > 700 THEN 1 END) as high_credit_clients,
    ROUND(COUNT(CASE WHEN credit_score > 700 THEN 1 END) * 100.0 / COUNT(*), 2) as high_credit_percentage
FROM clients
GROUP BY risk_category, employment_status
ORDER BY risk_category, total_annual_income DESC;

-- Ежемесячный анализ транзакционной активности
SELECT 
    YEAR(transaction_date) as year,
    MONTH(transaction_date) as month,
    category,
    transaction_type,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount,
    COUNT(DISTINCT from_account_id) as unique_accounts
FROM transactions
WHERE status = 'COMPLETED'
GROUP BY YEAR(transaction_date), MONTH(transaction_date), category, transaction_type
ORDER BY year DESC, month DESC, total_amount DESC;

-- Эффективность банковских продуктов
SELECT 
    p.product_name,
    p.product_type,
    COUNT(DISTINCT c.client_id) as client_count,
    COUNT(DISTINCT cnt.contract_id) as contract_count,
    ROUND(SUM(cnt.contract_amount), 2) as total_contract_amount,
    ROUND(AVG(cnt.contract_amount), 2) as avg_contract_amount,
    ROUND(SUM(cnt.monthly_payment), 2) as total_monthly_payments,
    COUNT(DISTINCT a.account_id) as account_count
FROM products p
LEFT JOIN contracts cnt ON p.product_id = cnt.product_id
LEFT JOIN clients c ON cnt.client_id = c.client_id
LEFT JOIN accounts a ON cnt.contract_id = a.contract_id
WHERE p.active = true
GROUP BY p.product_name, p.product_type
ORDER BY total_contract_amount DESC;

-- Детальный анализ кредитов и рисков
SELECT 
    l.delinquency_status,
    COUNT(*) as loan_count,
    ROUND(SUM(l.loan_amount), 2) as total_loan_amount,
    ROUND(SUM(l.outstanding_balance), 2) as total_outstanding,
    ROUND(AVG(l.interest_rate), 2) as avg_interest_rate,
    ROUND(SUM(l.outstanding_balance) * 100.0 / SUM(l.loan_amount), 2) as outstanding_percentage,
    ROUND(AVG(l.loan_to_value_ratio), 3) as avg_ltv,
    COUNT(CASE WHEN l.outstanding_balance > l.loan_amount * 0.8 THEN 1 END) as high_risk_loans
FROM loans l
JOIN contracts c ON l.contract_id = c.contract_id
GROUP BY l.delinquency_status
ORDER BY total_outstanding DESC;

-- Распределение средств по типам счетов
SELECT 
    a.account_type,
    a.status,
    COUNT(*) as account_count,
    ROUND(SUM(b.current_balance), 2) as total_balance,
    ROUND(AVG(b.current_balance), 2) as avg_balance,
    ROUND(MIN(b.current_balance), 2) as min_balance,
    ROUND(MAX(b.current_balance), 2) as max_balance,
    ROUND(SUM(b.credit_limit), 2) as total_credit_limit,
    COUNT(CASE WHEN b.current_balance < 0 THEN 1 END) as negative_balance_count
FROM accounts a
JOIN account_balances b ON a.account_id = b.account_id
WHERE a.status = 'ACTIVE'
GROUP BY a.account_type, a.status
ORDER BY total_balance DESC;

-- Сравнение показателей по отделениям
SELECT 
    b.branch_code,
    b.city,
    b.state,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(DISTINCT c.client_id) as total_clients,
    COUNT(DISTINCT e.employee_id) as total_employees,
    ROUND(SUM(ab.current_balance), 2) as total_deposits,
    ROUND(AVG(ab.current_balance), 2) as avg_account_balance,
    COUNT(DISTINCT card.card_id) as cards_issued,
    ROUND(SUM(t.amount), 2) as total_transaction_volume
FROM branches b
LEFT JOIN accounts a ON b.branch_code = a.branch_code
LEFT JOIN clients c ON a.client_id = c.client_id
LEFT JOIN employees e ON b.branch_id = e.branch_id
LEFT JOIN account_balances ab ON a.account_id = ab.account_id
LEFT JOIN cards card ON a.account_id = card.account_id
LEFT JOIN transactions t ON a.account_id = t.from_account_id AND t.status = 'COMPLETED'
GROUP BY b.branch_code, b.city, b.state
ORDER BY total_deposits DESC;

-- Статистика по кредитным заявкам и approval rate
SELECT 
    purpose,
    status,
    COUNT(*) as application_count,
    ROUND(AVG(requested_amount), 2) as avg_requested_amount,
    ROUND(AVG(approved_amount), 2) as avg_approved_amount,
    ROUND(SUM(requested_amount), 2) as total_requested,
    ROUND(SUM(approved_amount), 2) as total_approved,
    ROUND(COUNT(CASE WHEN status = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) as approval_rate,
    ROUND(AVG(interest_rate_proposed), 2) as avg_proposed_rate
FROM credit_applications
GROUP BY purpose, status
ORDER BY purpose, application_count DESC;

-- Активность клиентов по месяцам
SELECT 
    YEAR(t.transaction_date) as year,
    MONTH(t.transaction_date) as month,
    COUNT(DISTINCT t.from_account_id) as active_accounts,
    COUNT(DISTINCT c.client_id) as active_clients,
    COUNT(*) as total_transactions,
    ROUND(SUM(t.amount), 2) as transaction_volume,
    ROUND(AVG(t.amount), 2) as avg_transaction_size,
    COUNT(DISTINCT t.category) as unique_categories
FROM transactions t
JOIN accounts a ON t.from_account_id = a.account_id
JOIN clients c ON a.client_id = c.client_id
WHERE t.status = 'COMPLETED'
GROUP BY YEAR(t.transaction_date), MONTH(t.transaction_date)
ORDER BY year DESC, month DESC;

-- Сегментация клиентов по рискам и доходности
SELECT 
    c.risk_category,
    c.employment_status,
    CASE 
        WHEN c.annual_income < 50000 THEN 'Low Income'
        WHEN c.annual_income BETWEEN 50000 AND 100000 THEN 'Medium Income'
        ELSE 'High Income'
    END as income_segment,
    COUNT(*) as client_count,
    ROUND(AVG(c.credit_score), 2) as avg_credit_score,
    ROUND(AVG(c.annual_income), 2) as avg_income,
    COUNT(DISTINCT a.account_id) as accounts_per_client,
    ROUND(AVG(ab.current_balance), 2) as avg_balance,
    COUNT(DISTINCT l.loan_id) as total_loans,
    COUNT(CASE WHEN l.delinquency_status LIKE 'DELINQUENT%' THEN 1 END) as delinquent_loans
FROM clients c
LEFT JOIN accounts a ON c.client_id = a.client_id
LEFT JOIN account_balances ab ON a.account_id = ab.account_id
LEFT JOIN contracts cnt ON c.client_id = cnt.client_id
LEFT JOIN loans l ON cnt.contract_id = l.contract_id
GROUP BY c.risk_category, c.employment_status, 
    CASE 
        WHEN c.annual_income < 50000 THEN 'Low Income'
        WHEN c.annual_income BETWEEN 50000 AND 100000 THEN 'Medium Income'
        ELSE 'High Income'
    END
ORDER BY c.risk_category, avg_income DESC;

-- Анализ карточных продуктов и транзакций
SELECT 
    card.card_type,
    card.card_level,
    card.status as card_status,
    COUNT(DISTINCT card.card_id) as card_count,
    COUNT(DISTINCT card.client_id) as unique_clients,
    ROUND(AVG(c.credit_score), 2) as avg_client_credit_score,
    ROUND(SUM(ab.credit_limit), 2) as total_credit_limit,
    ROUND(SUM(ab.current_balance), 2) as total_balance,
    COUNT(DISTINCT t.transaction_id) as total_transactions,
    ROUND(SUM(CASE WHEN t.status = 'COMPLETED' THEN t.amount ELSE 0 END), 2) as total_spent,
    ROUND(AVG(CASE WHEN t.status = 'COMPLETED' THEN t.amount END), 2) as avg_transaction_amount,
    COUNT(DISTINCT t.category) as spending_categories
FROM cards card
JOIN clients c ON card.client_id = c.client_id
JOIN accounts a ON card.account_id = a.account_id
JOIN account_balances ab ON a.account_id = ab.account_id
LEFT JOIN transactions t ON a.account_id = t.from_account_id
GROUP BY card.card_type, card.card_level, card.status
ORDER BY card.card_type, total_spent DESC;

-- Data Quality Check - для демонстрации profiling capabilities
SELECT 
    'clients' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT client_id) as unique_ids,
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_emails,
    SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phones,
    SUM(CASE WHEN credit_score < 300 OR credit_score > 850 THEN 1 ELSE 0 END) as invalid_credit_scores,
    ROUND(AVG(credit_score), 2) as avg_credit_score
FROM clients
UNION ALL
SELECT 
    'transactions' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT transaction_id) as unique_ids,
    SUM(CASE WHEN amount IS NULL OR amount <= 0 THEN 1 ELSE 0 END) as invalid_amounts,
    SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) as missing_dates,
    SUM(CASE WHEN status NOT IN ('COMPLETED', 'PENDING', 'FAILED', 'CANCELLED') THEN 1 ELSE 0 END) as invalid_status,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions;

