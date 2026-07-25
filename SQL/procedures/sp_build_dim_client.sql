-- ============================================================================
-- Procedure: sp_build_dim_client
-- Layer: Silver -> Gold
-- Reads: silver.clients, silver.accounts, silver.contracts,
--        silver.client_products, silver.cards, silver.loans
-- Writes: gold.dim_client
-- Calls: fn_classify_risk (inline), fn_calc_client_segment (inline)
-- ============================================================================

INSERT OVERWRITE TABLE gold.dim_client
SELECT
    -- 1. client_key
    ROW_NUMBER() OVER (ORDER BY c.client_id)    AS client_key,
    -- 2. client_id
    c.client_id,
    -- 3. first_name
    c.first_name,
    -- 4. last_name
    c.last_name,
    -- 5. full_name
    c.full_name,
    -- 6. email
    c.email,
    -- 7. email_domain
    c.email_domain,
    -- 8. phone
    c.phone,
    -- 9. birth_date
    c.birth_date,
    -- 10. age
    c.age,
    -- 11. age_group
    CASE
        WHEN c.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
        WHEN c.age >= 65              THEN '65+'
        ELSE 'UNKNOWN'
    END                                         AS age_group,
    -- 12. registration_date
    c.registration_date,
    -- 13. customer_tenure_days
    DATEDIFF(CURRENT_DATE(), c.registration_date) AS customer_tenure_days,
    -- 14. customer_tenure_years
    CAST(DATEDIFF(CURRENT_DATE(), c.registration_date) / 365.25
         AS DECIMAL(10,2))                      AS customer_tenure_years,
    -- 15. address
    c.address,
    -- 16. city
    c.city,
    -- 17. country
    c.country,
    -- 18. region
    CASE
        WHEN c.country IN ('US', 'USA', 'United States') THEN
            CASE
                WHEN c.city IN ('New York', 'Boston', 'Philadelphia') THEN 'NORTHEAST'
                WHEN c.city IN ('Miami', 'Atlanta', 'Dallas', 'Houston') THEN 'SOUTH'
                WHEN c.city IN ('Chicago', 'Detroit', 'Minneapolis') THEN 'MIDWEST'
                WHEN c.city IN ('Los Angeles', 'San Francisco', 'Seattle', 'Denver') THEN 'WEST'
                ELSE 'OTHER'
            END
        WHEN c.country IN ('UK', 'GB', 'United Kingdom', 'France', 'Germany', 'Spain') THEN 'WEST'
        WHEN c.country IN ('Brazil', 'Argentina', 'Mexico') THEN 'SOUTH'
        ELSE 'OTHER'
    END                                         AS region,
    -- 19. risk_category: inline fn_classify_risk
    CASE
        WHEN loan_agg.worst_delinquency IN ('DELINQUENT_90', 'DELINQUENT_120_PLUS') THEN 'CRITICAL'
        WHEN loan_agg.worst_delinquency = 'DELINQUENT_60'                            THEN 'HIGH'
        WHEN c.credit_score < 580 OR c.annual_income < 20000                         THEN 'HIGH'
        WHEN loan_agg.worst_delinquency = 'DELINQUENT_30'                            THEN 'MEDIUM'
        WHEN c.credit_score < 670 OR c.annual_income < 40000                         THEN 'MEDIUM'
        WHEN c.credit_score >= 740 AND c.annual_income >= 100000                     THEN 'LOW'
        ELSE 'MODERATE'
    END                                         AS risk_category,
    -- 20. credit_score
    c.credit_score,
    -- 21. credit_score_category
    c.credit_score_category,
    -- 22. employment_status
    c.employment_status,
    -- 23. annual_income
    c.annual_income,
    -- 24. income_category
    c.income_category,
    -- 25. client_segment: inline fn_calc_client_segment
    CASE
        WHEN c.annual_income > 150000 AND c.credit_score > 750 THEN 'VIP'
        WHEN c.annual_income > 100000 AND c.credit_score > 700 THEN 'PREMIUM'
        WHEN c.annual_income > 50000  AND c.credit_score > 650 THEN 'REGULAR'
        ELSE 'BASIC'
    END                                         AS client_segment,
    -- 26. client_lifetime_value
    CAST(
        COALESCE(c.annual_income, 0) *
        CAST(DATEDIFF(CURRENT_DATE(), c.registration_date) / 365.25 AS DECIMAL(10,2)) *
        0.05
    AS DECIMAL(18,2))                           AS client_lifetime_value,
    -- 27. total_accounts
    COALESCE(acct.total_accounts, 0)            AS total_accounts,
    -- 28. total_active_accounts
    COALESCE(acct.total_active_accounts, 0)     AS total_active_accounts,
    -- 29. total_products
    COALESCE(prod.total_products, 0)            AS total_products,
    -- 30. total_contracts
    COALESCE(contr.total_contracts, 0)          AS total_contracts,
    -- 31. total_cards
    COALESCE(card.total_cards, 0)               AS total_cards,
    -- 32. total_loans
    COALESCE(loan_agg.total_loans, 0)           AS total_loans,
    -- 33. effective_date
    CURRENT_DATE()                              AS effective_date,
    -- 34. end_date
    CAST(NULL AS DATE)                          AS end_date,
    -- 35. is_current
    CAST(TRUE AS BOOLEAN)                       AS is_current,
    -- 36. created_timestamp
    CURRENT_TIMESTAMP()                         AS created_timestamp,
    -- 37. updated_timestamp
    CURRENT_TIMESTAMP()                         AS updated_timestamp

FROM silver.clients c

-- Aggregate accounts
LEFT JOIN (
    SELECT
        client_id,
        COUNT(*)                                           AS total_accounts,
        SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS total_active_accounts
    FROM silver.accounts
    GROUP BY client_id
) acct
    ON c.client_id = acct.client_id

-- Aggregate contracts
LEFT JOIN (
    SELECT client_id, COUNT(*) AS total_contracts
    FROM silver.contracts
    GROUP BY client_id
) contr
    ON c.client_id = contr.client_id

-- Aggregate products
LEFT JOIN (
    SELECT client_id, COUNT(DISTINCT product_id) AS total_products
    FROM silver.client_products
    GROUP BY client_id
) prod
    ON c.client_id = prod.client_id

-- Aggregate cards
LEFT JOIN (
    SELECT client_id, COUNT(*) AS total_cards
    FROM silver.cards
    GROUP BY client_id
) card
    ON c.client_id = card.client_id

-- Aggregate loans with worst delinquency
LEFT JOIN (
    SELECT
        ct.client_id,
        COUNT(*)                AS total_loans,
        MAX(l.delinquency_status) AS worst_delinquency
    FROM silver.loans l
    JOIN silver.contracts ct
        ON l.contract_id = ct.contract_id
    GROUP BY ct.client_id
) loan_agg
    ON c.client_id = loan_agg.client_id;
