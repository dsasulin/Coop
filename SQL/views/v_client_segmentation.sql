-- View: reporting.v_client_segmentation
-- Layer: reporting
-- Sources: gold.dim_client
-- Purpose: Client segmentation distribution for marketing

CREATE VIEW reporting.v_client_segmentation AS
SELECT
    client_segment,
    age_group,
    region,
    income_category,
    COUNT(*) AS client_count,
    AVG(credit_score) AS avg_credit_score,
    AVG(annual_income) AS avg_annual_income,
    SUM(client_lifetime_value) AS total_lifetime_value
FROM gold.dim_client
WHERE is_current = true
GROUP BY client_segment, age_group, region, income_category;
