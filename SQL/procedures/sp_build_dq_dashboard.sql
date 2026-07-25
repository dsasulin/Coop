-- ============================================================================
-- Procedure: sp_build_dq_dashboard
-- Layer: Gold
-- Reads: silver.dq_results, bronze.clients (count), bronze.accounts (count),
--        silver.clients (count), silver.accounts (count)
-- Writes: gold.data_quality_dashboard
-- Calls: none
-- ============================================================================

INSERT OVERWRITE TABLE gold.data_quality_dashboard

WITH record_counts AS (
    -- Current record counts per table for new/updated/deleted tracking
    SELECT 'bronze.clients'  AS table_name, 'bronze' AS database_name, COUNT(*) AS cnt FROM bronze.clients
    UNION ALL
    SELECT 'bronze.accounts', 'bronze', COUNT(*) FROM bronze.accounts
    UNION ALL
    SELECT 'silver.clients', 'silver', COUNT(*) FROM silver.clients
    UNION ALL
    SELECT 'silver.accounts', 'silver', COUNT(*) FROM silver.accounts
    UNION ALL
    SELECT 'silver.transactions', 'silver', COUNT(*) FROM silver.transactions
    UNION ALL
    SELECT 'silver.contracts', 'silver', COUNT(*) FROM silver.contracts
    UNION ALL
    SELECT 'silver.loans', 'silver', COUNT(*) FROM silver.loans
),

dq_agg AS (
    -- Aggregate dq_results by table_name for current day
    SELECT
        table_name,
        COUNT(*)                                           AS total_checks,
        AVG(pass_rate)                                     AS avg_pass_rate,
        SUM(failed_records)                                AS total_failed,
        MAX(total_records)                                 AS total_records_dq,
        SUM(CASE WHEN severity = 'CRITICAL' AND pass_rate < 1.0 THEN 1 ELSE 0 END) AS critical_issues,
        SUM(CASE WHEN severity = 'MAJOR'    AND pass_rate < 1.0 THEN 1 ELSE 0 END) AS major_issues,
        SUM(CASE WHEN severity NOT IN ('CRITICAL', 'MAJOR') AND pass_rate < 1.0 THEN 1 ELSE 0 END) AS minor_issues
    FROM silver.dq_results
    WHERE check_date = CURRENT_DATE()
    GROUP BY table_name
)

SELECT
    -- 1. report_date
    CURRENT_DATE()                              AS report_date,
    -- 2. table_name
    dq.table_name,
    -- 3. database_name
    COALESCE(rc.database_name,
        CASE
            WHEN dq.table_name LIKE 'bronze.%' THEN 'bronze'
            WHEN dq.table_name LIKE 'silver.%' THEN 'silver'
            ELSE 'unknown'
        END
    )                                           AS database_name,
    -- 4. total_records
    COALESCE(rc.cnt, dq.total_records_dq)       AS total_records,
    -- 5. new_records_today
    CAST(0 AS BIGINT)                           AS new_records_today,
    -- 6. updated_records_today
    CAST(0 AS BIGINT)                           AS updated_records_today,
    -- 7. deleted_records_today
    CAST(0 AS BIGINT)                           AS deleted_records_today,
    -- 8. complete_records
    COALESCE(rc.cnt, dq.total_records_dq) - dq.total_failed AS complete_records,
    -- 9. incomplete_records
    dq.total_failed                             AS incomplete_records,
    -- 10. completeness_rate
    CAST(dq.avg_pass_rate AS DECIMAL(10,4))     AS completeness_rate,
    -- 11. duplicate_records
    CAST(0 AS BIGINT)                           AS duplicate_records,
    -- 12. duplicate_rate
    CAST(0.0 AS DECIMAL(10,4))                  AS duplicate_rate,
    -- 13. null_critical_fields
    dq.total_failed                             AS null_critical_fields,
    -- 14. invalid_format_records
    CAST(0 AS BIGINT)                           AS invalid_format_records,
    -- 15. data_quality_score
    CAST(dq.avg_pass_rate * 100 AS DECIMAL(10,2)) AS data_quality_score,
    -- 16. data_quality_grade
    CASE
        WHEN dq.avg_pass_rate >= 0.95 THEN 'A'
        WHEN dq.avg_pass_rate >= 0.85 THEN 'B'
        WHEN dq.avg_pass_rate >= 0.75 THEN 'C'
        WHEN dq.avg_pass_rate >= 0.60 THEN 'D'
        ELSE 'F'
    END                                         AS data_quality_grade,
    -- 17. critical_issues
    dq.critical_issues,
    -- 18. major_issues
    dq.major_issues,
    -- 19. minor_issues
    dq.minor_issues,
    -- 20. issue_details
    CONCAT(
        'Checks: ', CAST(dq.total_checks AS STRING),
        ', Avg pass rate: ', CAST(ROUND(dq.avg_pass_rate * 100, 2) AS STRING), '%',
        ', Failed records: ', CAST(dq.total_failed AS STRING)
    )                                           AS issue_details,
    -- 21. created_timestamp
    CURRENT_TIMESTAMP()                         AS created_timestamp

FROM dq_agg dq
LEFT JOIN record_counts rc
    ON dq.table_name = rc.table_name;
