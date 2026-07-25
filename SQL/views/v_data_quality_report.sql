-- View:    reporting.v_data_quality_report
-- Layer:   Regulatory
-- Sources: gold.data_quality_dashboard
-- Purpose: Data quality monitoring report

CREATE VIEW reporting.v_data_quality_report AS
SELECT
    report_date,
    table_name,
    database_name,
    total_records,
    completeness_rate,
    duplicate_rate,
    data_quality_score,
    data_quality_grade,
    critical_issues,
    major_issues,
    minor_issues
FROM gold.data_quality_dashboard
ORDER BY data_quality_score ASC;
