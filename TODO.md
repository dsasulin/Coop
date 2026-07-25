# TODO: привести документацию в соответствие с реальностью

Только правки *.md. Код/SQL/конфиги не трогаем. Все пункты выполнены.

- [x] README.md — структура, счёт dims/facts/aggregates, dim_account, NiFi/Kafka/Informatica, FAQ, раздел статуса
- [x] FINAL_REPORT.md — credentials, Production Ready, покрытие 65.27%, разбивка тестов, Known blocking issues, заглушки
- [x] REFACTORING_SUMMARY.md — покрытие >70% → 65.27%, разбивка тестов
- [x] SPARK_AIRFLOW_SUMMARY.md — 4/3/3 gold, 7 джобов, снят Production Ready, расхождение схемы с SQL
- [x] config/*.md (5) + docker/README.md — несуществующие scripts/, yaml не читается кодом, K8s внешний, docker без Kafka/HDFS
- [x] CLOUDERA_SETUP_GUIDE.md — реальные имена файлов spark_jobs/01..05, актуальный airflow_dags/
- [x] DEPLOYMENT_GUIDE.md — 4 spark-джоба + Informatica + SQL, нерабочий rollback, поправлены номера строк
- [x] DEMO_INSTRUCTIONS.md — несуществующие ML/Scripts файлы, demo-пометки для #3-20, пустой accounts_bad_quality
- [x] SQL/README_SQL_ETL.md + SQL/FIXES_SUMMARY.md — procedures/views, схема reporting не создаётся, снят бейдж Tested
- [x] Финальная проверка grep + git diff (изменения только в *.md; правок кода не вносилось)
