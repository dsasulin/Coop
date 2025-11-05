# Banking Data Platform - Пилотный проект Cloudera

Проект демонстрирует современную архитектуру Data Lakehouse с использованием Cloudera Data Platform (CDP) на AWS для банковской предметной области.

## Архитектура

Проект реализует **Medallion Architecture** (Bronze-Silver-Gold) для управления данными:

```
┌─────────────────┐
│   Stage (test)  │  ← Исходные данные
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  ← Сырые данные "как есть" + метаданные
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  ← Очищенные, валидированные данные
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  ← Агрегаты, витрины, бизнес-метрики
└─────────────────┘
```

### Слои данных

1. **Stage (test schema)** - Таблицы для первоначальной загрузки данных
2. **Bronze** - Raw data с минимальной обработкой, технические метаданные
3. **Silver** - Cleaned data с применением бизнес-правил и валидацией
4. **Gold** - Aggregated data, dimensions, facts, аналитические витрины

## Структура проекта

```
Coop/
│
├── DDL/                              # SQL скрипты создания таблиц
│   ├── Create_Tables.sql            # Stage таблицы (test schema)
│   ├── 01_Create_Bronze_Layer.sql   # Bronze layer DDL
│   ├── 02_Create_Silver_Layer.sql   # Silver layer DDL
│   └── 03_Create_Gold_Layer.sql     # Gold layer DDL
│
├── Spark/                            # PySpark ETL jobs
│   ├── stage_to_bronze.py           # Stage → Bronze
│   ├── bronze_to_silver.py          # Bronze → Silver (cleaning, validation)
│   └── silver_to_gold.py            # Silver → Gold (aggregation)
│
├── Airflow/                          # Airflow DAGs
│   └── dags/
│       └── banking_etl_pipeline.py  # Оркестрация ETL процесса
│
├── Data/                             # Тестовые данные
│   ├── clients.csv                  # Клиенты
│   ├── accounts.csv                 # Счета
│   ├── transactions.csv             # Транзакции
│   ├── products.csv                 # Продукты
│   ├── contracts.csv                # Договоры
│   ├── account_balances.csv         # Остатки
│   ├── cards.csv                    # Карты
│   ├── branches.csv                 # Филиалы
│   ├── employees.csv                # Сотрудники
│   ├── loans.csv                    # Кредиты
│   ├── credit_applications.csv      # Заявки на кредит
│   └── quality_test/                # Данные с проблемами качества
│       ├── clients_bad_quality.csv
│       ├── transactions_bad_quality.csv
│       └── accounts_bad_quality.csv
│
├── Generator/                        # Утилиты генерации данных
│   ├── generate_banking_data.py     # Генератор тестовых данных
│   └── generate_bad_quality_data.py # Генератор данных с проблемами
│
├── SQL/                              # Аналитические SQL запросы
│   └── Analyse_Queries.sql          # Примеры аналитических запросов
│
└── CLOUDERA_SETUP_GUIDE.md          # Руководство по настройке
```

## Модель данных

### Основные сущности

- **Clients** (Клиенты) - Физические лица
- **Products** (Продукты) - Банковские продукты
- **Contracts** (Договоры) - Договоры клиентов на продукты
- **Accounts** (Счета) - Банковские счета
- **Transactions** (Транзакции) - Финансовые операции
- **Account Balances** (Остатки) - Текущие балансы счетов
- **Cards** (Карты) - Банковские карты
- **Branches** (Филиалы) - Отделения банка
- **Employees** (Сотрудники) - Персонал банка
- **Loans** (Кредиты) - Кредитные продукты
- **Credit Applications** (Заявки) - Заявки на кредит

### Связи между таблицами

```
Clients (1) ────┬──── (N) Accounts
                │
                ├──── (N) Contracts
                │
                ├──── (N) Client_Products
                │
                └──── (N) Credit_Applications

Accounts (1) ───┬──── (N) Transactions
                │
                ├──── (1) Account_Balances
                │
                └──── (N) Cards

Contracts (1) ──┬──── (N) Accounts
                │
                └──── (N) Loans

Branches (1) ───┬──── (N) Accounts
                │
                └──── (N) Employees
```

## Технологический стек

- **Cloudera Data Platform (CDP)** - Платформа для работы с большими данными
- **Apache Hive** - Хранилище данных, метаданные
- **Apache Spark** - Обработка данных (ETL)
- **Apache Airflow** - Оркестрация пайплайнов
- **Hue** - Web UI для работы с данными
- **Apache NiFi** - Data Flow (опционально)
- **AWS S3** - Объектное хранилище
- **Python** - Язык программирования для ETL

## Быстрый старт

### 1. Подготовка окружения

```bash
# Клонируйте репозиторий
git clone <repo-url>
cd Coop

# Сгенерируйте тестовые данные (опционально)
python3 Generator/generate_banking_data.py

# Сгенерируйте данные с проблемами качества
python3 Generator/generate_bad_quality_data.py
```

### 2. Создание структуры в Hive

Откройте Hue и выполните SQL скрипты по порядку:

```sql
-- 1. Создайте stage таблицы
-- Выполните DDL/Create_Tables.sql

-- 2. Создайте Bronze layer
-- Выполните DDL/01_Create_Bronze_Layer.sql

-- 3. Создайте Silver layer
-- Выполните DDL/02_Create_Silver_Layer.sql

-- 4. Создайте Gold layer
-- Выполните DDL/03_Create_Gold_Layer.sql

-- 5. Проверьте создание
SHOW DATABASES;
USE bronze;
SHOW TABLES;
```

### 3. Загрузка данных

**Через Hue Importer:**
1. Откройте Hue → Importer
2. Загрузите CSV файлы из папки Data/
3. Укажите destination: `test.<table_name>`

**Или через LOAD DATA (если данные в S3):**
```sql
USE test;
LOAD DATA INPATH 's3a://your-bucket/data/clients.csv'
OVERWRITE INTO TABLE clients;
```

### 4. Настройка Spark Jobs в CDE

1. Откройте Cloudera Data Engineering
2. Создайте Resource с Python файлами:
   - `stage_to_bronze.py`
   - `bronze_to_silver.py`
   - `silver_to_gold.py`

3. Создайте три Job'а для каждого скрипта

### 5. Настройка Airflow

1. Загрузите DAG в CDE:
   - `banking_etl_pipeline.py`

2. Откройте Airflow UI
3. Включите DAG `banking_etl_pipeline`
4. Запустите первый run

## Процесс ETL

### Stage → Bronze

```python
# Spark Job: stage_to_bronze.py
# - Читает данные из test schema
# - Добавляет технические поля (load_timestamp, source_file)
# - Сохраняет в bronze слой
```

Особенности:
- Сохранение всех данных "как есть"
- Добавление метаданных загрузки
- Партиционирование транзакций по году/месяцу

### Bronze → Silver

```python
# Spark Job: bronze_to_silver.py
# - Удаление дубликатов
# - Очистка и нормализация данных
# - Валидация бизнес-правил
# - Стандартизация форматов
# - Вычисление derived fields
# - Оценка качества данных (DQ score)
```

Применяемые трансформации:
- Нормализация email, телефонов
- Стандартизация статусов, категорий
- Расчет возраста, tenure, и других метрик
- Категоризация (возрастные группы, доходы, кредитные рейтинги)
- Маскировка чувствительных данных (номера карт)
- Флаги качества и подозрительных операций

### Silver → Gold

```python
# Spark Job: silver_to_gold.py
# - Построение dimensions (SCD Type 2 ready)
# - Создание facts (агрегированные метрики)
# - Построение бизнес-витрин (Client 360, Product Performance)
```

Создаваемые объекты:
- **Dimensions**: dim_client, dim_product, dim_branch, dim_date
- **Facts**: fact_transactions_daily, fact_account_balance_daily, fact_loan_performance
- **Business Views**: client_360_view, product_performance_summary, branch_performance_dashboard

## Мониторинг и проверка качества

### Проверка данных по слоям

```sql
-- Количество записей по слоям
SELECT 'bronze' as layer, COUNT(*) FROM bronze.clients
UNION ALL
SELECT 'silver' as layer, COUNT(*) FROM silver.clients
UNION ALL
SELECT 'gold' as layer, COUNT(*) FROM gold.dim_client;

-- Качество данных в Silver
SELECT
    AVG(dq_score) as avg_quality,
    MIN(dq_score) as min_quality,
    COUNT(*) as total_records,
    SUM(CASE WHEN dq_score < 0.8 THEN 1 ELSE 0 END) as low_quality_count
FROM silver.clients;

-- Client 360 View
SELECT * FROM gold.client_360_view
WHERE client_segment = 'VIP'
LIMIT 10;
```

### Мониторинг в Airflow

1. Откройте Airflow UI
2. Проверьте статус DAG `banking_etl_pipeline`
3. Посмотрите логи каждой задачи
4. Проверьте метрики выполнения

### Мониторинг в CDE

1. Откройте CDE UI
2. Перейдите в Job Runs
3. Проверьте статус и логи Spark jobs
4. Посмотрите метрики потребления ресурсов

## Тестирование качества данных

### Загрузка тестовых данных с проблемами

```sql
-- Создайте временную таблицу
USE test;
CREATE TABLE clients_bad_quality LIKE clients;

-- Загрузите данные с проблемами
LOAD DATA LOCAL INPATH 'Data/quality_test/clients_bad_quality.csv'
INTO TABLE clients_bad_quality;

-- Запустите ETL и проверьте результаты в Silver
-- В Silver должны быть низкие DQ scores и заполнены dq_issues
```

### Проверка обработки проблем

```sql
-- Проблемные записи в Silver
SELECT
    client_id,
    full_name,
    dq_score,
    dq_issues
FROM silver.clients
WHERE dq_score < 0.8
ORDER BY dq_score ASC;

-- Статистика по проблемам
SELECT
    dq_issues,
    COUNT(*) as issue_count
FROM silver.clients
WHERE dq_score < 1.0
GROUP BY dq_issues
ORDER BY issue_count DESC;
```

## Расширение функциональности

### Добавление новой таблицы

1. Создайте DDL во всех слоях (bronze, silver, gold)
2. Обновите Spark jobs для обработки новой таблицы
3. Добавьте новую задачу в Airflow DAG
4. Протестируйте на тестовых данных

### Добавление новых метрик в Gold

1. Обновите DDL в `03_Create_Gold_Layer.sql`
2. Добавьте логику расчета в `silver_to_gold.py`
3. Перезапустите ETL

### Настройка Data Quality Rules

1. Обновите логику в `bronze_to_silver.py`
2. Добавьте новые проверки в функцию расчета dq_score
3. Добавьте описание проблем в dq_issues

## Производительность

### Оптимизация Spark Jobs

- Используйте партиционирование для больших таблиц
- Настройте `spark.sql.adaptive.enabled=true`
- Используйте broadcast joins для маленьких таблиц
- Кэшируйте часто используемые датафреймы

### Оптимизация Hive таблиц

```sql
-- Включите статистику
ANALYZE TABLE bronze.clients COMPUTE STATISTICS;
ANALYZE TABLE bronze.clients COMPUTE STATISTICS FOR COLUMNS;

-- Используйте bucketing для часто джойнимых таблиц
CREATE TABLE silver.clients_bucketed
CLUSTERED BY (client_id) INTO 32 BUCKETS
AS SELECT * FROM silver.clients;

-- Используйте ORC вместо Parquet для OLAP
-- (в данном проекте используется Parquet для совместимости с S3)
```

### Мониторинг производительности

```sql
-- Размеры таблиц
SELECT
    table_name,
    num_rows,
    total_size
FROM (
    SELECT 'bronze.clients' as table_name, COUNT(*) as num_rows FROM bronze.clients
) t;

-- Партиции транзакций
SHOW PARTITIONS silver.transactions;

-- Статистика по партициям
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) as record_count
FROM silver.transactions
GROUP BY transaction_year, transaction_month;
```

## Дополнительная документация

- [CLOUDERA_SETUP_GUIDE.md](./CLOUDERA_SETUP_GUIDE.md) - Подробное руководство по настройке Cloudera
- [DDL/](./DDL/) - SQL скрипты создания таблиц
- [SQL/Analyse_Queries.sql](./SQL/Analyse_Queries.sql) - Примеры аналитических запросов

## Roadmap

- [ ] Добавить инкрементальную загрузку
- [ ] Реализовать SCD Type 2 для dimensions
- [ ] Добавить CDC (Change Data Capture)
- [ ] Интегрировать с системой мониторинга (Grafana)
- [ ] Добавить автоматические Data Quality checks
- [ ] Реализовать Data Lineage tracking
- [ ] Добавить ML модели для fraud detection
- [ ] Создать BI дашборды (Tableau/PowerBI)

## FAQ

**Q: Как часто запускается ETL?**
A: По умолчанию ежедневно в 2:00 UTC. Настраивается в Airflow DAG.

**Q: Сколько хранятся исторические данные?**
A: В текущей версии все данные хранятся бессрочно. Retention policy можно настроить через HDFS.

**Q: Как добавить нового пользователя?**
A: Через Cloudera Manager → Ranger → Policies

**Q: Как восстановить данные при ошибке?**
A: Все слои сохраняют timestamp загрузки, можно восстановить из bronze или предыдущих партиций.

**Q: Поддерживается ли real-time обработка?**
A: В текущей версии батчевая обработка. Для real-time можно добавить Kafka + Spark Streaming.

## Лицензия

Этот проект является пилотным и предназначен для демонстрационных целей.

## Контакты

Для вопросов по проекту обращайтесь к команде Data Engineering.
