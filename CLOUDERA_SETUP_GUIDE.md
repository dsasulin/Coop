# Руководство по настройке Cloudera Data Platform

## Оглавление
1. [Получение параметров подключения](#получение-параметров-подключения)
2. [Настройка Cloudera Data Engineering (CDE)](#настройка-cloudera-data-engineering)
3. [Настройка Airflow](#настройка-airflow)
4. [Настройка Hue для работы с таблицами](#настройка-hue)
5. [Загрузка данных через Data Flow](#загрузка-данных-через-data-flow)

---

## Получение параметров подключения

### 1. Параметры подключения к Hive через Hue

#### Через интерфейс Hue:

1. **Откройте Hue**
   - Перейдите в ваш Cloudera Data Platform
   - Найдите сервис **Hue** и откройте его

2. **Получите информацию о подключении**
   ```sql
   -- Выполните в редакторе Hive запросы:

   -- Текущая база данных
   SELECT current_database();

   -- Список всех баз данных
   SHOW DATABASES;

   -- Информация о конфигурации Hive
   SET;

   -- HDFS путь к warehouse
   SET hive.metastore.warehouse.dir;

   -- Информация о Metastore
   SET hive.metastore.uris;
   ```

3. **Получите параметры JDBC**
   ```sql
   -- Версия Hive
   SELECT version();

   -- Параметры для JDBC подключения можно найти в:
   -- Cloudera Manager -> Hive -> Configuration -> HiveServer2
   ```

#### Через Cloudera Manager:

1. Откройте **Cloudera Manager**
2. Перейдите в **Clusters** -> Ваш кластер
3. Выберите **Hive** сервис
4. Перейдите в **Configuration**
5. Найдите параметры:
   - `hive.metastore.uris` - адрес Hive Metastore
   - `hive.server2.thrift.port` - порт HiveServer2 (по умолчанию 10000)
   - `hive.metastore.warehouse.dir` - путь к HDFS warehouse

### 2. Параметры подключения для Spark

#### Получение через Cloudera Data Engineering UI:

1. **Откройте CDE (Cloudera Data Engineering)**
   - Перейдите в CDP Home
   - Выберите **Data Engineering**
   - Выберите ваш Virtual Cluster

2. **Получите Spark Submit параметры**
   - В CDE UI перейдите в раздел **Jobs**
   - Нажмите **Create Job**
   - В разделе Spark Configuration увидите доступные параметры:
     ```
     spark.master: yarn
     spark.submit.deployMode: cluster
     spark.dynamicAllocation.enabled: true
     ```

3. **Получите информацию о ресурсах**
   - Перейдите в **Virtual Clusters** -> Ваш кластер
   - Посмотрите доступные ресурсы:
     - CPU Requests
     - Memory Requests
     - Executor instances

#### SQL запросы для получения информации через Hue:

```sql
-- Информация о Spark SQL
SET spark.sql.warehouse.dir;
SET spark.master;

-- Проверка доступных баз данных
SHOW DATABASES;

-- Проверка таблиц в базе test
USE test;
SHOW TABLES;

-- Информация о таблице
DESCRIBE EXTENDED clients;

-- Путь к данным таблицы
DESCRIBE FORMATTED clients;
```

### 3. Параметры для Airflow

#### Получение через Cloudera Data Engineering:

1. **Откройте CDE Virtual Cluster**
2. **Перейдите в Airflow UI**
   - В CDE найдите свой Virtual Cluster
   - Нажмите на иконку **Airflow** (⚙️)
   - Откроется Airflow Web UI

3. **Получите Connection параметры**
   - В Airflow UI перейдите: **Admin** -> **Connections**
   - Найдите существующие подключения:
     - `spark_default` - для Spark jobs
     - `hive_cli_default` - для Hive
     - `aws_default` - для S3 (если используется)

4. **Создайте новое подключение для вашего проекта**
   - Connection Id: `banking_hive_connection`
   - Connection Type: `Hive Server 2 Thrift`
   - Host: `<hiveserver2-host>` (можно получить из CDE или Cloudera Manager)
   - Schema: `bronze` (или другая база по умолчанию)
   - Login: ваш username
   - Port: `10000` (или из Cloudera Manager)

### 4. Параметры AWS S3 (если используется)

#### Через Cloudera Manager или CDP Environment:

1. **Откройте CDP Environments**
2. Найдите **Data Lake** настройки
3. Посмотрите S3 параметры:
   ```
   Bucket: s3://your-cdp-bucket/
   Data Location: s3://your-cdp-bucket/warehouse/
   Logs Location: s3://your-cdp-bucket/logs/
   ```

#### Через Hue/Hive:

```sql
-- Проверка S3 location для таблиц
DESCRIBE FORMATTED bronze.clients;

-- Результат покажет location вида:
-- location: s3a://your-bucket/warehouse/bronze.db/clients
```

---

## Настройка Cloudera Data Engineering

### 1. Создание Virtual Cluster (если еще нет)

1. Откройте **Data Engineering** в CDP
2. Нажмите **Create Virtual Cluster**
3. Заполните параметры:
   - Name: `banking-etl-cluster`
   - Environment: выберите ваш environment
   - CPU Quota: рекомендуется минимум 10 CPU
   - Memory: рекомендуется минимум 40 GB

### 2. Загрузка Spark Jobs в CDE

1. **Создайте Resource для Python файлов**
   ```bash
   # В CDE CLI (если установлен) или через UI
   ```

2. **Через CDE UI:**
   - Перейдите в **Resources**
   - Нажмите **Create Resource**
   - Выберите тип: `Files`
   - Name: `banking-spark-jobs`
   - Upload files:
     - `stage_to_bronze.py`
     - `bronze_to_silver.py`
     - `silver_to_gold.py`

3. **Создайте Spark Jobs**
   - Перейдите в **Jobs**
   - Нажмите **Create Job**
   - Заполните параметры:

**Job 1: Stage to Bronze**
```
Name: stage_to_bronze_job
Type: Spark
Application File: stage_to_bronze.py (из Resources)
Main Class: (оставьте пустым для Python)
Arguments: --execution-date ${execution_date}

Spark Configuration:
spark.executor.memory: 4g
spark.executor.cores: 2
spark.driver.memory: 2g

Resources:
- banking-spark-jobs
```

**Job 2: Bronze to Silver**
```
Name: bronze_to_silver_job
Type: Spark
Application File: bronze_to_silver.py
Arguments: --execution-date ${execution_date}

Spark Configuration:
spark.executor.memory: 4g
spark.executor.cores: 2
spark.driver.memory: 2g

Resources:
- banking-spark-jobs
```

**Job 3: Silver to Gold**
```
Name: silver_to_gold_job
Type: Spark
Application File: silver_to_gold.py
Arguments: --execution-date ${execution_date}

Spark Configuration:
spark.executor.memory: 4g
spark.executor.cores: 2
spark.driver.memory: 2g

Resources:
- banking-spark-jobs
```

---

## Настройка Airflow

### 1. Загрузка DAG в CDE

1. **Создайте Resource для DAG**
   - В CDE UI перейдите в **Resources**
   - Создайте новый Resource типа `Files`
   - Name: `banking-airflow-dags`
   - Upload файл: `banking_etl_pipeline.py`

2. **DAG автоматически подхватится Airflow**
   - Перейдите в Airflow UI
   - Найдите DAG: `banking_etl_pipeline`
   - Включите его (toggle switch)

### 2. Обновление DAG для CDE

Обновите пути в DAG файле:

```python
# В начале файла banking_etl_pipeline.py замените:
SPARK_JOBS_PATH = "/path/to/spark/jobs"

# На:
SPARK_JOBS_PATH = "stage_to_bronze.py"  # CDE использует имена файлов напрямую
```

Для CDE используйте `CDEJobRunOperator` вместо `SparkSubmitOperator`:

```python
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

# Пример задачи
load_stage_to_bronze = CDEJobRunOperator(
    task_id='load_stage_to_bronze',
    job_name='stage_to_bronze_job',  # Имя созданного CDE Job
    variables={
        'execution_date': '{{ ds }}'
    },
)
```

### 3. Настройка расписания

В Airflow UI:
1. Перейдите к DAG `banking_etl_pipeline`
2. Нажмите **Edit** -> **Schedule**
3. Настройте расписание (по умолчанию: ежедневно в 2:00 UTC)

---

## Настройка Hue

### 1. Создание баз данных

Откройте Hue и выполните SQL скрипты:

```sql
-- 1. Создайте базы данных
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Bronze layer - raw data'
LOCATION '/user/hive/warehouse/bronze.db';

CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Silver layer - cleaned data'
LOCATION '/user/hive/warehouse/silver.db';

CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Gold layer - aggregated data'
LOCATION '/user/hive/warehouse/gold.db';

-- 2. Проверьте создание
SHOW DATABASES;

-- 3. Загрузите DDL скрипты из репозитория
-- Скопируйте и выполните содержимое файлов:
-- - DDL/01_Create_Bronze_Layer.sql
-- - DDL/02_Create_Silver_Layer.sql
-- - DDL/03_Create_Gold_Layer.sql
```

### 2. Проверка таблиц

```sql
-- Проверьте таблицы в bronze
USE bronze;
SHOW TABLES;

-- Проверьте структуру таблицы
DESCRIBE FORMATTED clients;

-- Проверьте данные (если загружены)
SELECT * FROM clients LIMIT 10;

-- Проверьте количество записей
SELECT COUNT(*) FROM clients;
```

### 3. Права доступа

```sql
-- Дайте права на базы данных (если нужно)
GRANT ALL ON DATABASE bronze TO USER your_username;
GRANT ALL ON DATABASE silver TO USER your_username;
GRANT ALL ON DATABASE gold TO USER your_username;

-- Проверьте права
SHOW GRANT USER your_username ON DATABASE bronze;
```

---

## Загрузка данных через Data Flow

### Вариант 1: Загрузка через Hue

1. **Создайте таблицы в схеме test** (из DDL/Create_Tables.sql)

2. **Загрузите CSV через Hue**:
   - Откройте Hue
   - Перейдите в **Importer**
   - Выберите файл (например, `Data/clients.csv`)
   - Выберите destination: `test.clients`
   - Настройте delimiter: `,`
   - Нажмите **Submit**

### Вариант 2: Загрузка через NiFi (Cloudera Data Flow)

1. **Откройте DataFlow**
   - В CDP перейдите в **DataFlow**
   - Создайте новый Flow Definition

2. **Создайте простой flow для загрузки CSV**:
   ```
   GetFile -> SplitRecord -> ConvertRecord -> PutHiveStreaming
   ```

3. **Настройте процессоры**:
   - **GetFile**: укажите путь к Data/
   - **SplitRecord**: укажите CSV Reader
   - **ConvertRecord**: CSV -> Avro
   - **PutHiveStreaming**:
     - Hive Metastore URI: (из Cloudera Manager)
     - Database: test
     - Table: clients

### Вариант 3: Загрузка через S3 и LOAD DATA

```sql
-- 1. Загрузите CSV в S3 bucket
-- (через AWS Console или AWS CLI)

-- 2. Загрузите данные в Hive
USE test;

LOAD DATA INPATH 's3a://your-bucket/data/clients.csv'
OVERWRITE INTO TABLE clients;

-- 3. Проверьте загрузку
SELECT COUNT(*) FROM clients;
```

---

## Полный процесс развертывания

### Шаг 1: Подготовка

```bash
# 1. Загрузите весь репозиторий в ваш S3 bucket или локально
git clone <your-repo>
cd Coop

# 2. Загрузите данные в S3 (если используется)
aws s3 cp Data/ s3://your-bucket/banking-data/Data/ --recursive
aws s3 cp Spark/ s3://your-bucket/banking-data/Spark/ --recursive
aws s3 cp Airflow/ s3://your-bucket/banking-data/Airflow/ --recursive
```

### Шаг 2: Создание структуры в Hive

1. Откройте Hue
2. Выполните DDL скрипты в порядке:
   - `DDL/Create_Tables.sql` (для test схемы)
   - `DDL/01_Create_Bronze_Layer.sql`
   - `DDL/02_Create_Silver_Layer.sql`
   - `DDL/03_Create_Gold_Layer.sql`

### Шаг 3: Загрузка данных в test схему

Выберите один из вариантов загрузки (см. раздел "Загрузка данных")

### Шаг 4: Настройка CDE Jobs

1. Создайте Resource в CDE с Spark jobs
2. Создайте три Job'а (stage_to_bronze, bronze_to_silver, silver_to_gold)
3. Протестируйте каждый job вручную

### Шаг 5: Настройка Airflow

1. Загрузите DAG в CDE
2. Обновите DAG для использования CDEJobRunOperator
3. Включите DAG в Airflow UI
4. Запустите первый run вручную

### Шаг 6: Мониторинг

1. Проверьте выполнение в Airflow UI
2. Проверьте логи в CDE UI
3. Проверьте данные в Hue:

```sql
-- Проверка bronze
USE bronze;
SELECT COUNT(*) FROM clients;

-- Проверка silver
USE silver;
SELECT COUNT(*) FROM clients;

-- Проверка gold
USE gold;
SELECT COUNT(*) FROM dim_client;
SELECT COUNT(*) FROM client_360_view;
```

---

## Troubleshooting

### Проблема: Не могу найти Hive Metastore URI

**Решение через Hue:**
```sql
SET hive.metastore.uris;
```

**Решение через Cloudera Manager:**
1. Cloudera Manager -> Hive -> Configuration
2. Search: "metastore.uris"

### Проблема: Spark job не может найти таблицы

**Решение:**
Убедитесь что в Spark используется правильная конфигурация:
```python
spark = SparkSession.builder \
    .appName("your_app") \
    .enableHiveSupport() \  # ВАЖНО!
    .getOrCreate()
```

### Проблема: Ошибки прав доступа

**Решение через Hue:**
```sql
-- Дайте права на все базы
GRANT ALL ON DATABASE test TO USER your_username;
GRANT ALL ON DATABASE bronze TO USER your_username;
GRANT ALL ON DATABASE silver TO USER your_username;
GRANT ALL ON DATABASE gold TO USER your_username;
```

### Проблема: DAG не появляется в Airflow

**Решение:**
1. Проверьте что файл загружен в CDE Resource
2. Проверьте синтаксис DAG файла
3. Проверьте логи Airflow Scheduler в CDE

---

## Полезные SQL запросы для мониторинга

```sql
-- Статистика по всем слоям
SELECT
    'bronze' as layer,
    'clients' as table_name,
    COUNT(*) as record_count
FROM bronze.clients
UNION ALL
SELECT
    'silver' as layer,
    'clients' as table_name,
    COUNT(*) as record_count
FROM silver.clients
UNION ALL
SELECT
    'gold' as layer,
    'dim_client' as table_name,
    COUNT(*) as record_count
FROM gold.dim_client;

-- Проверка качества данных в silver
SELECT
    AVG(dq_score) as avg_dq_score,
    MIN(dq_score) as min_dq_score,
    COUNT(*) as total_records,
    SUM(CASE WHEN dq_score < 0.8 THEN 1 ELSE 0 END) as low_quality_records
FROM silver.clients;

-- Статистика по транзакциям
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM silver.transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year DESC, transaction_month DESC;
```

---

## Дополнительные ресурсы

- [Cloudera Data Engineering Documentation](https://docs.cloudera.com/data-engineering/cloud/)
- [Cloudera Data Flow Documentation](https://docs.cloudera.com/dataflow/cloud/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

---

## Контакты и поддержка

Для вопросов по проекту обращайтесь к команде Data Engineering.
