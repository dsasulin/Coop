#!/usr/bin/env python3
"""
Spark utilities module.
Common Spark functions to eliminate code duplication.
"""
from typing import Optional, List, Dict, Any

# Conditional import of PySpark
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import current_timestamp, lit, col
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None  # type: ignore
    DataFrame = None  # type: ignore

from .config import get_config
from .logger import get_logger
from .retry import retry_spark_operation
from .error_handler import DataLoadError

logger = get_logger(__name__)


def create_spark_session(
    app_name: Optional[str] = None,
    config_overrides: Optional[Dict[str, str]] = None
) -> SparkSession:
    """
    Create Spark session with configuration from environment.

    Args:
        app_name: Application name (defaults from config)
        config_overrides: Optional configuration overrides

    Returns:
        SparkSession: Configured Spark session

    Usage:
        spark = create_spark_session("my_etl_job")
    """
    try:
        config = get_config()

        app_name = app_name or config.spark_app_name

        logger.info(
            f"Creating Spark session: {app_name}",
            app_name=app_name,
            master=config.spark_master
        )

        # Build Spark session
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", config.hive_metastore_uri) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.warehouse.dir", config.hive_warehouse_dir) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", config.spark_driver_memory) \
            .config("spark.driver.cores", config.spark_driver_cores) \
            .config("spark.executor.memory", config.spark_executor_memory) \
            .config("spark.executor.cores", config.spark_executor_cores)

        # Add master if not local
        if config.spark_master != "local[*]":
            builder = builder.master(config.spark_master)

        # Add AWS credentials if provided
        if config.aws_access_key_id and config.aws_secret_access_key:
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id) \
                .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key)

        # Add config overrides
        if config_overrides:
            for key, value in config_overrides.items():
                builder = builder.config(key, value)

        # Enable Hive support
        spark = builder.enableHiveSupport().getOrCreate()

        # Set dynamic partitioning
        spark.sql("SET hive.exec.dynamic.partition=true")
        spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("SET hive.exec.max.dynamic.partitions=1000")
        spark.sql("SET hive.exec.max.dynamic.partitions.pernode=1000")

        # Set checkpoint directory
        spark.sparkContext.setCheckpointDir(config.etl_checkpoint_dir)

        logger.info(
            f"Spark session created successfully",
            spark_version=spark.version,
            metastore=config.hive_metastore_uri
        )

        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise


@retry_spark_operation(max_retries=3)
def load_table_generic(
    spark: SparkSession,
    source_db: str,
    source_table: str,
    target_db: str,
    target_table: str,
    partition_cols: Optional[List[str]] = None,
    transformation_func: Optional[callable] = None,
    add_metadata: bool = True
) -> int:
    """
    Generic table load function to eliminate code duplication.

    Args:
        spark: Spark session
        source_db: Source database name
        source_table: Source table name
        target_db: Target database name
        target_table: Target table name
        partition_cols: Optional partition columns
        transformation_func: Optional transformation function to apply
        add_metadata: Whether to add load metadata

    Returns:
        int: Number of rows loaded

    Usage:
        row_count = load_table_generic(
            spark, "test", "clients", "bronze", "clients",
            transformation_func=lambda df: df.filter(col("client_id").isNotNull())
        )
    """
    logger.info(
        f"Loading table: {source_db}.{source_table} -> {target_db}.{target_table}",
        source=f"{source_db}.{source_table}",
        target=f"{target_db}.{target_table}"
    )

    try:
        # Truncate target table
        spark.sql(f"TRUNCATE TABLE {target_db}.{target_table}")

        # Read source data
        df = spark.table(f"{source_db}.{source_table}")

        # Apply transformation if provided
        if transformation_func:
            df = transformation_func(df)

        # Add metadata if requested
        if add_metadata:
            df = df \
                .withColumn("load_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(f"{source_db}.{source_table}"))

        # Write to target
        writer = df.write.mode("overwrite").format("hive")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(f"{target_db}.{target_table}")

        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_db}.{target_table}").collect()[0]['cnt']

        logger.info(
            f"✅ Table loaded successfully: {target_table}",
            table=target_table,
            row_count=row_count
        )

        return row_count

    except Exception as e:
        logger.error(
            f"Failed to load table {target_table}",
            table=target_table,
            error=str(e)
        )
        raise DataLoadError(
            f"Failed to load {target_table}: {str(e)}",
            job_name=f"load_{target_table}",
            source_table=f"{source_db}.{source_table}",
            target_table=f"{target_db}.{target_table}"
        )


@retry_spark_operation(max_retries=3)
def load_sql_transform(
    spark: SparkSession,
    target_db: str,
    target_table: str,
    sql_query: str,
    partition_cols: Optional[List[str]] = None
) -> int:
    """
    Load table using SQL transformation.

    Args:
        spark: Spark session
        target_db: Target database name
        target_table: Target table name
        sql_query: SQL query for transformation
        partition_cols: Optional partition columns

    Returns:
        int: Number of rows loaded

    Usage:
        row_count = load_sql_transform(
            spark, "silver", "clients",
            "SELECT * FROM bronze.clients WHERE client_id IS NOT NULL"
        )
    """
    logger.info(
        f"Loading table with SQL: {target_db}.{target_table}",
        target=f"{target_db}.{target_table}"
    )

    try:
        # Truncate target table
        spark.sql(f"TRUNCATE TABLE {target_db}.{target_table}")

        # Execute SQL transformation
        if partition_cols:
            spark.sql(f"""
                INSERT INTO TABLE {target_db}.{target_table}
                PARTITION ({', '.join(partition_cols)})
                {sql_query}
            """)
        else:
            spark.sql(f"""
                INSERT INTO TABLE {target_db}.{target_table}
                {sql_query}
            """)

        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_db}.{target_table}").collect()[0]['cnt']

        logger.info(
            f"✅ Table loaded successfully: {target_table}",
            table=target_table,
            row_count=row_count
        )

        return row_count

    except Exception as e:
        logger.error(
            f"Failed to load table {target_table}",
            table=target_table,
            error=str(e)
        )
        raise DataLoadError(
            f"Failed to load {target_table}: {str(e)}",
            job_name=f"load_{target_table}",
            target_table=f"{target_db}.{target_table}"
        )


def get_table_count(spark: SparkSession, database: str, table: str) -> int:
    """
    Get row count for a table.

    Args:
        spark: Spark session
        database: Database name
        table: Table name

    Returns:
        int: Row count
    """
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table}").collect()[0]['cnt']
        return count
    except Exception as e:
        logger.warning(f"Failed to get count for {database}.{table}: {e}")
        return 0


def validate_schema(
    df: DataFrame,
    expected_columns: List[str],
    required_columns: Optional[List[str]] = None
) -> bool:
    """
    Validate DataFrame schema.

    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
        required_columns: List of required column names (subset of expected)

    Returns:
        bool: True if schema is valid

    Raises:
        SchemaValidationError: If schema validation fails
    """
    from .error_handler import SchemaValidationError

    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)

    # Check for missing columns
    missing = expected_columns_set - actual_columns
    if missing:
        raise SchemaValidationError(
            f"Missing columns: {missing}",
            expected_columns=expected_columns,
            actual_columns=list(actual_columns),
            missing_columns=list(missing)
        )

    # Check required columns for nulls
    if required_columns:
        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                raise SchemaValidationError(
                    f"Column {col_name} has {null_count} null values",
                    column=col_name,
                    null_count=null_count
                )

    return True


def optimize_dataframe(
    df: DataFrame,
    repartition: Optional[int] = None,
    coalesce: Optional[int] = None,
    cache: bool = False
) -> DataFrame:
    """
    Optimize DataFrame for performance.

    Args:
        df: DataFrame to optimize
        repartition: Number of partitions (forces shuffle)
        coalesce: Number of partitions (no shuffle)
        cache: Whether to cache the DataFrame

    Returns:
        DataFrame: Optimized DataFrame
    """
    if repartition:
        df = df.repartition(repartition)

    if coalesce:
        df = df.coalesce(coalesce)

    if cache:
        df = df.cache()
        # Trigger caching
        df.count()

    return df


def write_dataframe(
    df: DataFrame,
    database: str,
    table: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    format_type: str = "hive"
) -> int:
    """
    Write DataFrame to table with consistent settings.

    Args:
        df: DataFrame to write
        database: Database name
        table: Table name
        mode: Write mode (overwrite, append)
        partition_by: Partition columns
        format_type: Output format (hive, parquet, delta)

    Returns:
        int: Number of rows written
    """
    logger.info(
        f"Writing DataFrame to {database}.{table}",
        target=f"{database}.{table}",
        mode=mode,
        partitions=partition_by
    )

    try:
        writer = df.write.mode(mode).format(format_type)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.saveAsTable(f"{database}.{table}")

        row_count = df.count()

        logger.info(
            f"✅ DataFrame written successfully",
            table=f"{database}.{table}",
            row_count=row_count
        )

        return row_count

    except Exception as e:
        logger.error(
            f"Failed to write DataFrame to {database}.{table}",
            error=str(e)
        )
        raise


def analyze_table_statistics(spark: SparkSession, database: str, table: str):
    """
    Compute table statistics for query optimization.

    Args:
        spark: Spark session
        database: Database name
        table: Table name
    """
    logger.info(f"Computing statistics for {database}.{table}")

    try:
        # Compute table statistics
        spark.sql(f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS")

        # Compute column statistics
        spark.sql(f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")

        logger.info(f"✅ Statistics computed for {database}.{table}")

    except Exception as e:
        logger.warning(f"Failed to compute statistics for {database}.{table}: {e}")
