"""
Utilities package for Banking ETL.
"""
from .config import Config, get_config
from .logger import get_logger, StructuredLogger
from .error_handler import (
    ETLError,
    ErrorHandler,
    ConfigurationError,
    DataQualityError,
    DataLoadError,
    DataTransformError,
    SchemaValidationError,
    handle_etl_errors,
    ETLErrorContext
)
from .retry import (
    retry_with_backoff,
    retry_on_transient_errors,
    retry_spark_operation,
    safe_execute
)
from .spark_utils import (
    create_spark_session,
    load_table_generic,
    load_sql_transform,
    get_table_count,
    validate_schema,
    optimize_dataframe,
    write_dataframe,
    analyze_table_statistics
)

__all__ = [
    # Config
    'Config',
    'get_config',
    # Logger
    'get_logger',
    'StructuredLogger',
    # Error handling
    'ETLError',
    'ErrorHandler',
    'ConfigurationError',
    'DataQualityError',
    'DataLoadError',
    'DataTransformError',
    'SchemaValidationError',
    'handle_etl_errors',
    'ETLErrorContext',
    # Retry
    'retry_with_backoff',
    'retry_on_transient_errors',
    'retry_spark_operation',
    'safe_execute',
    # Spark utilities
    'create_spark_session',
    'load_table_generic',
    'load_sql_transform',
    'get_table_count',
    'validate_schema',
    'optimize_dataframe',
    'write_dataframe',
    'analyze_table_statistics'
]
