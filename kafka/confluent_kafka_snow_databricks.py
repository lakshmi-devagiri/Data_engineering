from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
import re
import logging
from typing import Optional

# Precompile regex patterns for performance
_NON_ALNUM = re.compile(r'[^0-9A-Za-z_]')
_COLLAPSE_UNDERSCORES = re.compile(r'_+')
_LEADING_DIGITS = re.compile(r'^(\d+)(.*)$')
_LEADING_LETTER = re.compile(r'^[A-Za-z]')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Snowflake source name
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# Default path; override with CSV_INPUT_PATH environment variable in production
DEFAULT_INPUT_PATH = r"D:\bigdata\drivers\"


def get_sf_options() -> dict:
    """Read Snowflake connection options from environment variables with sensible defaults.

    In production, provide credentials via secure environment variables or a secrets manager.
    """
    return {
        "sfURL": os.environ.get("SF_URL", "upzwliu-vq36736.snowflakecomputing.com"),
        "sfUser": os.environ.get("SF_USER", "patelmohit1987"),
        "sfPassword": os.environ.get("SF_PASSWORD", os.environ.get("PASSWORD", "January-01-2000")),
        "sfDatabase": os.environ.get("SF_DATABASE", "mohitdb"),
        "sfSchema": os.environ.get("SF_SCHEMA", "public"),
        "sfWarehouse": os.environ.get("SF_WAREHOUSE", "COMPUTE_WH"),
        "sfRole": os.environ.get("SF_ROLE", "Accountadmin"),
    }


def clean_column(col_name: str) -> str:
    """Normalize a column name to safe identifier form.

    - Replace non-alphanumeric characters with underscore
    - Collapse consecutive underscores
    - Trim leading/trailing underscores
    - Convert to lowercase
    """
    if col_name is None:
        return col_name
    col = _NON_ALNUM.sub('_', col_name)
    col = _COLLAPSE_UNDERSCORES.sub('_', col)
    return col.strip('_').lower()


def detect_delimiter(file_path: str, sample_size: int = 8192) -> str:
    """Detect the likely delimiter by sampling the beginning of the file.

    Checks common delimiters and returns the one with
    highest count in the sample.
    Falls back to comma if none detected.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.read(sample_size)
            if not sample:
                return ','
            candidates = {',': sample.count(','), ';': sample.count(';'), '\t': sample.count('\t'), '|': sample.count('|')}
            delimiter, count = max(candidates.items(), key=lambda kv: kv[1])
            return delimiter if count > 0 else ','
    except Exception as e:
        logger.exception("Failed to detect delimiter for %s: %s", file_path, e)
        return ','


def fix_table_name(file_name: str) -> str:
    """Convert a filename to a Snowflake-friendly table name.

    - Remove extension
    - Move leading digits to the end
    - Replace invalid chars with underscore
    - Collapse underscores and ensure it starts with a letter (prefix with 't_')
    - Lowercase final result
    """
    base_name = os.path.splitext(os.path.basename(file_name))[0]
    m = _LEADING_DIGITS.match(base_name)
    if m:
        base_name = f"{m.group(2)}{m.group(1)}"
    name = _NON_ALNUM.sub('_', base_name)
    name = _COLLAPSE_UNDERSCORES.sub('_', name).strip('_')
    if not _LEADING_LETTER.match(name or ''):
        name = f"t_{name}"
    return name.lower()


def compute_partitions(file_path: str, bytes_per_partition: int = 128 * 1024 * 1024, max_partitions: int = 200) -> int:
    """Simple heuristic to choose number of partitions based on input file size.

    This helps control write parallelism to Snowflake.
    """
    try:
        size = os.path.getsize(file_path)
        parts = max(1, min(max_partitions, (size // bytes_per_partition) + 1))
        return int(parts)
    except Exception:
        return 1


def process_file(spark: SparkSession, file_path: str, sf_options: dict) -> None:
    """Read CSV, normalize columns/table name and write to Snowflake.

    This function is resilient: it logs errors and raises them so callers can decide retry behavior.
    """
    table_name = fix_table_name(file_path)
    logger.info("Processing %s -> table %s", file_path, table_name)

    delimiter = detect_delimiter(file_path)

    # Read CSV without inferSchema to keep it fast; for production provide a schema if possible
    df = spark.read.option('header', 'true').option('sep', delimiter).csv(file_path)

    # Normalize column names
    new_cols = [clean_column(c) for c in df.columns]
    df = df.toDF(*new_cols)

    # Repartition to reasonable parallelism for writing
    parts = compute_partitions(file_path)
    if parts > 1:
        df = df.repartition(parts)

    # Write to Snowflake; failing fast and letting caller handle retries is preferable
    df.write.mode('overwrite').format(SNOWFLAKE_SOURCE_NAME).options(**sf_options).option('dbtable', table_name).save()
    logger.info("Successfully wrote %s to Snowflake table %s (partitions=%d)", file_path, table_name, parts)


def main(input_path: Optional[str] = None):
    """Main entrypoint: process all CSV files in input_path."""
    input_path = input_path or os.environ.get('CSV_INPUT_PATH', DEFAULT_INPUT_PATH)
    if not os.path.isdir(input_path):
        logger.error("Input path does not exist or is not a directory: %s", input_path)
        return

    files = [f for f in os.listdir(input_path) if f.lower().endswith('.csv')]
    if not files:
        logger.info("No CSV files found in %s", input_path)
        return

    sf_options = get_sf_options()

    # Create SparkSession here so it can be configured externally (e.g., via spark-submit configs)
    master = os.environ.get('SPARK_MASTER', 'local[*]')
    spark = SparkSession.builder.appName('csv-to-snowflake').master(master).getOrCreate()

    for file in files:
        file_path = os.path.join(input_path, file)
        try:
            process_file(spark, file_path, sf_options)
        except Exception:
            logger.exception("Failed to process %s", file_path)

    spark.stop()


if __name__ == '__main__':
    main()
