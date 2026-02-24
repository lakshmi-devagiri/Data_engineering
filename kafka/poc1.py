"""ETL pipeline (Bronze / Silver / Gold) for CSV data using PySpark.

This script implements a small, production-oriented template that reads CSV files,
stores a raw copy (bronze), performs cleaning and deduplication (silver), and
creates business-ready aggregates (gold).

Usage (example):
  set CSV_INPUT_PATH=D:\bigdata\drivers\bank-full.csv
  set BRONZE_PATH=D:\bigdata\lake\bronze
  set SILVER_PATH=D:\bigdata\lake\silver
  set GOLD_PATH=D:\bigdata\lake\gold
  python poc1.py

Configuration can be provided via environment variables. For cluster runs,
prefer using spark-submit and providing master/configuration via spark-submit options.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os
import re
import logging
from typing import Optional, List

# -- Configuration and logging -------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Defaults (override with environment variables)
DEFAULT_INPUT_PATH = r"D:\bigdata\drivers\bank-full.csv"
DEFAULT_BRONZE = r"D:\bigdata\lake\bronze"
DEFAULT_SILVER = r"D:\bigdata\lake\silver"
DEFAULT_GOLD = r"D:\bigdata\lake\gold"

# Precompiled regexes
_NON_ALNUM = re.compile(r"[^0-9A-Za-z_]")
_COLLAPSE_UNDERSCORES = re.compile(r"_+")


# -- Utilities ----------------------------------------------------------------
def normalize_column_name(name: str) -> str:
    """Return a cleaned column name suitable for dataframes / downstream systems.

    - Replace non-alphanumeric characters with underscore
    - Collapse multiple underscores
    - Strip leading/trailing underscores
    - Lowercase
    """
    if name is None:
        return name
    col = _NON_ALNUM.sub("_", name)
    col = _COLLAPSE_UNDERSCORES.sub("_", col)
    return col.strip("_").lower()


def detect_delimiter(file_path: str, sample_size: int = 8192) -> str:
    """Detect likely CSV delimiter by sampling the first part of the file.

    Checks common delimiters and returns the one with highest occurrence; falls back to comma.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(sample_size)
            if not sample:
                return ","
            candidates = {",": sample.count(","), ";": sample.count(";"), "\t": sample.count("\t"), "|": sample.count("|")}
            delimiter, count = max(candidates.items(), key=lambda kv: kv[1])
            return delimiter if count > 0 else ","
    except Exception as e:
        logger.exception("Delimiter detection failed for %s: %s", file_path, e)
        return ","


def compute_partitions(file_path: str, bytes_per_partition: int = 128 * 1024 * 1024, max_partitions: int = 200) -> int:
    """Determine a reasonable number of partitions for writing based on file size."""
    try:
        size = os.path.getsize(file_path)
        parts = max(1, min(max_partitions, (size // bytes_per_partition) + 1))
        return int(parts)
    except Exception:
        return 1


# -- Data processing building blocks ------------------------------------------

def read_csv_to_df(spark: SparkSession, file_path: str, header: bool = True, infer_schema: bool = False) -> 'DataFrame':
    """Read CSV into a DataFrame with a detected delimiter and optional schema inference.

    Keep infer_schema=False in production for speed; provide explicit schema where possible.
    """
    sep = detect_delimiter(file_path)
    logger.info("Reading %s with delimiter '%s' (infer_schema=%s)", file_path, sep, infer_schema)
    df = spark.read.format("csv").option("header", str(header).lower()).option("sep", sep)
    if infer_schema:
        df = df.option("inferSchema", "true")
    return df.load(file_path)


def normalize_columns_df(df: 'DataFrame') -> 'DataFrame':
    """Normalize column names in-place (return a new DataFrame with renamed columns)."""
    new_names = [normalize_column_name(c) for c in df.columns]
    return df.toDF(*new_names)


def trim_and_nullify(df: 'DataFrame') -> 'DataFrame':
    """Trim string columns and convert empty strings to nulls.

    Non-string columns are left unchanged.
    """
    exprs = []
    for fld in df.schema.fields:
        name = fld.name
        if isinstance(fld.dataType, T.StringType):
            # trim and replace empty string with null
            exprs.append(F.when(F.trim(F.col(name)) == "", F.lit(None)).otherwise(F.trim(F.col(name))).alias(name))
        else:
            exprs.append(F.col(name))
    return df.select(*exprs)


def drop_all_null_rows(df: 'DataFrame') -> 'DataFrame':
    """Drop rows that are completely null across all columns."""
    # build condition: at least one column is not null
    not_null_cond = F.coalesce(*[F.col(c) for c in df.columns]).isNotNull()
    return df.filter(not_null_cond)


def write_parquet(df: 'DataFrame', path: str, mode: str = 'overwrite', partitions: Optional[List[str]] = None) -> None:
    """Write DataFrame to Parquet. Optionally partition by given columns."""
    writer = df.write.mode(mode).parquet
    # If partitions provided, use partitionBy
    if partitions:
        df.write.mode(mode).partitionBy(*partitions).parquet(path)
    else:
        df.write.mode(mode).parquet(path)
    logger.info("Wrote parquet to %s (mode=%s)%s", path, mode, f" partitioned by {partitions}" if partitions else "")


# -- Layered pipeline ---------------------------------------------------------

def bronze_layer(spark: SparkSession, input_path: str, bronze_path: str) -> str:
    """Read raw CSV and persist as Parquet in the bronze layer. Returns written path."""
    df_raw = read_csv_to_df(spark, input_path, header=True, infer_schema=False)
    df_raw = normalize_columns_df(df_raw)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    out_path = os.path.join(bronze_path, base_name)
    # Use a single file partition heuristic: avoid tiny files but keep it simple for local runs
    write_parquet(df_raw, out_path, mode='overwrite')
    return out_path


def silver_layer(spark: SparkSession, bronze_input_path: str, silver_path: str) -> str:
    """Clean data read from bronze, remove duplicates, and write to silver layer."""
    logger.info("Loading bronze data from %s", bronze_input_path)
    df = spark.read.parquet(bronze_input_path)
    # Trim strings and convert blanks to null
    df = trim_and_nullify(df)
    # Drop rows that are entirely null
    df = drop_all_null_rows(df)
    # Deduplicate: simple strategy - drop exact duplicate rows
    df = df.dropDuplicates()
    base_name = os.path.basename(bronze_input_path)
    out_path = os.path.join(silver_path, base_name)
    write_parquet(df, out_path, mode='overwrite')
    return out_path


def gold_layer(spark: SparkSession, silver_input_path: str, gold_path: str) -> str:
    """Create business-friendly aggregates / features and write to gold layer.

    This example creates a simple conversion rate by job (for the bank dataset):
    - total_count
    - subscribed_count (y == 'yes')
    - conversion_rate
    """
    logger.info("Loading silver data from %s", silver_input_path)
    df = spark.read.parquet(silver_input_path)

    # Ensure the key columns exist
    if 'job' not in df.columns or 'y' not in df.columns:
        logger.warning("Expected columns 'job' and 'y' not found in silver dataset; writing empty gold result")
        out_path = os.path.join(gold_path, os.path.basename(silver_input_path))
        spark.createDataFrame([], schema=T.StructType([T.StructField('job', T.StringType()), T.StructField('total_count', T.LongType()), T.StructField('subscribed_count', T.LongType()), T.StructField('conversion_rate', T.DoubleType())])).write.mode('overwrite').parquet(out_path)
        return out_path

    agg = (
        df.groupBy('job')
        .agg(
            F.count(F.lit(1)).alias('total_count'),
            F.sum(F.when(F.lower(F.col('y')) == 'yes', 1).otherwise(0)).alias('subscribed_count')
        )
        .withColumn('conversion_rate', F.col('subscribed_count') / F.col('total_count'))
        .orderBy(F.desc('conversion_rate'))
    )

    out_path = os.path.join(gold_path, os.path.basename(silver_input_path))
    write_parquet(agg, out_path, mode='overwrite')
    return out_path


# -- Entrypoint ----------------------------------------------------------------

def main(input_path: Optional[str] = None, bronze: Optional[str] = None, silver: Optional[str] = None, gold: Optional[str] = None):
    """Run the Bronze -> Silver -> Gold pipeline on a single CSV input file."""
    input_path = input_path or os.environ.get('CSV_INPUT_PATH', DEFAULT_INPUT_PATH)
    bronze = bronze or os.environ.get('BRONZE_PATH', DEFAULT_BRONZE)
    silver = silver or os.environ.get('SILVER_PATH', DEFAULT_SILVER)
    gold = gold or os.environ.get('GOLD_PATH', DEFAULT_GOLD)

    if not os.path.exists(input_path):
        logger.error("Input path does not exist: %s", input_path)
        return

    # Prepare output directories
    os.makedirs(bronze, exist_ok=True)
    os.makedirs(silver, exist_ok=True)
    os.makedirs(gold, exist_ok=True)

    # Create SparkSession (allow overriding master with SPARK_MASTER env var)
    master = os.environ.get('SPARK_MASTER', 'local[*]')
    spark = SparkSession.builder.appName('etl-bronze-silver-gold').master(master).getOrCreate()

    try:
        # Bronze
        bronze_out = bronze_layer(spark, input_path, bronze)

        # Silver
        silver_out = silver_layer(spark, bronze_out, silver)

        # Gold
        gold_out = gold_layer(spark, silver_out, gold)

        logger.info("Pipeline complete. Bronze: %s; Silver: %s; Gold: %s", bronze_out, silver_out, gold_out)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
