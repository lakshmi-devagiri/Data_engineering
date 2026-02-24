# ---------- Imports & Spark ----------
from pyspark.sql import SparkSession, functions as F, types as T
import re

spark = (SparkSession.builder
         .appName("auto_date_detection")
         .master("local[*]")
         .getOrCreate())

spark.conf.set("spark.sql.session.timeZone", "IST")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------- Paths ----------
donations_path = r"D:\bigdata\drivers\donations.csv"
tzdiff_path    = r"D:\bigdata\drivers\date_differ_timezone.csv"

donations_df = (spark.read.option("header", True).option("inferSchema", True).csv(donations_path))
tzdiff_df    = (spark.read.option("header", True).option("inferSchema", True).csv(tzdiff_path))

# ---------- Common date & timestamp patterns ----------
DATE_FMTS = [
    "yyyy-MM-dd", "dd-MM-yyyy", "d-M-yyyy","dd-MMM-yyyy", "d-MMM-yyyy",
    "MM-dd-yyyy", "M-d-yyyy", "dd/MM/yyyy", "d/M/yyyy", "yyyy/MM/dd"
]
TS_FMTS = [
    "yyyy-MM-dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "d-M-yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss", "yyyy/MM/dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ssXXX"
]

# ---------- Detect by actual value pattern ----------
def looks_like_datetime(sample_val: str):
    """Quick regex-based heuristic."""
    if sample_val is None:
        return False
    val = sample_val.strip()
    # Date: has 2+ separators and digits
    date_like = bool(re.match(r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$", val))
    # Timestamp: has time part
    ts_like = bool(re.match(r".*\d{1,2}:\d{2}(:\d{2})?$", val))
    return date_like or ts_like

def coalesce_parse(col_expr, fmts, as_ts=True):
    exprs = [(F.to_timestamp(col_expr, f) if as_ts else F.to_date(col_expr, f)) for f in fmts]
    return F.coalesce(*exprs)

def auto_convert_datetime(df, date_fmts=DATE_FMTS, ts_fmts=TS_FMTS, sample_rows=20):
    """
    1️⃣ Scans a few sample values from each string column.
    2️⃣ If looks like date/time, tries all known formats.
    3️⃣ Converts column to appropriate type automatically.
    """
    df_out = df
    for col_name, col_type in df.dtypes:
        if col_type not in ("string", "varchar", "char"):
            continue

        # Collect sample values (non-null) to decide
        vals = [r[col_name] for r in df_out.select(col_name).na.drop().limit(sample_rows).collect()]
        if not any(looks_like_datetime(str(v)) for v in vals if v is not None):
            continue  # skip if not date/time-like

        ts_try  = coalesce_parse(F.col(col_name), ts_fmts, as_ts=True)
        date_try = coalesce_parse(F.col(col_name), date_fmts, as_ts=False)

        df_out = df_out.withColumn(
            col_name,
            F.when(ts_try.isNotNull(), ts_try)
             .when(date_try.isNotNull(), date_try)
             .otherwise(F.col(col_name))
        )
        print(f"✅ Converted column '{col_name}' to datetime/timestamp.")
    return df_out

# ---------- Apply ----------
donations_df_conv = auto_convert_datetime(donations_df)
tzdiff_df_conv    = auto_convert_datetime(tzdiff_df)

# ---------- Show results ----------
print("\nDONATIONS SCHEMA:")
donations_df_conv.printSchema()
donations_df_conv.show(truncate=False)

print("\nTIMEZONE SCHEMA:")
tzdiff_df_conv.printSchema()
tzdiff_df_conv.show(truncate=False)
