
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# initialize Spark
spark = SparkSession.builder.appName("birth_timezone_order").master("local[*]").getOrCreate()

# CSV path (example): `D:\bigdata\drivers\date_differ_timezone.csv`
data_path = r"D:\bigdata\drivers\date_differ_timezone.csv"

# read CSV with header and infer schema
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_path)

# build a local timestamp:
# - if `birthTime` already contains a date (e.g. '2025-11-09 13:15:00') parse it directly
# - otherwise combine `dateofbirth` + `birthTime` (e.g. '1988-05-10' + '13:15:00')
local_ts = when(
    instr(col("birthTime"), "-") > 0,
    to_timestamp(col("birthTime"), "yyyy-MM-dd HH:mm:ss")
).otherwise(
    to_timestamp(concat_ws(" ", col("dateofbirth"), col("birthTime")), "yyyy-MM-dd HH:mm:ss")
)

# create UTC instant using the provided timezone (default to 'UTC' if missing)
df2 = df.withColumn("local_timestamp", local_ts) \
        .withColumn("tz", coalesce(col("timezone"), lit("UTC"))) \
        .withColumn("utc_instant", to_utc_timestamp(col("local_timestamp"), col("tz")))

# rank by UTC instant ascending (earliest global birth first)
w = Window.orderBy(col("utc_instant").asc())
result = df2.withColumn("birth_order", row_number().over(w)) \
            .select("birth_order", "name", "email", "country", "timezone", "local_timestamp", "utc_instant") \
            .orderBy("utc_instant")

result.show(truncate=False)

spark.stop()
