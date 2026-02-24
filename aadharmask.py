# python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("aadharmask").master("local[*]").getOrCreate()
path = r"C:\bigdata\drivers\aadharpancarddata.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
# Normalize headers
normalized_columns = [c.strip().replace(" ", "").lower() for c in df.columns]
df = df.toDF(*normalized_columns)

required_cols = ["age", "aadharcardnumber", "pancard", "email"]
missing = [col for col in required_cols if col not in df.columns]
if missing:
    raise ValueError(f"Missing columns after normalization: {missing}")

df_masked = (
    df
    .withColumn("age_masked", F.when(F.col("age").isNotNull(), F.lit("**")).otherwise(F.col("age")))
    .withColumn("aadhar_clean", F.regexp_replace(F.col("aadharcardnumber"), r"\D", ""))
    .withColumn("aadhar_masked", F.expr("concat(repeat('*', greatest(length(aadhar_clean)-4, 0)), right(aadhar_clean, 4))"))
    .withColumn("pan_masked", F.expr("concat(repeat('*', greatest(length(pancard)-2, 0)), right(pancard, 2))"))
    .withColumn("email_local", F.regexp_extract(F.col("email"), r'([^@]+)@(.+)', 1))
    .withColumn("email_domain", F.regexp_extract(F.col("email"), r'([^@]+)@(.+)', 2))
    .withColumn(
        "email_masked",
        F.when(
            (F.col("email_local").isNotNull()) & (F.length(F.col("email_local")) > 2),
            F.concat(
                F.substring(F.col("email_local"), 1, 2),
                F.expr("repeat('*', greatest(length(email_local) - 2, 0))"),
                F.lit("@"),
                F.col("email_domain")
            )
        ).otherwise(F.col("email"))
    )
    .select(
        *[c for c in df.columns if c not in required_cols],
        "age_masked", "aadhar_masked", "pan_masked", "email_masked"
    )
)

df_masked.show(truncate=False)
df.show()