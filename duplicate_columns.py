# python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("aadharmask").master("local[*]").getOrCreate()
data = r"C:\bigdata\drivers\aadharpancarddata.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(data)

# Normalize headers
normalized_columns = [c.strip().replace(" ", "").lower() for c in df.columns]
df = df.toDF(*normalized_columns)

# Check for required column
if "aadharcardnumber" not in df.columns:
    raise ValueError("Column 'aadharcardnumber' not found after normalization. Found columns: {}".format(df.columns))

df2 = (
    df
    .withColumn("aadhar_clean", F.regexp_replace(F.col("aadharcardnumber"), r"\s+", ""))
    .withColumn("aadhar_clean", F.regexp_replace(F.col("aadhar_clean"), r"\D", ""))
    .withColumn(
        "aadhar_masked",
        F.expr("concat(repeat('*', greatest(length(aadhar_clean)-4, 0)), right(aadhar_clean, 4))")
    )
    .withColumn("email_local", F.when(F.col("email").isNotNull() & (F.instr(F.col("email"), "@") > 0), F.regexp_extract(F.col("email"), r'([^@]+)@(.+)', 1)))
    .withColumn("email_domain", F.when(F.col("email").isNotNull() & (F.instr(F.col("email"), "@") > 0), F.regexp_extract(F.col("email"), r'([^@]+)@(.+)', 2)))
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
    .drop("aadhar_clean", "email_local", "email_domain")
)

df2.printSchema()
df2.show(truncate=False)