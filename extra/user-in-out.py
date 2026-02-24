from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\office_users_in_out_data.csv"
df = spark.read.format("csv").option("header", "true").load(data)

# Convert user_in and user_out to timestamp
df = df.withColumn("user_in", unix_timestamp(col("user_in"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("user_out", unix_timestamp(col("user_out"), "yyyy-MM-dd HH:mm:ss"))

# Calculate time spent in seconds for each entry
df = df.withColumn("time_spent_seconds", col("user_out") - col("user_in"))

# Aggregate total time spent by each user
total_time_df = df.groupBy("user_id", "user_name") \
    .agg(sum("time_spent_seconds").alias("total_time_spent_seconds"))

# Convert total time spent to hours and minutes
total_time_df = total_time_df.withColumn(
    "total_time_spent_hours", (col("total_time_spent_seconds") / 3600)
)

# Show the result
total_time_df.show()