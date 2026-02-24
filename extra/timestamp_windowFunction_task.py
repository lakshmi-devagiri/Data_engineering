from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\session_identification_data.csv"
df = spark.read.format("csv").option("header", "true").load(data)
# Sort by timestamp and user
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

window_spec = Window.partitionBy("user_id").orderBy("timestamp")

# Calculate time difference with the previous event
df = df.withColumn("prev_timestamp", lag("timestamp").over(window_spec))
df = df.withColumn("time_diff", (col("timestamp").cast("long") - col("prev_timestamp").cast("long")) / 60)
df = df.filter(col("timestamp").isNotNull())
# Define new sessions where time difference exceeds threshold
df = df.withColumn("new_session", when(col("time_diff") > 30, 1).otherwise(0))
df = df.withColumn("session_id", sum("new_session").over(window_spec))

# TODO: Complete the session ID generation logic
df.show()

