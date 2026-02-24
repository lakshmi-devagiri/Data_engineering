

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaJsonConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for JSON payload
json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("generated_at_utc", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("aadhar", StringType(), True),
    StructField("pan", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("tags", ArrayType(StringType()), True)
])

# Read streaming data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "indsa") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and select fields
parsed_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")) \
    .select("data.*")

# Write parsed data to console
parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()