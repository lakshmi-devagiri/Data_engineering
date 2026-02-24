from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re

# ---------- your helpers ----------
spark = (SparkSession.builder
         .appName("KafkaJsonFlatten")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data_sample = [("1","2025-07-01 12:01:19.111"),
               ("2","2025-06-24 12:01:19.222"),
               ("3","2025-11-16 16:44:55.406"),
               ("4","2025-11-16 16:50:59.406")]

df = spark.createDataFrame(data=data_sample, schema=["id","from_timestamp"])

#  Adding columns to convert 'from_timestamp' to timestamp, add current timestamp and calculate difference in seconds
df2 = df.withColumn('from_timestamp', to_timestamp(col('from_timestamp')))\
        .withColumn('end_timestamp', current_timestamp())\
        .withColumn('DiffInSeconds', col("end_timestamp").cast("long") - col('from_timestamp').cast("long"))

df2.show(truncate=False)

# Performing the same operations using unix_timestamp functions to calculate the difference in seconds
df.withColumn('from_timestamp', to_timestamp(col('from_timestamp')))\
  .withColumn('end_timestamp', current_timestamp())\
  .withColumn('DiffInSeconds', unix_timestamp("end_timestamp") - unix_timestamp('from_timestamp')) \
  .show(truncate=False)

# Calculating the difference in minutes using the difference in seconds calculated earlier
df2.withColumn('DiffInMinutes', round(col('DiffInSeconds') / 60)).show(truncate=False)

# Calculando a diferença em horas usando a diferença de segundos calculada anteriormente
df2.withColumn('DiffInHours', round(col('DiffInSeconds') / 3600)).show(truncate=False)

data_sample2 = [("12:01:19.000", "13:01:19.000"),
               ("12:01:19.000", "12:02:19.000"),
               ("16:44:55.406", "17:44:55.406"),
               ("16:50:59.406", "16:44:59.406")]

# Criando um DataFrame a partir dos dados especificados com as colunas 'from_timestamp' e 'to_timestamp'
df3 = spark.createDataFrame(data = data_sample2, schema = ["from_timestamp","to_timestamp"])

# Converting 'from_timestamp' and 'to_timestamp' to timestamp and calculating difference in seconds, minutes and hours
df3.withColumn("from_timestamp", to_timestamp(col("from_timestamp"), "HH:mm:ss.SSS")) \
   .withColumn("to_timestamp", to_timestamp(col("to_timestamp"), "HH:mm:ss.SSS")) \
   .withColumn("DiffInSeconds", col("from_timestamp").cast("long") - col("to_timestamp").cast("long")) \
   .withColumn("DiffInMinutes", round(col("DiffInSeconds") / 60)) \
   .withColumn("DiffInHours", round(col("DiffInSeconds") / 3600)) \
   .show(truncate=False)

# Creating a DataFrame with timestamp data to calculate differences with the current timestamp
df3 = spark.createDataFrame(data = [("1","07-01-2019 12:01:19.406")], schema = ["id","input_timestamp"])

# Converting 'input_timestamp' to timestamp, adding current timestamp and calculating difference in seconds, minutes, hours and days
df3.withColumn("input_timestamp", to_timestamp(col("input_timestamp"), "MM-dd-yyyy HH:mm:ss.SSS")) \
    .withColumn("current_timestamp", current_timestamp().alias("current_timestamp")) \
    .withColumn("DiffInSeconds", current_timestamp().cast("long") - col("input_timestamp").cast("long")) \
    .withColumn("DiffInMinutes", round(col("DiffInSeconds") / 60)) \
    .withColumn("DiffInHours", round(col("DiffInSeconds") / 3600)) \
    .withColumn("DiffInDays", round(col("DiffInSeconds") / (24*3600))) \
    .show(truncate=False)

# Running SQL queries to calculate the difference in seconds, minutes and hours between two specific timestamps
spark.sql("select unix_timestamp('2019-07-02 12:01:19') - unix_timestamp('2019-07-01 12:01:19') as DiffInSeconds").show()
spark.sql("select (unix_timestamp('2019-07-02 12:01:19') - unix_timestamp('2019-07-01 12:01:19')) / 60 as DiffInMinutes").show()
spark.sql("select (unix_timestamp('2019-07-02 12:01:19') - unix_timestamp('2019-07-01 12:01:19')) / 3600 as DiffInHours").show()


# TIMESTAMP
print('\nTimestamp:\n')

data_sample3 = [("2024-07-01 12:01:19",
                "07-01-2024 12:01:19",
                "07-01-2024")]

cols = ["timestamp_1", "timestamp_2", "timestamp_3"]
df = spark.createDataFrame(data = data_sample3, schema = cols)
df.printSchema()
df.show(truncate=False)

# Selecting columns from DataFrame and converting them to Unix timestamp format
df2 = df.select(
    unix_timestamp(col("timestamp_1")).alias("timestamp_1"),
    unix_timestamp(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
    unix_timestamp(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
    unix_timestamp().alias("timestamp_4"))

df2.printSchema()
df2.show(truncate=False)

# Converting Unix timestamp values ​​back to human-readable date format, using different formats
df3 = df2.select(
    from_unixtime(col("timestamp_1")).alias("timestamp_1"),
    from_unixtime(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
    from_unixtime(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
    from_unixtime(col("timestamp_4")).alias("timestamp_4"))

df3.printSchema()
df3.show(truncate=False)

#Current date
print('\nDate Timestamp:\n')

spark = SparkSession.builder.appName('currentdate').getOrCreate()


data = [["1"]]
df = spark.createDataFrame(data, ["id"])

# Adding columns with the current date and time to the DataFrame and displaying the result
df.withColumn("current_date", current_date()) \
  .withColumn("current_timestamp", current_timestamp()) \
  .show(truncate=False)

# Executing a SQL query directly to get the current date and time and displaying the result
spark.sql("select current_date(), current_timestamp()").show(truncate=False)

# Adding formatted columns for date and timestamp conversion and displaying the result
df.withColumn("date_format", date_format(current_date(), "MM-dd-yyyy")) \
  .withColumn("to_timestamp", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss SSS")) \
  .show(truncate=False)

# Executing a SQL query to get the formatted date and timestamp conversion and displaying the result
spark.sql("select date_format(current_date(),'MM-dd-yyyy') as date_format ," + \
          "to_timestamp(current_timestamp(),'MM-dd-yyyy HH mm ss SSS') as to_timestamp") \
     .show(truncate=False)
#https://raw.githubusercontent.com/Silmara-Basso/streaming-kafka-spark/2cc11292472dd34c428109783f770437248dcd5d/jobs/time.py