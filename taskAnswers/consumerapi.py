from pyspark.sql import *
from pyspark.sql.functions import *
import re
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "june27").load()
#df=df.selectExpr("CAST(value AS STRING)")
df=df.selectExpr("CAST(value AS STRING) as json_value")

sch = StructType([
    StructField("results", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("seed", StringType(), True),
    StructField("version", StringType(), True)
])

df1=df.select(from_json(col("json_value"), sch).alias("data")).select("data.*")

sfOptions = {
  "sfURL" : "piiuqyk-zn10531.snowflakecomputing.com",
  "sfUser" : "goutamimca",
  "sfPassword" : "SFpassword.1",
  "sfDatabase" : "venudb",
  "sfSchema" : "public",
  "sfWarehouse" : "compute_wh"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df=df.na.drop().withColumn("ts",current_timestamp())
    df.show()
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","sfkafkalivedata").save()
    pass

#df1.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
df1.writeStream.outputMode("append").format("console").start().awaitTermination()

