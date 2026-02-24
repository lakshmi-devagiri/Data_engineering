from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").config("spark.sql.streaming.forceDeleteTempCheckpointLocation","true").appName("test").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "feb12").option("failOnDataLoss", "false").load()

ndf=df.selectExpr("CAST(value AS STRING)")
#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

#schema = spark.read.json(ndf.select("value").rdd.map(lambda x: x.value)).schema

sch = StructType([
    StructField("results", StringType()),
    StructField("nationality", StringType()),
    StructField("seed", StringType()),
    StructField("version", StringType())
])

res=ndf.select(from_json(col("value"), sch).alias("data")).select("data.*")
#res.writeStream.outputMode("append").format("console").start().awaitTermination()

#res= ndf.withColumn("parsed_value", from_json(col("value"), sch)).select("parsed_value.*")
def foreach_batch_function(df, epoch_id):
  # Transform and write batchDF
  df1=df.withColumn("ts",current_timestamp())
  sfOptions = {
    "sfURL": "vbalnbx-su38244.snowflakecomputing.com",
    "sfUser": "pgireeshkrishna",
    "sfPassword": "Gireesh123@#",
    "sfDatabase": "gireeshdb",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
  }

  SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  df1.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","livenifidata").save()

  pass


res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
#res.writeStream.outputMode("append").format("console").start().awaitTermination()