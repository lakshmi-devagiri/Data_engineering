from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "ec2-3-110-196-13.ap-south-1.compute.amazonaws.com:9092") \
  .option("subscribe", "june27").load()

ndf=df.selectExpr("CAST(value AS STRING)")
ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

#res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

