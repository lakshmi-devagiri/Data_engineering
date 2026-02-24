from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

'''df=spark.readStream.format("socket") \
    .option("host", "ec2-3-109-32-173.ap-south-1.compute.amazonaws.com") \
    .option("port", 2222).load()
res=df.withColumn("name", split(col("value"),",")[0])\
    .withColumn("age", split(col("value"),",")[1])\
    .withColumn("city", split(col("value"),",")[2]).drop("value")
    
res.writeStream.outputMode("append").format("console").start().awaitTermination()

hyddf=res.where(col("city")=="hyd")'''
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "aug1") \
  .load()
df=df.selectExpr( "CAST(value AS STRING)")
res=df.withColumn("name", split(col("value"),",")[0])\
    .withColumn("age", split(col("value"),",")[1])\
    .withColumn("city", split(col("value"),",")[2]).drop("value")

res.writeStream.outputMode("append").format("console").start().awaitTermination()
