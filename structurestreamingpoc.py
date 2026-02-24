from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

df = (spark.readStream.format("socket") \
    .option("host", "ec2-13-232-187-192.ap-south-1.compute.amazonaws.com")\
    .option("port", 1111).load())
res=df.withColumn("name", split(col("value"),",")[0])\
    .withColumn("age", split(col("value"),",")[1])\
    .withColumn("city", split(col("value"),",")[2]).drop("value")
res1=res.where(col("city")=="hyd")

res.writeStream.outputMode("append").format("console").start().awaitTermination()