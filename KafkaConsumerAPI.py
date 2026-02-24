from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "aug31") \
  .load()

schema = ""
res = df.selectExpr("CAST(value AS STRING)")\
  .select(from_json("value", schema).alias("data")).select("data.*")
print("Inferred Schema:")
schema.printTreeString()
#res.writeStream.outputMode("append").format("console").start().awaitTermination()
def foreach_batch_function(df, epoch_id):
    sfOptions = {
        "sfURL": "xrdjflh-igb12300.snowflakecomputing.com",
        "sfUser": "CHARANCHEEDETI",
        "sfPassword": "SFpassword.1",
        "sfDatabase": "charandb",
        "sfSchema": "public",
        "sfWarehouse": "COMPUTE_WH"
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "livedatafromkafka").save()

    # Transform and write batchDF
    pass

res.writeStream.outputMode("append").format("console").start().awaitTermination()
#res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()



