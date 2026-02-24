from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import re
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "oct15").load()
df.printSchema()
df=df.selectExpr( "CAST(value AS STRING)")
res=df.withColumn("name", split(col("value"),",")[0]) \
    .withColumn("age", split(col("value"),",")[1]) \
    .withColumn("city", split(col("value"),",")[2]).drop("value")

#res.writeStream.outputMode("append").format("console").start().awaitTermination()
sfOptions = {
  "sfURL" : "upzwliu-vq36736.snowflakecomputing.com",
  "sfUser" : "patelmohit1987",
  "sfPassword" : "January-01-2000",
  "sfDatabase" : "mohitdb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH",
  "sfRole" : "Accountadmin"
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


# --- helper to turn any city into a safe table name like "mastable" ---
def _sanitize(s: str) -> str:
    # lower, trim, keep [a-z0-9_], then add "table" suffix
    s = re.sub(r'[^a-z0-9_]', '_', s.strip().lower())
    if not s:
        s = "unknown"
    return f"{s}table"

def foreach_batch_function(df, epoch_id):
    # normalize city once per batch
    dfn = (df
           .withColumn("city", F.lower(F.trim(F.col("city"))))
           .filter(F.col("city").isNotNull() & (F.col("city") != "")))

    # get distinct cities in this micro-batch
    cities = [r["city"] for r in dfn.select("city").distinct().collect()]

    for city in cities:
        table = _sanitize(city)     # e.g., "mas" -> "mastable"
        df_city = dfn.filter(F.col("city") == city)
        (df_city.write
               .mode("append")
               .format(SNOWFLAKE_SOURCE_NAME)
               .options(**sfOptions)
               .option("dbtable", table)
               .save())
        print(f"[epoch {epoch_id}] wrote {df_city.count()} rows to {table}")

# res = your streaming DataFrame with columns: name, age, city
# Example:
# res = spark.readStream.format("...").load(...)

(res.writeStream
    .outputMode("append")
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", "/tmp/ckpt_city_to_sf")  # set a durable path
    .start()
    .awaitTermination())
'''
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.show()
    print("s working")
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "venulivedata").save()

res.writeStream.foreachBatch(foreach_batch_function).outputMode("append").start().awaitTermination()
'''