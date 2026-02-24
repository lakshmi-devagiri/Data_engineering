from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "sep8topic") \
  .load()
df.printSchema()
df=df.selectExpr("CAST(value AS STRING)")

pattern = r'^(\S+) (\S+) (\S+) \[([0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+\-][0-9]{4})\] ' \
          r'"(\S+) ([^"]*?) (\S+)" (\d{3}) (\S+) "([^"]*)" "([^"]*)"'

df = (
    df.withColumn("host",       regexp_extract("value", pattern, 1))
    .withColumn("ts",         regexp_extract("value", pattern, 4))
    .withColumn("endpoint",   regexp_extract("value", pattern, 6))
    .withColumn("status",     regexp_extract("value", pattern, 8).cast(IntegerType()))
    .withColumn("bytes_raw",  regexp_extract("value", pattern, 9))
    .withColumn("referrer",   regexp_extract("value", pattern, 10))

    .withColumn("bytes", when(col("bytes_raw") == "-", None).otherwise(col("bytes_raw").cast(LongType())))
    # parsed timestamp
    .withColumn("timestamp", to_timestamp(col("ts"), "dd/MMM/yyyy:HH:mm:ss Z"))
    .drop("bytes_raw")
    .drop("value")
)
df=df.na.fill(0)

mshost="jdbc:sqlserver://yashoda.ci9mki4eidd9.us-east-1.rds.amazonaws.com:1433;databaseName=venudb;trustServerCertificate=true;"

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.show()
    df=df.withColumn("ts",current_timestamp())
    (df.write.mode("append").format("jdbc").option("url", mshost)
     .option("dbtable", "kafkalivedata").option("user","admin").option("password", "Mypassword.1").save())

    pass

df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
#df.writeStream.outputMode("append").format("console").start().awaitTermination()
