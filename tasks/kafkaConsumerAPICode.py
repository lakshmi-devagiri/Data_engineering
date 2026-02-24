from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "june3") \
  .load()
df=df.selectExpr("CAST(value AS STRING)")
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'


res = df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))

#res.writeStream.outputMode("append").format("console").start().awaitTermination()
'''def foreach_batch_function(df, epoch_id):
  # Transform and write batchDF
  host="jdbc:mysql://ramdb.c5o0u8e24lo2.us-east-2.rds.amazonaws.com:3306/mysqldb"
  df.write.mode("append").format("jdbc").option("url", host).option("user", "myuser")\
      .option("password","mypassword").option("dbtable", "livelogs").save()

  pass

res.writeStream.foreachBatch(foreach_batch_function).outputMode('append').start().awaitTermination()
'''
sfOptions = {
  "sfURL" : "pjzvebd-op86681.snowflakecomputing.com",
  "sfUser" : "VENUJUSTFORU",
  "sfPassword" : "Venu@500038",
  "sfDatabase" : "VENUDB",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def foreach_batch_function(df, epoch_id):
  # Transform and write batchDF
  df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable","logsinfo").save()

  pass


res.writeStream.foreachBatch(foreach_batch_function).outputMode('append').start().awaitTermination()
