from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("consumer code").getOrCreate()
sc=spark.sparkContext


df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "may13").load()
df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")
ndf.writeStream.outputMode("append").format("console").start().awaitTermination()
'''def read_nested_json(df):
  column_list = []
  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      df = df.withColumn(column_name, explode(column_name))
      column_list.append(column_name)
    elif isinstance(df.schema[column_name].dataType, StructType):
      for field in df.schema[column_name].dataType.fields:
        column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
    else:
      column_list.append(column_name)
  df = df.select(column_list)
  cols = [re.sub('[^a-zA-Z0-9]', "", c.lower()) for c in df.columns]
  df = df.toDF(*cols)
  return df


def flatten(df):
  read_nested_json_flag = True
  while read_nested_json_flag:
    df = read_nested_json(df)
    read_nested_json_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True
  return df;


res = flatten(ndf)
#res.writeStream.outputMode("append").format("console").start().awaitTermination()
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
    .option("dbtable","restapilive").save()

  pass


res.writeStream.foreachBatch(foreach_batch_function).outputMode('append').start().awaitTermination()'''
#https://docs.snowflake.com/en/user-guide/spark-connector-use
#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.11.3-spark_3.1
#https://mvnrepository.com/artifact/net.snowflake/snowflake-ingest-sdk/2.1.0
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.13.29