from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.sparkContext

path=r"C:\bigdata\nifilogs\00030543-7ac7-4f90-b082-47def42c17fa"
myschema = spark.read.format("json").option("multiLine","true").load(path).schema
# Define the main schema for the entire JSON data

def read_nested_json(df):
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



df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "dec14").load()

df=df.selectExpr("CAST(value AS STRING)")

#data cleaning ...
df1=df.select(from_json("value",myschema).alias("data")).select(col("data.*"))
ndf = flatten(df1)
#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

sfOptions = {
  "sfURL" : "yvjyafv-bz92894.snowflakecomputing.com",
  "sfUser" : "SREYOBHILASHIIT",
  "sfPassword" : "Dec-13-2023",
  "sfDatabase" : "VENUDB",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def export2mysql(df, epoch_id):
    # Transform and write batchDF
    df=df.withColumn("ts",current_timestamp())
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","nifidec14").save()


    pass

ndf.writeStream.foreachBatch(export2mysql).start().awaitTermination()


