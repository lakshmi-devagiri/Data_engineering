from pyspark.sql.functions import *
from pyspark.sql import *
import re
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import explode, col, from_json, expr
from pyspark.sql.types import *
# Detailed schema definition based on your nested JSON structure


spark=SparkSession.builder.appName("test").master("local[2]").getOrCreate()



df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mar27") \
  .load()

# Schema definition based on your JSON structure
def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        # Handle ArrayType to explode it
        if isinstance(df.schema[column_name].dataType, ArrayType):
            element_type = df.schema[column_name].dataType.elementType
            if isinstance(element_type, MapType):
                df = df.withColumn(column_name, explode_outer(col(column_name)))
                key_cols = map_keys(col(column_name)).alias(column_name + "_keys")
                value_cols = map_values(col(column_name)).alias(column_name + "_values")
                df = df.withColumn(column_name + "_keys", key_cols).withColumn(column_name + "_values", value_cols)
            else:
                df = df.withColumn(column_name, explode_outer(col(column_name)))
            column_list.append(column_name)
        # Handle MapType to turn keys into columns
        elif isinstance(df.schema[column_name].dataType, MapType):
            keys_df = map_keys(col(column_name)).alias(column_name + "_keys")
            values_df = map_values(col(column_name)).alias(column_name + "_values")
            df = df.withColumn(column_name + "_keys", keys_df).withColumn(column_name + "_values", values_df)
            column_list.extend([column_name + "_keys", column_name + "_values"])
        # Handle StructType to flatten struct
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


df=df.selectExpr("CAST(value AS STRING) as json")
schema = StructType([
    StructField("results", ArrayType(MapType(StringType(), StringType(), True)), True),
    StructField("nationality", StringType(), True),
    StructField("seed", StringType(), True),
    StructField("version", StringType(), True)
])
df = df.select(from_json("json", schema).alias("data")).select(col("data.*"))

# Exploding the array and flattening the structure
ndf=flatten(df)


def explode_results(df):
    # Define the schema for the JSON string inside the 'user' map
    user_schema = StructType([
        StructField("gender", StringType()),
        StructField("name", StructType([
            StructField("title", StringType()),
            StructField("first", StringType()),
            StructField("last", StringType())
        ])),
        StructField("location", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", IntegerType())
        ])),
        StructField("email", StringType()),
        StructField("username", StringType()),
        StructField("password", StringType()),
        StructField("salt", StringType()),
        StructField("md5", StringType()),
        StructField("sha1", StringType()),
        StructField("sha256", StringType()),
        StructField("registered", StringType()),
        StructField("dob", StringType()),
        StructField("phone", StringType()),
        StructField("cell", StringType()),
        StructField("picture", StructType([
            StructField("large", StringType()),
            StructField("medium", StringType()),
            StructField("thumbnail", StringType())
        ]))
    ])

    # Since 'results' is a MapType initially, we need to extract and parse the JSON from the 'user' key
    df = df.withColumn("user_json", col("results")["user"])

    # Parse the JSON string in 'user_json' to struct using the defined schema
    df = df.withColumn("user_struct", from_json("user_json", user_schema))

    # Explode the nested fields to flatten the structure
    df = df.select(
        col("nationality"),
        col("seed"),
        col("version"),
        col("user_struct.*")
    )

    return df

res = explode_results(ndf)
#res.writeStream.outputMode("append").format("console").start().awaitTermination()


def foreach_batch_function(df, epoch_id):
  df=df.withColumn("today",current_timestamp())
  sfOptions = {
      "sfURL": "kwwlcdv-qb74784.snowflakecomputing.com",
      "sfUser": "nvnkmraws",
      "sfPassword": "Mypassword.123",
      "sfDatabase": "navdb",
      "sfSchema": "public",
      "sfWarehouse": "compute_wh",
      "sfRole": "Accountadmin"
  }
  driv = "net.snowflake.spark.snowflake"
  df.write.mode("append").format(driv).options(**sfOptions).option("dbtable", "livesnowf").save()

  # Transform and write batchDF
  pass


res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

