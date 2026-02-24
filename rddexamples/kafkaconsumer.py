from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = (spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "nifilog").load())
ndf=df.selectExpr("CAST(value AS STRING)")


def flatten_df(nested_df):
    def flatten_once(df):
        flat_cols = []
        explode_cols = []

        for field in df.schema.fields:
            field_name = field.name
            field_type = field.dataType

            if isinstance(field_type, StructType):
                for subfield in field_type.fields:
                    flat_cols.append(col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}"))
            elif isinstance(field_type, ArrayType):
                explode_cols.append(field_name)
            else:
                flat_cols.append(col(field_name))

        # Explode arrays after flattening other fields
        for col_name in explode_cols:
            df = df.withColumn(col_name, explode(col_name))

        return df.select(flat_cols)

    # Iteratively flatten
    read_nested_json_flag = True
    while read_nested_json_flag:
        read_nested_json_flag = False
        for field in nested_df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType)):
                read_nested_json_flag = True
                break
        nested_df = flatten_once(nested_df)

    # Fix column names with invalid characters
    for old_col in nested_df.columns:
        new_col = re.sub(r'[^a-zA-Z0-9]', '', old_col.lower())
        if old_col != new_col:
            nested_df = nested_df.withColumnRenamed(old_col, new_col)

    return nested_df
ndf = flatten_df(ndf)


#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()
sfOptions = {
  "sfURL" : "bribfbs-bh88035.snowflakecomputing.com",
  "sfUser" : "sitaramdod",
  "sfPassword" : "January-01-2000",
  "sfDatabase" : "mydb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH",
  "sfRole" : "Accountadmin"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    #bdf=spark.read.romat...
    #df.write.mode("append").format("jdbc").options(**mysqlconf).option("dbtable","livedata1810").save()
    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", "nifilivedata") \
            .save()

    pass

ndf.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
