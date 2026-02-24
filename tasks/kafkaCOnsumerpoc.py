from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
# ---------- helpers ----------
sample_json_str = """{"results":[{"user":{"gender":"female","name":{"title":"madame","first":"elia","last":"durand"},"location":{"street":"5611 rue de l'abbé-soulange-bodin","city":"st-saphorin-sur-morges","state":"genève","zip":9823},"email":"elia.durand@example.com","username":"brownbutterfly684","password":"xanadu","salt":"yJKmi3HM","md5":"99cba208397d3fc5f3a9c6202c12353d","sha1":"acb75ae6f74db88095730f70d0ec41de12755c10","sha256":"433d02ce314df24af8f663089562e00bd42027f4374927626d321219000516e3","registered":1026963112,"dob":596043531,"phone":"(269)-833-3501","cell":"(715)-205-4164","AVS":"756.RDKX.JVSO.56","picture":{"large":"https://randomuser.me/api/portraits/women/69.jpg","medium":"https://randomuser.me/api/portraits/med/women/69.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/69.jpg"}}}],"nationality":"CH","seed":"3ffffe06df34e17903","version":"0.8"}"""

json_schema = schema_of_json(lit(sample_json_str))

# ---- read Kafka (streaming) ----
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "abcd")
       .option("startingOffsets", "latest")
       .load())

# Kafka 'value' is binary -> cast to string first
df = raw.select(col("value").cast("string").alias("value"))

# ---- parse JSON using the STATIC schema ----
parsed = df.select(from_json(col("value"), json_schema).alias("obj")).select("obj.*")

# ---- flatten helpers (streaming-safe; only transformations) ----
def flatten_all(df, sep="_"):
    from pyspark.sql.functions import col, explode_outer
    from pyspark.sql.types import StructType, ArrayType
    while True:
        complex_cols = [(f.name, f.dataType) for f in df.schema.fields
                        if isinstance(f.dataType, (StructType, ArrayType))]
        if not complex_cols: break
        progressed = False
        for name, dtype in complex_cols:
            if isinstance(dtype, StructType):
                df = df.select("*", *[
                    col(f"{name}.{c.name}").alias(f"{name}{sep}{c.name}") for c in dtype.fields
                ]).drop(name)
                progressed = True; break
            if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, (StructType, ArrayType)):
                df = df.withColumn(name, explode_outer(col(name)))
                progressed = True; break
        if not progressed: break
    return df

ndf = flatten_all(parsed).filter(
    (col("language") == "en") | col("locale").startswith("en")
)

sfOptions = {
  "sfURL" : "frssspq-gx49712.snowflakecomputing.com",
  "sfUser" : "VENUMSSI",
  "sfPassword" : "January-01-2025",
  "sfDatabase" : "VENUDB",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH",
  "sfRole" : "Accountadmin"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

#ndf=df.selectExpr("CAST(value AS STRING) as asl").select(from_csv(col("asl"),"name string, age int, city string").alias("data")).select(col("data.*"))

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF

    df=df.withColumn("ts", current_timestamp())

    df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "nifikafkajsondata").save()

    pass

ndf.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()