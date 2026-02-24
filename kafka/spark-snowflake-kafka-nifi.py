from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("randomuser_stream").getOrCreate()

# 1) Define the JSON schema of messages coming from Kafka (payload you posted)
name_schema = T.StructType([
    T.StructField("title", T.StringType()),
    T.StructField("first", T.StringType()),
    T.StructField("last",  T.StringType())
])

location_schema = T.StructType([
    T.StructField("street", T.StringType()),
    T.StructField("city",   T.StringType()),
    T.StructField("state",  T.StringType()),
    T.StructField("zip",    T.StringType())  # read as string; we can cast later if needed
])

picture_schema = T.StructType([
    T.StructField("large",     T.StringType()),
    T.StructField("medium",    T.StringType()),
    T.StructField("thumbnail", T.StringType())
])

user_schema = T.StructType([
    T.StructField("gender",     T.StringType()),
    T.StructField("name",       name_schema),
    T.StructField("location",   location_schema),
    T.StructField("email",      T.StringType()),
    T.StructField("username",   T.StringType()),
    T.StructField("password",   T.StringType()),
    T.StructField("salt",       T.StringType()),
    T.StructField("md5",        T.StringType()),
    T.StructField("sha1",       T.StringType()),
    T.StructField("sha256",     T.StringType()),
    T.StructField("registered", T.LongType()),  # epoch seconds
    T.StructField("dob",        T.LongType()),  # epoch seconds
    T.StructField("phone",      T.StringType()),
    T.StructField("cell",       T.StringType()),
    T.StructField("picture",    picture_schema),
])

payload_schema = T.StructType([
    T.StructField("results",     T.ArrayType(T.StructType([T.StructField("user", user_schema)]))),
    T.StructField("nationality", T.StringType()),
    T.StructField("seed",        T.StringType()),
    T.StructField("version",     T.StringType()),
])

# 2) Read from Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "oct10")
       .option("startingOffsets", "latest")
       .load())

# 3) Parse the JSON to get `results`, `nationality`, etc.
json_df = raw.select(F.col("value").cast("string").alias("value_str"))
parsed = json_df.select(F.from_json(F.col("value_str"), payload_schema).alias("j"))

# 4) Minimal shaping on the stream (explode results -> one row per user)
users_stream = (parsed
    .select("j.results", "j.nationality", "j.seed", "j.version")
    .withColumn("res", F.explode("results"))
    .select(
        "nationality", "seed", "version",
        F.col("res.user").alias("user")
    )
)

# 5) Write dims/fact to Snowflake inside foreachBatch
sfOptions = {
  "sfURL"      : "upzwliu-vq36736.snowflakecomputing.com",
  "sfUser"     : "patelmohit1987",
  "sfPassword" : "January-01-2000",     # <-- move to a secret ASAP
  "sfDatabase" : "mohitdb",
  "sfSchema"   : "public",
  "sfWarehouse": "COMPUTE_WH",
  "sfRole"     : "Accountadmin"
}
sf_fmt = "net.snowflake.spark.snowflake"

def build_dim_date(df_in, date_col, id_col_name):
    d = (df_in.select(F.col(date_col).alias("d"))
         .dropna()
         .dropDuplicates())
    d = (d.withColumn("year",    F.year("d"))
          .withColumn("quarter", F.quarter("d"))
          .withColumn("month",   F.month("d"))
          .withColumn("day",     F.dayofmonth("d"))
          .withColumn("dow",     F.dayofweek("d"))
          .withColumn("week",    F.weekofyear("d"))
          .withColumn(id_col_name, F.abs(F.xxhash64("d"))))
    return d.select(id_col_name, "d", "year","quarter","month","day","dow","week").dropDuplicates()

def upsert_batch_to_snowflake(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # ---- Flatten + transform inside batch ----
    df = (batch_df
          .select(
              "nationality", "seed", "version",
              F.col("user.gender").alias("gender"),
              F.col("user.name.title").alias("name_title"),
              F.col("user.name.first").alias("name_first"),
              F.col("user.name.last").alias("name_last"),
              F.col("user.location.street").alias("street"),
              F.col("user.location.city").alias("city"),
              F.col("user.location.state").alias("state"),
              F.col("user.location.zip").cast("string").alias("zip"),
              "user.email", "user.username",
              "user.registered", "user.dob",
              "user.phone", "user.cell",
              F.col("user.picture.large").alias("pic_large"),
              F.col("user.picture.medium").alias("pic_medium"),
              F.col("user.picture.thumbnail").alias("pic_thumbnail"),
          )
          .withColumn("dob_ts",        F.to_timestamp(F.from_unixtime(F.col("dob"))))
          .withColumn("registered_ts", F.to_timestamp(F.from_unixtime(F.col("registered"))))
          .withColumn("dob_date",      F.to_date("dob_ts"))
          .withColumn("registered_date", F.to_date("registered_ts"))
          .withColumn("username_lc", F.lower("username"))
          .withColumn("email_lc",    F.lower("email"))
          .withColumn("gender_lc",   F.lower("gender"))
    )

    # ---- Dimensions ----
    dim_location = (df.select("street","city","state","zip","nationality")
                      .dropna()
                      .dropDuplicates()
                      .withColumn("location_id", F.abs(F.xxhash64("street","city","state","zip","nationality"))))

    dim_picture = (df.select("pic_large","pic_medium","pic_thumbnail")
                     .dropna()
                     .dropDuplicates()
                     .withColumn("picture_id", F.abs(F.xxhash64("pic_large","pic_medium","pic_thumbnail"))))

    dim_dob_date = build_dim_date(df, "dob_date", "dob_date_id")
    dim_reg_date = build_dim_date(df, "registered_date", "reg_date_id")

    base_user = (df.dropDuplicates(["username_lc"])
                   .select(
                       F.col("username_lc").alias("username"),
                       "email_lc","gender_lc",
                       "name_title","name_first","name_last",
                       "phone","cell",
                       "street","city","state","zip","nationality",
                       "dob_date","registered_date",
                       "pic_large","pic_medium","pic_thumbnail"
                   ))

    dim_user = (base_user
        .join(dim_location, on=["street","city","state","zip","nationality"], how="left")
        .join(dim_picture,  on=["pic_large","pic_medium","pic_thumbnail"],    how="left")
        .join(dim_dob_date.select("dob_date_id", F.col("d").alias("dob_date")), on="dob_date", how="left")
        .join(dim_reg_date.select("reg_date_id", F.col("d").alias("registered_date")), on="registered_date", how="left")
        .withColumn("user_id", F.abs(F.xxhash64("username","email_lc")))
        .select(
            "user_id","username",
            F.col("email_lc").alias("email"),
            F.col("gender_lc").alias("gender"),
            "name_title","name_first","name_last",
            "phone","cell",
            "location_id","dob_date_id","reg_date_id","picture_id"
        )
    )

    fact_user = (dim_user
                 .select("user_id","location_id","dob_date_id","reg_date_id","picture_id")
                 .withColumn("user_count", F.lit(1)))

    # ---- Write each table to Snowflake (append) ----
    # Consider de-duplicating at the DB layer or making PKs with constraints.
    dim_location.write.format(sf_fmt).options(**sfOptions).mode("append").option("dbtable","dim_location").save()
    dim_picture.write .format(sf_fmt).options(**sfOptions).mode("append").option("dbtable","dim_picture").save()
    dim_dob_date.write .format(sf_fmt).options(**sfOptions).mode("append").option("dbtable","dim_date_dob").save()
    dim_reg_date.write  .format(sf_fmt).options(**sfOptions).mode("append").option("dbtable","dim_date_registered").save()
    dim_user.write      .format(sf_fmt).options(**sfOptions).mode("append").option("dbtable","dim_user").save()
    fact_user.write     .format(sf_fmt).options(**sfOptions).mode("append").option("dbtable","fact_user").save()

# 6) Start the stream with foreachBatch sink
query = (users_stream
         .writeStream
         .outputMode("update")   # "append" also fine here
         .option("checkpointLocation", "/tmp/ru_checkpoints/oct10")  # change path
         .foreachBatch(upsert_batch_to_snowflake)
         .start())

query.awaitTermination()
print("successfully exported")

# Prefer Delta in Databricks; use Parquet if you don’t have Delta
'''fmt = "delta"   # change to "parquet" if needed
mode = "overwrite"

dim_location.write.format(fmt).mode(mode).saveAsTable("randomuser.dim_location")
dim_picture.write.format(fmt).mode(mode).saveAsTable("randomuser.dim_picture")
dim_dob_date.write.format(fmt).mode(mode).saveAsTable("randomuser.dim_date_dob")
dim_registered_date.write.format(fmt).mode(mode).saveAsTable("randomuser.dim_date_registered")
dim_user.write.format(fmt).mode(mode).saveAsTable("randomuser.dim_user")
fact_user.write.format(fmt).mode(mode).saveAsTable("randomuser.fact_user")
'''
