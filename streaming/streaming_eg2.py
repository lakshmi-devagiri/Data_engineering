from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
spark=SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# --------------------------------------------------------------------------------------
# Paths & config
# --------------------------------------------------------------------------------------
INPUT_DIR       = r"C:\bigdata\livedata"        # folder where CSV files arrive
CHECKPOINT_DIR  = r"file:///C:/bigdata/chk/livedata_to_mysql"  # durable path recommended

# MySQL JDBC config
mysqlhost = "jdbc:mysql://lakshmimysql.c4x6aq42qycf.us-east-1.rds.amazonaws.com:3306/lakshmidb?useSSL=false"
mysql_user = "admin"          # <--- avoid hardcoding: use env vars or secrets
mysql_password = "Mypassword.1"
mysql_table = "livedata"      # target table

# --------------------------------------------------------------------------------------
# Schema (explicit to avoid header/typing issues)
# --------------------------------------------------------------------------------------
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age",  IntegerType(), True),
    StructField("city", StringType(), True),
])

# --------------------------------------------------------------------------------------
# Streaming source: CSV files landing in INPUT_DIR
# Notes:
#  - header=True is fine even if each file has its own header row.
#  - maxFilesPerTrigger controls how many new files are processed per micro-batch.
# --------------------------------------------------------------------------------------
src = (
    spark.readStream
         .format("csv")
         .schema(schema)                 # enforce types
         .option("header", True)         # your generator writes headers
         .option("mode", "DROPMALFORMED")
         .option("maxFilesPerTrigger", 10)
         .load(INPUT_DIR)
)

# Basic hygiene
res = (
    src
    .select(
        F.trim(F.col("name")).alias("name"),
        F.col("age").cast("int").alias("age"),
        F.trim(F.col("city")).alias("city"),
    )
    .filter(F.col("name").isNotNull() & F.col("city").isNotNull())
    .filter((F.col("age").isNull()) | ((F.col("age") >= 0) & (F.col("age") <= 120)))
    .withColumn("ingest_ts", F.current_timestamp())
)

mysqlhost="jdbc:mysql://lakshmimysql.c4x6aq42qycf.us-east-1.rds.amazonaws.com:3306/lakshmidb?useSSL=false"


def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.write.format("jdbc").option("url", mysqlhost)\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "filesreaming").option("user", "admin")\
    .option("password", "Mypassword.1").mode("append").save()

    pass

res.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start().awaitTermination()


