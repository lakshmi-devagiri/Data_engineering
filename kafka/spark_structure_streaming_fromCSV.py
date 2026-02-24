from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
INPUT_DIR  = r"C:\bigdata\in"          # written by tail_to_folder.py
CHECKPOINT = r"C:\bigdata\_ckpt_csv"   # any writeable folder

schema = StructType([
    StructField("name", StringType()),
    StructField("age",  IntegerType()),
    StructField("city", StringType()),
])

df = (spark.readStream
      .format("csv")
      .option("header", True)
      .schema(schema)
      .option("maxFilesPerTrigger", 1)       # tune
      .load(INPUT_DIR))

(q := df.writeStream
      .outputMode("append")
      .format("console")                     # or "delta"
      .option("truncate", "false")
      .option("checkpointLocation", CHECKPOINT)
      .trigger(processingTime="5 seconds")   # on some DBR use .trigger(once=True) / .trigger(availableNow=True)
      .start()
).awaitTermination()