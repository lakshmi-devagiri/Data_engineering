from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#spark = getSparkSessionInstance(rdd.context.getConf())
host="ec2-3-110-147-114.ap-south-1.compute.amazonaws.com"
from pyspark.streaming import StreamingContext
ssc = StreamingContext(spark.sparkContext, 10)
#lines = ssc.socketTextStream("ec2-3-110-147-114.ap-south-1.compute.amazonaws.com", 1234)
lines = spark.readStream.format("socket").option("host", host) \
    .option("port", 1111).load()
'''res=(lines.withColumn("name", split("value",",")[0])
     .withColumn("age", split("value",",")[1])
     .withColumn("city", split("value",",")[2]).drop("value"))'''

schema = "name STRING, score INT, city STRING"

res = (
    lines.select(
        from_csv(col("value"), schema, {"delimiter": ",", "ignoreLeadingWhiteSpace": True,
                                            "ignoreTrailingWhiteSpace": True}).alias("data")
    )
    .select("data.*")  # expands into name, score, city
)

#complete recommended after aggregate, just display purpose use append
#res.writeStream.outputMode("append").format("console").start().awaitTermination()
myhost="jdbc:mysql://venumysql.cnay6k4i6xv6.ap-south-1.rds.amazonaws.com:3306/mysqldb"
def foreach_batch_function(df, epoch_id):
    df=df.withColumn("ts",current_timestamp())
    (df.write.mode("append").format("jdbc").option("url",myhost).option("user","admin")
     .option("password","Mypassword.1").option("dbtable","structurestreamingdata").save())

    # Transform and write batchDF
    pass

res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
