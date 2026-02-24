from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = (spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mar23").load())
ndf=df.selectExpr("CAST(value AS STRING)")

pattern = r'^(\S+) \S+ \S+ \[(\d+/\w+/\d+:\d+:\d+:\d+ \+\d+)\] "(\w+) ([^"]+)" (\d+)'

# Use regexp_extract to extract specific fields
ndf = ndf.withColumn("IP", regexp_extract(col("value"), pattern, 1))
ndf = ndf.withColumn("Ts", regexp_extract(col("value"), pattern, 2))
ndf = ndf.withColumn("Request", regexp_extract(col("value"), pattern, 3))
ndf = ndf.withColumn("StatusCode", regexp_extract(col("value"), pattern, 5))\
    .drop("value").withColumn("Ts", to_timestamp(col("ts"),"dd/MMM/yyyy:HH:mm:ss Z"))
mysqlconf = {
    "url":"jdbc:mysql://akshaymysql.c3dc8ohf85k5.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false",
    "user":"myuser",
    "password":"mypassword",
    "driver":"com.mysql.cj.jdbc.Driver"
}
sfOptions = {
  "sfURL" : "dphmvid-qq66024.snowflakecomputing.com",
  "sfUser" : "sowmyadandu",
  "sfPassword" : "Hadoop.123",
  "sfDatabase" : "sowmyadb",
  "sfSchema" : "public",
  "sfWarehouse" :"COMPUTE_WH"
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    #bdf=spark.read.romat...
    #df.write.mode("append").format("jdbc").options(**mysqlconf).option("dbtable","livedata1810").save()
    df.write.mode("append").format("jdbc") \
            .options(**mysqlconf) \
            .option("dbtable", "logsdata") \
            .save()

    pass

ndf.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()



#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

