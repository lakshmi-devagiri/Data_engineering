from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
lines = (spark.readStream.format("socket").option("host", "104.43.113.57")
         .option("port", 1111).load())

#create structure to this data
df=(lines.withColumn("name", split(col("value"),",")[0])
    .withColumn("age", split(col("value"),",")[1])
    .withColumn("city", split(col("value"),",")[2])).drop("value")

#after create structure display data
#df.writeStream.outputMode("append").format("console").start().awaitTermination()
mshost="jdbc:sqlserver://yashoda.ci9mki4eidd9.us-east-1.rds.amazonaws.com:1433;databaseName=venudb;trustServerCertificate=true;"

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.show()
    df=df.withColumn("ts",current_timestamp())
    (df.write.mode("append").format("jdbc").option("url", mshost)
     .option("dbtable", "locallive").option("user","admin").option("password", "Mypassword.1").save())

    pass

df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()