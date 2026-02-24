from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

host="ec2-44-210-133-235.compute-1.amazonaws.com"
lines = spark.readStream.format("socket").option("host", host).option("port", 1234).load()

res=lines.withColumn("name", split(col("value"),",").getItem(0))\
    .withColumn("age", split(col("value"),",").getItem(1).cast("int"))\
    .withColumn("city", split(col("value"),",").getItem(2))\
    .select("name","age","city")
#res.writeStream.outputMode("append").format("console").option("truncate","false").start().awaitTermination()
#show the results thats it .. but if u eant to store data in mysql
mysqlhost="jdbc:mysql://lakshmimysql.c4x6aq42qycf.us-east-1.rds.amazonaws.com:3306/lakshmidb?useSSL=false"


def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.write.format("jdbc").option("url", mysqlhost)\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "livedata").option("user", "admin")\
    .option("password", "Mypassword.1").mode("append").save()

    pass

res.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start().awaitTermination()

