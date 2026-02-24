'''from kafka import KafkaConsumer
consumer = KafkaConsumer('feb6')
for msg in consumer:
    print(msg.value)'''
# Subscribe to 1 topic
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "feb6") \
  .load()
df.printSchema()
df=df.selectExpr("CAST(value AS STRING)")
res=(df.withColumn("name", split(col("value"),',')[0])
    .withColumn("age", split(col("value"),',')[1]))
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df=df.withColumn("ts",current_timestamp())
    df.show()
    host = "jdbc:mysql://diwakarmysql.cf08m8g4ysxd.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    (df.write.mode("append").format("jdbc").option("url", host).option("user", "admin").option("password","Mypassword.1")
     .option("dbtable", "livekafkafeb6").save())

    pass


res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
#df.writeStream.outputMode("append").format("console").start().awaitTermination()


