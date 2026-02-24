from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "feb10").load()
df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

res=ndf.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))
host="jdbc:mysql://diwakarmysql.cf08m8g4ysxd.ap-south-1.rds.amazonaws.com:3306/mysqldb?user=admin&password=Mypassword.1"

def foreach_batch_function(df, epoch_id):
    df=df.withColumn("ts",current_timestamp())

    df.write.mode("append").format("jdbc").option("url",host).option("dbtable","kafka_live").save()

    # Transform and write batchDF
    pass

res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
#res.writeStream.outputMode("append").format("console").start().awaitTermination()