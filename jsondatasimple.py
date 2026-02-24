from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\zips.json"
df = spark.read.format("json").load(data)
df.printSchema()

#df.show(truncate=False)
#light weight format used to exchange data quickly between two servers use json
#web servers, social media, cloud, many devices like routers
#explode ... list/array/struct/map data explode/unnest data
df=df.withColumn("loc",explode(col("loc")))\
    .withColumnRenamed("_id","id")\
    .withColumnRenamed("pop","pincode")
#df.show()
#df.printSchema()
#res=df.groupBy("state").agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
#res.show()
host="jdbc:mysql://keerthanadb.cvu8xib1cycn.us-east-2.rds.amazonaws.com:3306/mysqldb?useSSL=false&user=myuser&password=mypassword"
df.write.mode("overwrite").format("jdbc").option("url",host).option("dbtable","jsondata").option("driver","com.mysql.cj.jdbc.Driver").save()

#df.show()

#withColumnRenamed ... used to rename one column at a time.