from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\emailsmay4.txt"
#df = spark.read.format("csv").option("header", "true").load(data)
#df.show()
rdd1 = spark.sparkContext.textFile(data)
t1=rdd1.filter(lambda x:"@" in x).map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1])).toDF(["name","email"])
#t1=rdd1.filter(lambda x:"@" in x).map(lambda x:x.split(" "))
#t1.show()
res=t1.withColumn("mail", split(col("email"),"@")[1]).groupBy("mail").agg(count("*").alias("cnt"))
res.show(truncate=False)
'''for x in t1.take(9):
    print(x)
'''