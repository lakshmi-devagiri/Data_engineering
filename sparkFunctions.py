from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"C:\bigdata\drivers\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
#df.show()
res=df.withColumn("phone1", regexp_replace(col("phone1"),"-",""))\
    .withColumn("phone2", regexp_replace(col("phone2"),"-",""))\
    .withColumn("email",regexp_replace(col("email"),"@gmail","@googlemail"))
res.show(truncate=False)