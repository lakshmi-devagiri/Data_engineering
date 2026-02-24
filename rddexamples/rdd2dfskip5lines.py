from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\asl.csv"
ardd = spark.sparkContext.textFile(data)

pre=(ardd.zipWithIndex().filter(lambda x:x[1]>4).map(lambda x:x[0])
     .map(lambda x:x.split(",")))
df=pre.toDF(["name","age","city"])
df.show()
