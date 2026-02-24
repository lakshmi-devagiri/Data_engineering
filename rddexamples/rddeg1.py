from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")
data=[12,23,43,11,12,98,54]
#list convert to rdd
lrdd = sc.parallelize(data)
process = lrdd.filter(lambda x:x%2==0)
for x in process.collect():
    print(x)

