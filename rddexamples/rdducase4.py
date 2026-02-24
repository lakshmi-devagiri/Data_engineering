from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"D:\bigdata\drivers\donations.csv"
drdd = spark.sparkContext.textFile(data)
skip = drdd.first()

pro = drdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
    .reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)

for x in pro.take(19):
    print(x)
