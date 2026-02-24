from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sc=spark.sparkContext
data=r"D:\bigdata\drivers\asl.csv"
res = sc.textFile(data)
#in this scenario sc.parallalize not applicable ..it's refer external data.
#proc = res.filter(lambda x: "hyd" in x)
#filter what it does? filter all records not only one line all records applicable.

proc=res.map(lambda x:x.split(",")).filter(lambda x:"hyd" in x[2])
#its same like select * from tab where city='hyd' but problem is rdd is only programming friendly.

for x in proc.take(20):
    print(x)
#collect: in for x in proc.collect(): collect all elements and return (action)
#take(9) ... it return first9 elements only if u mention take(2) return first 2 elements only instead of all.

