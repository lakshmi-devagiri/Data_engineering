from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")
data=r"D:\bigdata\datasets\emailsmay4.txt"
drdd =sc.textFile(data,10)

#res=drdd.flatMap(lambda x:x.split(" ")).filter(lambda x:"@" in x)
import re
stp=['PM','to','the','','you','me','is','i','in']

res=drdd.flatMap(lambda x:x.split(" ")).map(lambda x:(re.sub("[^a-zA-z]","",x)))\
    .filter(lambda x: x not in stp).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],False)

for x in res.take(29):
    print(x)

'''
reduceByKey: it's same like group by in sql ... used to group the values, but only imp thing is 
input data must be key, value format

flatMap = map + flatten .. means first apply a map, next flatter the results (remove array)
apply a logic on top of each and every element next flatter the results.
'''