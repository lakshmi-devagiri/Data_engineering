from pyspark.sql import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sc=spark.sparkContext
data = r"D:\bigdata\drivers\donations.csv"
drdd=sc.textFile(data)
skip=drdd.first()
#res=drdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y)
res=drdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).groupByKey()\
    .map(lambda x:(x[0],sum(x[1])))


'''
('venu', [1000,2000,4000,7000,8000,2000,1000])
('anu', [1000,4000,7000])
('venkat',[1000,2000,9000,1000])
('sita', [1000,2000,9000])

'''

for x in res.take(9):
    print(x)