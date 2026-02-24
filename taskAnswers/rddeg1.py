from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sc=spark.sparkContext
data=r"D:\bigdata\drivers\asl_skip4lines.txt"

drdd = sc.textFile(data)

cln = drdd.zipWithIndex().filter(lambda x:x[1]>4).map(lambda x:x[0])
res=cln.map(lambda x:x.split(",")).toDF(["name","age","city"])
#res.show()
pro = res.where((col("city")=="hyd") & (col("age")>=35))
pro.show()
'''for x in res.take(9):
    print(x)'''