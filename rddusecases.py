from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#sparkSession is unified context (unifying sqlCOntext, sparkContext, spark streaming context)
sc=spark.sparkContext

data=r"D:\bigdata\drivers\chatdata.txt"
drdd = sc.textFile(data)
nrdd=drdd.flatMap(lambda x:x.split("\t")).flatMap(lambda x:x.split(" ")).filter(lambda x: "@" in x)
for x in nrdd.take(9):
    print(x)
