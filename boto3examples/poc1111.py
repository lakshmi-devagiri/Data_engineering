from pyspark.sql import *
from pyspark.sql.functions import *
from pandas import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#spark.sparkContext.textFile()


data1=r"D:\bigdata\drivers\nep.csv"
ndf=spark.read.format("csv").option("header","true").load(data1)
data2=r"D:\bigdata\drivers\asl.csv"
adf=spark.read.format("csv").option("header","true").load(data2)
adf.show(5)
ndf.show(9)
#join two dataframes
#jdf = adf.join(ndf,"name","inner") #
#jdf = adf.join(ndf,"name","left") #
#jdf = adf.join(ndf,"name","right")
#jdf = adf.join(ndf,"name","full")
#jdf = adf.join(ndf,"name","leftanti")
jdf = adf.join(ndf,"name","inner").withColumn("mail", split(col("email"),"@")[1])\
    .groupBy(col("mail")).agg(count("*").alias("cnt"))
jdf.show(29)

