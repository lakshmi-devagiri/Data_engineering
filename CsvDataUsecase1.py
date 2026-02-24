from pyspark.sql import *
from pyspark.sql.functions import *
from pandas import *

spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"C:\bigdata\drivers\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
df.printSchema()   #display datatypes in nice tree format

res=df.where((col("marital")=="married") & (col("balance")>=20000)).orderBy(col("age").desc())
res.show()
#by default spark consider as every column is string...
#inferSchema, true ... input data convert to appropriate datatypes