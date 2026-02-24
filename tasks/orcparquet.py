from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\airbnblistings.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()
df.write.format("orc").save(r"D:\bigdata\drivers\output\airbnblistingsorc")
df.write.format("parquet").save(r"D:\bigdata\drivers\output\airbnblistingparquet")
