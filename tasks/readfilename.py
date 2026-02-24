from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\donations_new_tx.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#df.show()

print(ndf)
