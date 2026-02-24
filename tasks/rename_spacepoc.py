from pyspark.sql import *
from pyspark.sql.functions import *
# my requirement is if anywhere i have space, column append with 'my' keyword if no space don't prefix with 'my'.
# next remove all special characters.
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\10000Records.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#df.show()
import re
cols1 = ['my' + word if ' ' in word else word for word in df.columns]
cols=[re.sub("[^0-9a-zA-Z]","",c) for c in cols1]
mydf=df.toDF(*cols)
mydf.show()

