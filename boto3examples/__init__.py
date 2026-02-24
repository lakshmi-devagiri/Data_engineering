from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r""
df = spark.read.format("").option("", "").load(data)
df.show()
