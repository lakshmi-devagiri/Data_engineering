from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\json_multiline.json"
#df = spark.read.format("json").load(data)
from pyspark.sql.types import *


df=spark.read.option("multiLine","true").json(data)

df.show()

