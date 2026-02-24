from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\datemultiformat.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#df.show()
#create udf apply to existing column
possible_formats = ['dd-MM-yyyy', "dd-MMM-yyyy", "dd/MMM/yyyy","d-MMM-yyyy", "dd/mm/yyyy", "yyyy-MMM-d", "yyyy-MM-dd"]
def dateto(col, formats=possible_formats):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])

res =df.withColumn("hiredate",dateto(col("hiredate")))
res.show()