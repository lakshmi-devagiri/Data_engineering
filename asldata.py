from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = r"C:\bigdata\drivers\aadharpancarddata.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)

ndf = df.withColumn("AadharCardNumber", regexp_replace(col("AadharCardNumber"), "[- ]","")).withColumn("AadharCardNumberStar", concat(lit("*" * 12), substring(col("AadharCardNumber").cast("string"), -4,4)))
ndf.show()

