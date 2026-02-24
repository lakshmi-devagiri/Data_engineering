from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\Array_Intersect_Use_Case.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
#string convert to array
df=df.withColumn("dt2024", split("dt2024",",")).withColumn("dt2025", split("dt2025",","))
ndf=df.withColumn("common_st",array_intersect(col("dt2024"),col("dt2025"))).where(size(col("common_st"))>0)
#ndf.show()
#ndf=df.withColumn("common_st",array_intersect(col("dt2024"),col("dt2025"))).where(length(concat_ws(",",col("common_st")))>0)
df.printSchema()
ndf.show(30,truncate=False)
