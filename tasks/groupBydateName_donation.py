from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\donations.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))
df=df.withColumn("year",year(col("dt")))
# Calculate total amount donated by each year
s1 = df.groupBy(col("year"),col("name")).agg(sum(col("amount")).alias("total_donated"))
s1.orderBy(col("year").desc()).show()

win = Window.partitionBy("year").orderBy(col("total_donated").desc())
fdf = s1.withColumn("top",dense_rank().over(win)).where(col("top")==1).drop("top").orderBy(col("year"))
fdf.show()

