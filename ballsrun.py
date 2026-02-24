from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data=r"C:\bigdata\drivers\balls-overs.txt"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

df1 = df.withColumn("runs", when(col("runs") == "1wide", 1).when(col("runs") == "2wide", 2).otherwise(col("runs")))
df2 = df1.withColumn("wicket", when(col("runs") == "w", 1).otherwise(0))
df3 = df2.withColumn("runs", when(col("runs") == "w", "0").otherwise(col("runs")))
df4 = df3.withColumn("balls", col("balls").cast("int"))
df5 = df4.withColumn("over", ceil(col("balls")/6).cast("int"))
df6 = df5.withColumn("runs", col("runs").cast("int"))
df7 = df6.groupBy("over").agg(sum("runs").alias("runs"),sum("wicket").alias("wicket"))
df8 = df7.withColumn("cum_runs", sum("runs").over(Window.orderBy("over")))
df9 = df8.withColumn("score", concat(col("cum_runs").cast("string"),lit("/"),col("wicket").cast("string")))
df9.select("over","runs","score").show()