from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\empmysql.csv"
df = spark.read.format("csv").option("header", "true").option("mode","DROPMALFORMED").option("inferSchema","true").load(data)
#df.show()
#res=df.orderBy(col("sal").desc())
from pyspark.sql.window import *
win=Window.orderBy(col("sal").desc())
res=df.withColumn("drank",dense_rank().over(win)).withColumn("rank",rank().over(win))\
    .withColumn("rno",row_number().over(win)).withColumn("percent",percent_rank().over(win))\
    .withColumn("ntile",ntile(3).over(win))\
    .withColumn("lead",lead(col("sal")).over(win))\
    .withColumn("lag",lag(col("sal")).over(win)).withColumn("diff",col("sal")-col("lead"))\
    .withColumn("fst",first(col("sal")).over(win))

res.show()
#dense_rank() give rank in seq manner.
#rank() give rank not in seq manner especilly when u have duplicate values.
#row_number give unique rank.. means without any duplicate rank always give 1,2,3,4, like that even if u have duplicate value ignore
#percent_rank() it give rank based on percentage between 0 and 1 ....rank-1/total_num_records-1
#https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
#ntilerank


