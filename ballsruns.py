from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"D:\bigdata\drivers\matches.csv"
df = spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
#df.show()
df.printSchema()
"""TASC3"""
df1= df.groupBy("season").agg(max("id").alias("finalMatchId")).sort("season")\
    .withColumnRenamed("season","season1")
#df1.show()
df2 = df1.join(df,col("finalMatchId")==col("id"),"left")
res3 = df2.select("season","winner")
res3.show()
"""Tasc4"""
res4=df2.drop("id","season1")
res4.show()
"""Test2"""
df3 = df.groupBy("season","player_of_match").agg(count("player_of_match").alias("count_playerofmatch"))\
    .sort("season").withColumnRenamed("season","season1")
df4 = df3.groupBy("season1").agg(max("count_playerofmatch").alias("max_palyerofmatch")).sort("season1")\
    .withColumnRenamed("season1","season2")
cond = [col("season1")==col("season2"),col("count_playerofmatch")==col("max_palyerofmatch")]
df5 = df4.join(df3,cond,"left")
df6 = df5.select("season2","player_of_match","max_palyerofmatch")
df7 = df6.withColumnRenamed("season2","season").withColumnRenamed("player_of_match","player_of_series")
df7.show()
df8 = df7.groupBy("season").agg(collect_list("player_of_series")).sort("season")
df8.show()