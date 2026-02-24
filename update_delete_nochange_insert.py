from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\donations.csv"
data2 = r"D:\bigdata\drivers\donations_new.csv"
spark.conf.set("spark.sql.session.timeZone", "CST")
#pls note if u have primary key than u can easily find updated records
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))
df.show()
df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data2)
df2=df2.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))

res = df\
    .join(df2, df.name == df2.name, how="full_outer")\
    .withColumn("Action",
        when(df.name == df2.name, 'NOCHANGE')
        .when(df2.name.isNull(), 'DELETE')
        .when(df.name.isNull(), 'INSERT')
        .when(df.name != df2.name, 'UPDATE')
        .otherwise('UNKNOWN')
    )
res.show(400)
#https://priteshjo.medium.com/working-with-scd-type-2-in-pyspark-8eb32963724a