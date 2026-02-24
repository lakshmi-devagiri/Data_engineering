from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\donations.csv"
spark.conf.set("spark.sql.session.timeZone", "CST")

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today",current_date()).withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("datediff",col("today")-col("dt"))\
    .withColumn("dtadd",date_add(col("today"),100)) \
    .withColumn("dtaddminus", date_add(col("today"), -100))\
    .withColumn("dtformat",date_format(col("dt"),"dd-MMM-yyyy-EEE"))\
    .withColumn("dttrun",date_trunc("year",col("ts")))\
    .withColumn("nextday",next_day(col("today"),"Fri"))\
    .withColumn("lastday",last_day(col("dt"))).withColumn("dayofmon",dayofmonth(col("dt")))\
    .withColumn("dayofweek",dayofweek(col("dt"))).withColumn("dayofyr",dayofyear(col("dt")))\
    .withColumn("monbet",months_between(col("today"),col("dt")))\
    .withColumn("weeksofyr",weekofyear(col("dt")))\
    .withColumn("lastfri",next_day(date_add(last_day(col("dt")),-7),"Fri"))\
    .withColumn("ux",unix_timestamp())


#dayofweek ..1-sun,2-mon,3-tue,4-wed,5-thu,6-fri,7-sat

df.show(truncate=False)
#to_date() it convert string/int/tiestamp data to date format ..
#to_date(col("dt"),"d-M-yyyy")) here d-M-yyyy format is data input format. means 1-3-2021 consider as d-M-yyyy
#https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df.printSchema()
#printSchema() used to print column datatypes very useful.
#by default spark consider as yyyy-MM-dd format at that time only spark consier as date format, but
#input data u have dd-MM-yyyy (30-10-2021) or d-M-yyyy (1-3-2021) format so default its string format.
