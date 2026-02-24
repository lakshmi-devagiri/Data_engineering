from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "CST")
data = r"D:\bigdata\drivers\donations.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#convert date colum as date format
df=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy")).withColumn("ts",current_timestamp())
#res=df.where(year(col("dt"))>2023)
res=(df.withColumn("today",current_date())
     .withColumn("after100",date_add(col("dt"),100))
     .withColumn("before100",date_add(col("dt"),-100))
     .withColumn("lastdt",last_day(col("dt")))
     .withColumn("dtdiff",datediff(col("today"),col("dt")))
     .withColumn("dtformat",date_format(col("dt"), "dd-MMM-yyyy-EEEE"))
     .withColumn("dttruuncate",date_trunc("hour",col("ts")))
     .withColumn("nxtday",next_day(col("dt"),"Fri"))
     .withColumn("lastfri", next_day(date_add(last_day(col("dt")),-7),"Fri"))
     .withColumn("dyofyr",dayofyear(col("dt"))).withColumn("dyofmon",dayofmonth(col("dt")))
     .withColumn("dayofweek",dayofweek(col("dt")))
     .withColumn("year",year("dt")).withColumn("month",month(col("dt")))
     .withColumn("dy",months_between(col("today"),col("dt")))
     .withColumn("ld",ceil(col("dy"))).withColumn("rnd",round(col("dy")))
     .withColumn("flr",floor(col("dy")))
     .withColumn("uxts",unix_timestamp())
     )
#Get the Date of the Last Working Day of the Month (Monday to Friday)

res = res.withColumn(
    "last_working_day",
    expr("""
        CASE 
            WHEN dayofweek(last_day(dt)) = 1 THEN date_add(last_day(dt), -2)  -- Sunday, go back 2 days
            WHEN dayofweek(last_day(dt)) = 7 THEN date_add(last_day(dt), -1)  -- Saturday, go back 1 day
            ELSE last_day(dt)  -- Weekday
        END
    """)
)
#dynamically calculates the number of weekends and working days for each month
df=res
df = df.withColumn("month", to_date(date_trunc("month", col("dt"))))

days_in_month = df.withColumn("day", explode(sequence(col("month"), to_date(last_day(col("dt"))))))

df = days_in_month.groupBy("month").agg(
    sum(when(dayofweek(col("day")).isin([2, 3, 4, 5, 6]), 1).otherwise(0)).alias("working_days_count"),
    sum(when(dayofweek(col("day")).isin([1, 7]), 1).otherwise(0)).alias("weekend_days_count")
).withColumnRenamed("month","dt")
df = df.withColumn("dt",last_day(col("dt")))
df.show()
'''res= res.withColumn("month", to_date(date_trunc("month", col("dt"))))

days_in_month = res.withColumn("day", explode(sequence(col("month"), to_date(last_day(col("dt"))))))

working_days = (
    days_in_month.filter(dayofweek(col("day")).isin([2, 3, 4, 5, 6]))
    .groupBy("month")
    .agg(countDistinct(col("day")).alias("working_days_count"))
)

weekend_days = (
    days_in_month.filter(dayofweek(col("day")).isin([1, 7]))
    .groupBy("month")
    .agg(countDistinct(col("day")).alias("weekend_days_count"))
)

res = (
    res.join(working_days, "month", "left")
    .join(weekend_days, "month", "left")
    .fillna(0)
).drop("month")

res.show()'''