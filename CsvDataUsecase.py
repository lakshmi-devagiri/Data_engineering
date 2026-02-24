from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"D:\bigdata\drivers\asl.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").option("mode","DROPMALFORMED").option("inferSchema","true").load(data)
#df.show()

#data cleaning & processing
#dsl friendly or programmming friendly or dataframe api friendly
res=df.na.fill("nodata").na.fill(0).withColumn("email", lit("***@gmail.com"))\
    .withColumn("grade",when(col("age")<=20,"minor").when(col("age")>60,"oldaged").otherwise("Major"))\
    .withColumn("city", regexp_replace(col("city"),"l","A"))

df.createOrReplaceTempView("tab")
#res=spark.sql("select *, '****@yarhoo.com' mail,  (case when(age)<23 then'minor' when(age)>60 then 'oldaged' else 'major' end) grade from tab")
res.show()
#na.fill("nodata") ...... if u have any string value is null, at that time string null value fill with "nodata"
#.na.fill(0) if any neumerical value is null that number fill with 0
#withColumn ...used to add a new column with specified functions
#withColumn .... if theere is no column (like grade) it added new column... if already that column exists it's update existing column
#lit is a function used to add something dummy values.