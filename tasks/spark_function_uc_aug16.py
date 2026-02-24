from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\us-500.csv"
df = spark.read.format("csv").option("header", "true").option("mode","DROPMALFORMED").load(data)
#df.show()
ndf=df.withColumn("phone1",regexp_replace(col("phone1"),"-",""))\
    .withColumn("email", regexp_replace(col("email"),"gmail","google"))\
    .withColumn("mail",split(col("email"),"[@,\.]")[1])\
    .withColumn("rno",monotonically_increasing_id()+1)\
    .where(col("rno").between(20,35))

#i want a ow between 20 , 35 only ..
    #.groupBy("mail").agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
#if u have below 20 characters like [jbutt, google, com] at that time show all characters, but if more than 20 characters it truncated as ...
#in split(col("email"),"@") if u mention @ ... it ll split based on @ so u ll get jbutt@google.com like that, but if large url like lpaprocki@hotmail.com it contains21 characters so shows lpaprocki@hotmail...
# split(col("email"),"[@,\.]") if u mention [@,\.] it means either @ symbol or . symbol (dot) split that ,
#but here instead of . u mention \. the main reason it's escape character. tjats y u got like [jbutt, google, com]
#split returns List so list always call [list] call like list[0] list[1] like that
#list always starts from 0 so list[1] means e-mail..



ndf.show()