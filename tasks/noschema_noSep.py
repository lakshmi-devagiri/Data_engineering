from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\name_Phone_email_empid_noheader_nosep.txt"
df = spark.read.text(data)

df.show()
#by default schema is value now
#df.show()
#now i want to create schema to this data in this scenario use
reg = r"([A-Za-z]+)(\d+)([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})([A-Za-z0-9]+)"
ndf = df.select(
    regexp_extract("value", reg, 1).alias("firstname"),
    regexp_extract("value", reg, 2).alias("contactno"),
    regexp_extract("value", reg, 3).alias("email"),
    regexp_extract("value", reg, 4).alias("empid")
)
fdf = ndf.withColumn("filename", split(input_file_name(), "/")).collect()[0]["filename"][-1]
cls = fdf.split("_")[:4]
#now u can do like df = ndf.toDF(*cls)

5
print(cls)
#schema is created
#ndf.show(truncate=False)
#now i want to hide phone number
ndf = ndf.withColumn("contactno", concat(lit("*" * 7), substring(col("contactno").cast("string"), -3,3)))
def hide_email_username(email):
    username, domain = email.split('@')
    return '*' * len(username) + '@' + domain
hide = udf(hide_email_username, StringType())

res=  ndf.withColumn("email", hide(col("email")))

res.show(truncate=False)