from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r'D:\bigdata\drivers\Name_phones_emails.csv'
df=(spark.read.format("csv").option("header","true")
    .option("inferSchema","true").option("delimiter",",")
    .option("mode","DROPMALFORMED").option("path",data).load())
#df.show()

#ndf = (df.withColumn("mail",split(col("email"),"@")[1]).groupBy(col("mail")).agg(count("*").alias("cnt")))
#ndf=df.where(col("name").like("A%n"))
#ndf=df.where(col("name").startswith("A"))
#ndf=df.where(col("name").endswith("n"))
#ndf=df.where(col("name").rlike("^[AEIOU]"))
ndf=df.where(col("email").contains("gmail"))


ndf.show()