from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\Name_phones_emails.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df.show()
ndf=df.groupBy("name").agg(collect_list(col("phone")).alias("phone_numbers"), collect_list(col("email")).alias("email_ids"))
ndf.show(truncate=False)