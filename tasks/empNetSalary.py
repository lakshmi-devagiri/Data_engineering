from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\emp_manager_sal.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df = df.withColumn("net_salary", col("sal") - col("spend_amount"))

df.show()
