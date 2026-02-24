from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\downloads\*.csv"
df = spark.read.format("csv").option("header", "true").load(data)
cols = [re.sub("[^a-zA-Z0-1]","",x.lower()) for x in df.columns]
df=df.toDF(*cols).dropDuplicates(["phone"]).select("firstname","lastname","phone","email")
df.toPandas().to_csv( r"D:\bigdata\drivers\downloads\allcontacts.csv",header=True)
op=r"D:\bigdata\drivers\downloads\contactnumbers.csv"
#df.write.mode("append").format("csv").option("header","true").save(op)
df.show(int(df.count()))