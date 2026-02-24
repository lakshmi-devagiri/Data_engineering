from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").config("spark.sql.debug.maxToStringFields","100").getOrCreate()
host="jdbc:mysql://database-mysql.cn6m6kai25os.ap-south-1.rds.amazonaws.com:3306/sriramdb"

data = r"C:\bigdata\drivers\r10000Records.csv"
aaa =spark.sparkContext.textFile(data)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load(data)
df.show()

df.printSchema()
#display dataframe columns and it's datatypes in nice tree format

#all dataframe columns u ll get in the form of list
import re
cols = [re.sub("[^a-zA-Z0-1]","",x.lower()) for x in df.columns]
df=df.toDF(*cols)
df.show()
#toDF used to rename all columns ...and rdd convert to dataframe ..
#process
df.select("email").rdd.collect()

df.createOrReplaceTempView("tab")
res=spark.sql("select email from tab")
res.write.mode("append")
res.show()
#pip install -U "jupyter-server<2.0.0