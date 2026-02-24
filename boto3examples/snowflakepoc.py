from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sfOptions = {
  "sfURL" : "sqtrjca-qp36149.snowflakecomputing.com",
  "sfUser" : "SYEDFAWWAZ2003",
  "sfPassword" : "Mypassword.1",
  "sfDatabase" : "syeddb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH"
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
'''df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("dbtable",  "asltab") \
  .load()'''

data=r'D:\bigdata\drivers\Name_phones_emails.csv'
df=(spark.read.format("csv").option("header","true")
    .option("inferSchema","true").option("delimiter",",")
    .option("mode","DROPMALFORMED").option("path",data).load())
df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","namephoneemail").save()
