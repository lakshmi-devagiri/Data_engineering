from pyspark.sql import *
from pyspark.sql.functions import *
from pandas import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#extract
spark.conf.set("spark.sql.session.timeZone", "IST")
host="jdbc:mysql://keerthanadb.cvu8xib1cycn.us-east-2.rds.amazonaws.com:3306/mysqldb?useSSL=false&user=myuser&password=mypassword"
df=spark.read.format("jdbc").option("url",host).option("dbtable","emp").option("driver","com.mysql.cj.jdbc.Driver").load()
#df.show()
#processing / transformations
res=df.na.fill(0).withColumn("fullsal",col("sal")+col("comm"))\
    .withColumn("today",current_date()).withColumn("dtdiff",datediff(col("today"),col("hiredate")))\
    .withColumn("testing", col("today")-col("hiredate"))\
    .withColumn("testing",col("testing").cast(StringType()))
res.printSchema()
#load ...save ...store
#local computer
op=r"C:\bigdata\drivers\output\mysqlempresult.csv"
#res.toPandas().to_csv(op)
#res.write.mode("overwrite").format("csv").option("header","true").save(op)
#res.write.mode("append").format("jdbc").option("url",host).option("dbtable","charantask123").option("driver","com.mysql.cj.jdbc.Driver").save()

#https://mvnrepository.com/artifact/net.snowflake/snowflake-ingest-sdk/2.0.1
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.13.30
#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.11.3-spark_3.1
#https://docs.snowflake.com/en/user-guide/spark-connector-use
sfOptions = {
  "sfURL" : "xrdjflh-igb12300.snowflakecomputing.com",
  "sfUser" : "CHARANCHEEDETI",
  "sfPassword" : "SFpassword.1",
  "sfDatabase" : "charandb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
res.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",  "cheransftab").save()

res.show(truncate=False)

#: java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver
#if u get anywhere classNotFoundException ... it's dependncy issue ... solution.. add mysql jar in spark/jars folder
