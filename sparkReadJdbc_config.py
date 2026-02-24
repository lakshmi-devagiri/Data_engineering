from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
host="jdbc:mysql://akshaymysql.c3dc8ohf85k5.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false&user=myuser&password=mypassword"
df=spark.read.format("jdbc").option("url",host).option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","asltab").load()
res=df.where(col("city").isin(["hyd","blr"])).withColumn("all",concat_ws("",*df.columns))
res.show()
#store/export
