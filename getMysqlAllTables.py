from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
tabs=["aadharpancarddata","alldata","aslblr","aslhyd","aslhydnisha","asltab","bank560k","donations","hydinfo","kafkastruct_streamdata","livestuctdata1810M","logsdatanew","masinfo","nishamarried50k","oct17livedata","salman_blr"]

for x in tabs:
    print("importing table: ",x)
    host="jdbc:mysql://akshaymysql.c3dc8ohf85k5.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false&user=myuser&password=mypassword"
    df=(spark.read.format("jdbc").option("url",host)
        .option("dbtable",x)
        .option("driver","com.mysql.cj.jdbc.Driver").load())
    df.show()
