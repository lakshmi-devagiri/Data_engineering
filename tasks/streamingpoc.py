from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

ssc = StreamingContext(spark.sparkContext, 10)
#wait 10 sec how much data u got within 10 sec, make as a batch ...
#StreamingContext only for learning purpose not used anywhere in ur office

lines = ssc.socketTextStream("ec2-13-126-169-18.ap-south-1.compute.amazonaws.com", 1111)

#lines.pprint()
# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        df = rdd.map(lambda x:x.split(",")).toDF(["name","age","city"])
        df.show()
        df.createOrReplaceTempView("tab")
        hyd=spark.sql("select * from tab where city='hyd'")
        delh = spark.sql("select * from tab where city='del'")
        url="jdbc:mysql://tulasimysql.cexqo3lfzsdq.ap-south-1.rds.amazonaws.com:3306/mysqldb"
        hyd.write.mode("append").format("jdbc").option("url",url).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","hydlivedatadec4").save()
        delh.write.mode("append").format("jdbc").option("url", url).option("user", "myuser").option("password", "mypassword")\
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "dellivedatadec4").save()

    except:
        pass

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
#socketTextStreaming .. what it does? it get data from terminal/console /command prompt ... from specified's server and port


