from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#data = r"D:\bigdata\nov22_batch_attended_students.txt"
ssc = StreamingContext(spark.sparkContext, 10)
#spark streaming context used to create dstream api
lines = ssc.socketTextStream("ec2-65-2-179-216.ap-south-1.compute.amazonaws.com", 9999)

#get data from socket/terminal from specified server
#lines.pprint()
#foreachRDD
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
        hydinfo=df.where(col("city")=="hyd")
        delinfo=df.where(col("city")=="del")

        host="jdbc:mysql://database-1.c7uokm4egrwc.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
        hydinfo.write.mode("append").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("dbtable","hydinfo").save()
        delinfo.write.mode("append").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("dbtable","delhiinfo").save()

    except:
        pass

lines.foreachRDD(process)

ssc.start()             # if batch duration 10 sec completed now Start the computation
ssc.awaitTermination()  # ur batch duration 10 sec so pls wait 10 sec to process like wait