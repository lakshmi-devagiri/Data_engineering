from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
ssc= StreamingContext(spark.sparkContext,10)
#wait 10 sec within this 10 sec how much data u got it make as a batch.
#100% ur not using anywhere in ur office.. its just learning/testing purpose only.
lines = ssc.socketTextStream("ec2-3-109-217-167.ap-south-1.compute.amazonaws.com", 1235)
#get live data from ****localhost*** server ..**9999 port number**** get socket/terminal/command prompt
#get data from terminal
#lines.pprint()
#process data
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
        hyddf=df.where(col("city")=="hyd")
        deldf=df.where(col("city")=="del")
        host = "jdbc:mysql://charanmssql.cnzsqoiwvoaj.us-east-2.rds.amazonaws.com:3306/mysqldb?user=myuser&password=mypassword"

        hyddf.write.mode("append").format("jdbc").option("url",host).option("dbtable","hydinfo").option("driver","com.mysql.cj.jdbc.Driver").save()
        deldf.write.mode("append").format("jdbc").option("url",host).option("dbtable", "delinfo").option("driver","com.mysql.cj.jdbc.Driver").save()
    except:
        pass

lines.foreachRDD(process)


#just pring instead of processing
ssc.start()
ssc.awaitTermination()
#spark streaming wait 10 sec (above config) wait 10 sec dont do anything..awaitTermination() thats y use it
#hi after ur waiting period (awaitTermination() ) period pls start process..
