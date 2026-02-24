from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
lines = ssc.socketTextStream("ec2-13-201-223-76.ap-south-1.compute.amazonaws.com", 1111)
#lines.pprint()
#foreachRDD
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
        hydf=df.where(col("city")=="hyd")

        host="jdbc:mysql://diwakarmysql.cf08m8g4ysxd.ap-south-1.rds.amazonaws.com:3306/mysqldb"
        df.write.mode("append").format("jdbc").option("url",host).option("user","admin").option("password","Mypassword.1").option("dbtable","livefeb3").save()

        df.show()
    except:
        pass

lines.foreachRDD(process)
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
