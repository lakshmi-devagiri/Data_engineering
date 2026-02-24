import os
import sys

# Set the SPARK_HOME path to your local Spark installation
os.environ['SPARK_HOME'] = 'D:\\bigdata\\spark-3.5.4-bin-hadoop3'  # Example: '/usr/local/spark'

# Add PySpark to the system path
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = (spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "nifilog").load())
ndf=df.selectExpr("CAST(value AS STRING)")
ndf.writeStream.outputMode("append").format("console").start().awaitTermination()
