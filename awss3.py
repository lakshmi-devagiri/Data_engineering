from pyspark.sql import *
ACCESS_KEY = "AAKIAT5CQT4XOMNL5OPGE"
SECRET_KEY = """ZTjAzp8NIAwwdEThoYEwz7xsXLrHhEZfIOTP1yBJ"""

spark = (SparkSession.builder
         .appName("pyspark")
         .master("local[*]")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentia ls.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
         .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
         .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
         .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
.config("spark.executor.extraJavaOptions","-XX:+UseG1GC")
.config("spark.driver.extraJavaOptions","-XX:+UseG1GC")
.config("-XX:G1HeapRegionSize","16m")
         .getOrCreate())

data=r"3://lakshmi2026/city-pincode.csv"
df=spark.read.format("csv").option("sep",";").option("header","true").load(data)
df.show()