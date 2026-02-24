import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\laksh\anaconda3\envs\py310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\laksh\anaconda3\envs\py310\python.exe"

from pyspark.sql import SparkSession
import requests
import json

# Spark session
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("test") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# API call
url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)

# Convert JSON
jsontext = response.json()

# Print countprint(type(jsontext))
print(len(jsontext))
print(type(jsontext))
print(type(jsontext[0]))
print(jsontext[0].keys())
print(jsontext[1].keys())

df = spark.createDataFrame(jsontext)

print(df.printSchema())
df = spark.createDataFrame([jsontext])
parsed = json.loads(jsontext)
df1 = spark.createDataFrame(parsed)
