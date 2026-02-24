import os
import json
import csv
import requests
from pyspark.sql import SparkSession

# ==============================
# 🔑 REQUIRED FOR PYCHARM + SPARK (Windows)
# ==============================
os.environ["PYSPARK_PYTHON"] = r"C:\Users\laksh\anaconda3\envs\py310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\laksh\anaconda3\envs\py310\python.exe"

# ==============================
# Spark Session
# ==============================
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("API_to_JSON_CSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================
# API Call
# ==============================
url = "https://jsonplaceholder.typicode.com/posts"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()   # list of dictionaries
    print(f"API call successful. Records received: {len(data)}")

except requests.exceptions.RequestException as e:
    print("API call failed:", e)
    spark.stop()
    exit(1)

# ==============================
# Save as JSON file
# ==============================
json_file_path = "posts.json"

with open(json_file_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print(f"JSON file created: {json_file_path}")

# ==============================
# Save as CSV file (Python)
# ==============================
csv_file_path = "posts.csv"

with open(csv_file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

print(f"CSV file created: {csv_file_path}")

# ==============================
# OPTIONAL: Create Spark DataFrame & Save CSV via Spark
# ==============================
df = spark.createDataFrame(data)
df.show(5)
df.printSchema()

spark_csv_path = "spark_posts_csv"

df.coalesce(1).write.mode("overwrite").option("header", True).csv(spark_csv_path)

print(f"Spark CSV created in folder: {spark_csv_path}")

spark.stop()
