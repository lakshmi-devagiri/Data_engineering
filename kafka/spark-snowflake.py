from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
# Set options below
sfOptions = {
  "sfURL" : "upzwliu-vq36736.snowflakecomputing.com",
  "sfUser" : "patelmohit1987",
  "sfPassword" : "January-01-2000",
  "sfDatabase" : "mohitdb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH",
  "sfRole" : "Accountadmin"
}
import os
import re

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
path="D:\\bigdata\\drivers\\"
csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
print(csv_files)
#df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",  "asl").load()
#df.show()
def clean_column(col_name):
    # Replace non-alphanumeric chars with underscore
    col = re.sub(r'[^0-9a-zA-Z_]', '_', col_name)
    # Ensure no double underscores and lowercase
    col = re.sub(r'_+', '_', col).lower()
    return col.strip('_')
def detect_delimiter(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
        if ";" in first_line:
            return ";"
        elif "\t" in first_line:
            return "\t"

        else:
            return ","   # default fallback
def fix_table_name(file_name: str) -> str:
    # base file name without extension
    base_name = os.path.splitext(os.path.basename(file_name))[0]

    # If it starts with digits, move them to the end: 10000Records -> Records10000
    m = re.match(r'^(\d+)(.*)$', base_name)
    if m:
        base_name = f"{m.group(2)}{m.group(1)}"

    # Replace any non [A-Za-z0-9_] with underscore
    name = re.sub(r'[^A-Za-z0-9_]', '_', base_name)

    # Collapse consecutive underscores and trim leading/trailing underscores
    name = re.sub(r'_+', '_', name).strip('_')

    # Ensure it starts with a letter (Snowflake/Hive friendly). If not, prefix.
    if not re.match(r'^[A-Za-z]', name or ''):
        name = f"t_{name}"

    return name.lower()

for file in csv_files:
    file_path = os.path.join(path, file)
    table_name = fix_table_name(file)
    print("exportng data from", file_path)

    delimiter = detect_delimiter(file_path)
    df = spark.read.option("header", "true").option("sep",delimiter).csv(file_path)
    new_cols = [clean_column(c) for c in df.columns]
    df = df.toDF(*new_cols)
    df.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",  table_name).save()
    df.show(5, truncate=False)
