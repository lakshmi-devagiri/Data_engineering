from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"C:\Users\ADMIN\Downloads\EmployerInformation.csv"
df = (spark.read.format("csv").option("header", "true")
      .option("sep","\t").option("inferSchema","true").load(data))
import re
cln = [re.sub('[^a-zA-Z0-9]','',x) for x in df.columns]
df=df.toDF(*cln)
#df.show()
'''df=df.select(*(regexp_replace(col(c),'[^a-zA-Z0-9 ]','').alias(c) for c in df.columns))
ndf=df.withColumn("all",col("InitialApproval").cast(IntegerType())+col("ContinuingApproval").cast(IntegerType())).where((col("all")>10) & ((col("IndustryNAICSCode")=="51  Information") | (col("IndustryNAICSCode")=="54  Professional Scientific and Technical Services")))
#df.show(truncate=False)
win=Window.partitionBy("IndustryNAICSCode").orderBy(col("all").asc())
ndf=ndf.withColumn("rno",row_number().over(win))
ndf.show(99,truncate=False)'''
from itertools import cycle
from typing import List

import os
import csv
# Path where files are stored
path = "D:\\bigdata\\drivers\\"
df_list = []

# Loop through files in the directory
for filename in os.listdir(path):
    file_path = os.path.join(path, filename)

    # Determine file format based on the file extension
    if filename.endswith(".csv"):
        try:
            # Open the file with UTF-8 encoding and read the first line to determine the separator
            with open(file_path, 'r', encoding='utf-8') as file:
                first_line = file.readline().strip()

            # Simple logic to determine the separator
            if ',' in first_line:
                separator = ','
            elif ';' in first_line:
                separator = ';'
            elif '\t' in first_line:
                separator = '\t'
            else:
                separator = ','  # Default to comma if no known separator is found

            print("Importing CSV data from", file_path)
            df = spark.read.csv(file_path, inferSchema=True, sep=separator, header=True)

        except UnicodeDecodeError as e:
            print(f"Error reading {filename}: {e}")
            continue


    elif filename.endswith(".json"):

        print("Importing JSON data from", file_path)

        # Decide whether to use multiLine option based on file size or filename

        file_size = os.path.getsize(file_path)

        if file_size > 1_000_000 or "multiLine" in filename.lower():  # Example condition

            df = spark.read.option("multiLine", "true").json(file_path)

        else:

            df = spark.read.json(file_path)

    elif filename.endswith(".xml"):
        print("Importing XML data from", file_path)
        df = spark.read.format("xml").option("rowTag", "row_tag").load(file_path)

    elif filename.endswith(".orc"):
        print("Importing ORC data from", file_path)
        df = spark.read.orc(file_path)

    elif filename.endswith(".parquet"):
        print("Importing Parquet data from", file_path)
        df = spark.read.parquet(file_path)

    else:
        print("Unsupported file format:", filename)
        continue

    # Add the DataFrame to the list with its name
    df_name = filename.split(".")[0]
    df.name = df_name
    df_list.append(df)

    # Show the DataFrame (for debugging purposes)
    df.show()

# Processing of df_list can be done here
print("Finished processing all files.")