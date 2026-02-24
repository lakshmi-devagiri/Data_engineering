from pyspark.sql import *
from pyspark.sql.functions import *
#this code read data from a folder process different ways.
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers"
import os
def read_write_files_with_pyspark(input_folder):
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("ReadWriteFilesWithPySpark") \
        .getOrCreate()

    # List all files in the input folder
    for filename in os.listdir(input_folder):
        input_path = os.path.join(input_folder, filename)

        # Check if it's a file (not a directory)
        if os.path.isfile(input_path):
            # Read the file into a DataFrame based on the file extension
            if filename.endswith(".csv"):
                # Check if the CSV file has a different separator like , and ;
                try:
                    with open(input_path, 'r') as file:
                        first_line = file.readline()
                    if ';' in first_line:
                        df = spark.read.option("sep", ";").option("header", "true").option("inferSchema", "true").csv(
                            input_path)
                    else:
                        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
                    continue

            elif filename.endswith(".json"):
                df = spark.read.json(input_path)
            elif filename.endswith(".parquet"):
                df = spark.read.parquet(input_path)
            else:
                print(f"Unsupported file format for {filename}")
                continue

            # Show the DataFrame
            print(f"reading data from {filename} file ")
            df.show()

    # Stop the SparkSession
    spark.stop()


read_write_files_with_pyspark(data)