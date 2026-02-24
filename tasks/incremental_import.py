from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from functools import reduce
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Incremental Data Processing") \
    .master("local[*]") \
    .getOrCreate()

# Load a reference file to check schema if needed (optional)
data = r"D:\bigdata\drivers\User_Activity_Data.csv"
df_sample = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)
# df_sample.show()  # Preview optional

# Define expected schema for the incoming data
schema = StructType.fromJson({
    "fields": [
        {"metadata": {}, "name": "person_name", "nullable": True, "type": "string"},
        {"metadata": {}, "name": "activity_type", "nullable": True, "type": "string"},
        {"metadata": {}, "name": "activity_score", "nullable": True, "type": "integer"},
        {"metadata": {}, "name": "timestamp", "nullable": True, "type": "timestamp"}
    ],
    "type": "struct"
})

# Function to process only newly arrived data from a folder based on timestamp
def process_incremental_data(folder_path, checkpoint_path, schema, checkpoint_time):
    # === 1. Load last checkpoint timestamp (if exists) ===
    # This prevents reprocessing the same records
    if os.path.exists(r"D:\bigdata\drivers\incdata\last_checkpoint.txt"):
        with open(r"D:\bigdata\drivers\incdata\last_checkpoint.txt", "r") as f:
            last_checkpoint = datetime.fromisoformat(f.read().strip())
    else:
        last_checkpoint = datetime(1970, 1, 1)  # Default to start of epoch

    # === 2. Load all files in folder and filter data based on timestamp column ===
    df_raw = spark.read.schema(schema).option("header", "true").csv(folder_path)
    df = df_raw.filter(col("timestamp") > lit(last_checkpoint))  # Only new records
    df.show()  # Preview filtered records

    # === 3. Prefix all columns with '_ABH_123_' to uniquely identify source fields ===
    df = df.select([
        col(field.name).alias(f"_ABH_123_{field.name}") for field in schema.fields
    ])

    # === 4. Add a column with running total (only for numeric fields) ===
    numeric_cols = [
        col(f"_ABH_123_{field.name}") for field in schema.fields
        if field.dataType.simpleString() in ["int", "bigint", "double", "float"]
    ]
    if numeric_cols:
        df = df.withColumn("running_total", reduce(lambda a, b: a + b, numeric_cols))
    else:
        df = df.withColumn("running_total", lit(0))

    # === 5. Add metadata columns ===
    df = df.withColumn("ingest_timestamp", lit(checkpoint_time))       # Timestamp of ingestion
    df = df.withColumn("source_file_path", input_file_name())          # Source file path

    # === 6. Write the result to checkpoint location (overwrite mode) ===
    df.write.mode("overwrite").option("checkpointLocation", checkpoint_path).parquet(checkpoint_path)

    # === 7. Update last checkpoint timestamp file with max timestamp of this batch ===
    if df.count() > 0:
        latest_ts = df.select(max(col("_ABH_123_timestamp"))).first()[0]
        if latest_ts:
            with open(r"D:\bigdata\drivers\incdata\last_checkpoint.txt", "w") as f:
                f.write(latest_ts.isoformat())

# === Input paths and current timestamp ===
folder_path = r"D:\bigdata\drivers\incdata"
checkpoint_path = r"D:\bigdata\drivers\checkpoint"
checkpoint_time = datetime.now()

# === Run the incremental data processing ===
process_incremental_data(folder_path, checkpoint_path, schema, checkpoint_time)
