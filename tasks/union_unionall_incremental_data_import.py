from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
mySchema = StructType([
 StructField("ID", IntegerType(), nullable = False),
 StructField("date", TimestampType(), nullable = True),
 StructField("customerId", IntegerType(), nullable = False),
 StructField("customerId", StringType(), nullable = True),
 StructField("address", ArrayType(StringType()), nullable = True),
 StructField("wishlisted", MapType(StringType(),BooleanType()), nullable = True),
])
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data1 = r"D:\bigdata\drivers\User_Activity_Data.csv"
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data1)
# df_sample.show()  # Preview optional

# Load existing transaction data
existing_data_df = spark.read \
 .format("csv") \
 .option("header", "true").option("inferSchema","true") \
 .load(data1)
data2=r"D:\bigdata\drivers\User_Activity_Data_incremental.csv"
# Load new transaction data
new_data_df = spark.read \
 .format("csv") \
 .option("header", "true").option("inferSchema","true") \
 .load(data2)

existing_data_df.show()
existing_data_df.printSchema()
new_data_df.printSchema()
# Identify new records based on timestamp
max_timestamp_existing = existing_data_df.selectExpr("max(timestamp)").collect()[0][0]
print("maximum timestamp from existing dataframe: ",max_timestamp_existing)
new_records_df = new_data_df.filter(new_data_df.timestamp > max_timestamp_existing)
print("latest records")
new_records_df.show()
# Append new records to existing data
updated_data_df = existing_data_df.union(new_records_df).orderBy(col("timestamp").desc())

# Show the updated dataset
updated_data_df.show()


