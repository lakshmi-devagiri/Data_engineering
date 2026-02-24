from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("learning-schema") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Read CSV using explicit schema
df = spark.read.format("json") \
    .option("header", "true") \
    .schema(schema) \
    .load("C:/bigdata/drivers/US_STATES_recipes.csv")
# Show data and schema
df.show()
df.printSchema()
