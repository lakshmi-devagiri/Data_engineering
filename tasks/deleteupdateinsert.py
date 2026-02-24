from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, udf
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType

# Create a Spark session
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

# Sample DataFrame
data = [(1, "John", "2022-01-01"),
        (2, "Alice", "2022-01-02"),
        (1, "Bob", "2024-01-19"),
        (2, "venu", "2022-02-02")]

columns = ["id", "name", "timestamp"]

df = spark.createDataFrame(data, columns).withColumn("timestamp", col("timestamp").cast(TimestampType()))
windowSpec = Window.partitionBy("id").orderBy("timestamp")

df.show()

# Register a UDF to identify updates based on timestamp
@udf(StringType())
def identify_operation(prev_timestamp, current_timestamp):
    if prev_timestamp is None:
        return "INSERT"
    elif current_timestamp > prev_timestamp:
        return "UPDATE"
    elif current_timestamp < prev_timestamp:
        return "DELETE"
    else:
        return "NO CHANGE"

# Add a column to the DataFrame with the operation type
df = df.withColumn("prev_timestamp", lag("timestamp").over(windowSpec))
df = df.withColumn("operation", identify_operation(col("prev_timestamp"), col("timestamp")))

# Show the result
df.show()
'''df = df.withColumn("operation", when(col("prev_timestamp").isNull(), "INSERT")
                                 .when(col("timestamp") > col("prev_timestamp"), "UPDATE")
                                 .when(col("timestamp") < col("prev_timestamp"), "DELETE")
                                 .otherwise("NO CHANGE"))'''