from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\emailsmay4.txt"
df = spark.read.text(data)
df.show()
# Patterns
#name_pattern = r"^(\w+\s+\w+)"  # Extract first two words
name_pattern = r"""^(.*?)\s*\("""

mobile_pattern = r"\+?\d{1,3}[-.\s]?\d{10}"  # Mobile number pattern
email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"


# Extract data
df_extracted = df.withColumn("full_name", regexp_extract(col("value"), name_pattern, 1)) \
                 .withColumn("mobile_number", regexp_extract(col("value"), mobile_pattern, 0)) \
                 .withColumn("email", regexp_extract(col("value"), email_pattern, 0))

# Show the result
df_filtered = df_extracted.filter((col("mobile_number") != "") | (col("email") != ""))

df_filtered.show(truncate=False)
df_filtered.printSchema()