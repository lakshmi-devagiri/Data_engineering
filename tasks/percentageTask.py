from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").config("SPARK_LOCAL_IP","localhost").getOrCreate()
#spark.sparkContext
import os
data = r"D:\bigdata\drivers\sales_restatement.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
#df.show()
df = df.withColumn("reporting_month", col("Reporting_Month").cast(DateType()))
df = df.withColumn("sales_month_date", col("Sales_Month_Date").cast(DateType()))
# Define a window based on reporting_month, file_name, and product
window_spec = Window.partitionBy("reporting_month", "file_name", "product").orderBy(col("Sales_Month_Date").desc())

# Add a column with row number within each group
df = df.withColumn("row_num", row_number().over(window_spec))

# Filter for rows where Sales_Month_Date matches reporting_month
filtered_df = df.filter(df["Sales_Month_Date"] == df["reporting_month"])
filtered_df.show()
# Join data for two reporting months using lag window function

'''joined_df = (filtered_df.alias("new").join(
    filtered_df.alias("prev"),
    on=(
        (col("prev.reporting_month") == col("new.reporting_month"))
        & (col("prev.file_name") == col("new.file_name"))
        & (col("prev.product") == col("new.product"))
        & (col("prev.row_num") == col("new.row_num") - 1)
    ),
    how="inner",
))

joined_df.show()

# Calculate percentage change (handling zero in denominator)
joined_df = joined_df.withColumn(
    "percent_change",
    when(col("value1") == 0, 0)
    .otherwise(100 * ((col("value2") - col("value1")) / col("value1"))),
)

# Select desired columns for the final result
result_df = joined_df.select(
    "reporting_month", "file_name", "product", "sales_month_date", "value1", "value2", "percent_change"
)

# Display the resulting DataFrame
result_df.show()
'''