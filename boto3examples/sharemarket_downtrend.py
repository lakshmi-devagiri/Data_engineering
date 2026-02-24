from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\INFYHistoricalData.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()
# Step 1: Clean and convert "Change %" to numeric
stock_df = df.withColumn("Change", regexp_replace(col("Change %"), "%", "").cast("float"))

# Step 2: Identify days with negative changes
stock_df = stock_df.withColumn("is_negative", when(col("Change") < 0, 1).otherwise(0))

# Step 3: Set up window specifications to detect consecutive negative days
window_spec = Window.orderBy("Date")
group_window_spec = Window.partitionBy("is_negative").orderBy("Date")

# Calculate group ID for consecutive negative changes
stock_df = stock_df.withColumn(
    "grp",
    row_number().over(window_spec) - row_number().over(group_window_spec)
)

# Step 4: Filter groups with exactly 3 consecutive negative days
negative_streaks_df = (
    stock_df
    .filter(col("is_negative") == 1)
    .groupBy("grp")
    .agg(
        min("Date").alias("start_date"),
        max("Date").alias("end_date"),
        count("Date").alias("negative_streak_days")
    )
    .filter(col("negative_streak_days") == 3)  # Modify as needed for different streak lengths
)

# Display results
negative_streaks_df.select("start_date", "end_date", "negative_streak_days").show()