from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# 1. Spark session
spark = SparkSession.builder.appName("ProductStats").getOrCreate()

# 2. Sample data creation (simulate 10 records)
data = r"D:\bigdata\drivers\products_30_rows.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)
df.show()


# 4. #calculate the bonus if price > 2500 bonus is price * 25% , between 1500 to 2499 then 15% else 10 percent
bonus_df = df.withColumn("Bonus", round(
  when(col("Price") >= 2500, col("Price") * 0.25)
  .when((col("Price") >= 1500) & (col("Price") <= 2499), col("Price") * 0.15)
  .otherwise(col("Price") * 0.10), 2))

# 5. # calculate the top selling products ,category wiese give top total sales and give 3 products from each
sales_window = Window.orderBy("Price")
bonus_df = bonus_df.withColumn("Total_sales", col("Price") * col("Quantity"))

# 6. Top 3 products by category # top Selling products
rank_window = Window.partitionBy("Category").orderBy(col("Total_sales").desc())
ranked_df = bonus_df.withColumn("Ranking", dense_rank().over(rank_window)).filter(col("Ranking") <= 3)

# 7. Max price column # calculate running total and max,min and avg price in front of every column
max_price_window = Window.orderBy(col("Price").desc())
ranked_df = ranked_df.withColumn("Max_price", max("Price").over(max_price_window))

# 8. Max price + category # Combine product name and maximum price into one column
ranked_df = ranked_df.withColumn("Max_Price_Product", concat(col("Category"), lit(" - "), col("Max_price")))

# 9. Min price logic# # # Calculate minimum price and add it as a new column
min_price_window = Window.partitionBy("Category").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
ranked_df = ranked_df.withColumn("Min_price", min("Price").over(min_price_window))

# 10. Min price + category # # Combine product name and maximum price into one column
ranked_df = ranked_df.withColumn("Min_Price_Product", concat(col("Category"), lit(" - "), col("Min_price")))

# 11. Final output
ranked_df.show(truncate=False)
