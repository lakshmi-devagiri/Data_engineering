from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
orders_df = spark.read.option("header","true").option("inferSchema","true").csv("D:/bigdata/drivers/Orders.csv")
products_df = spark.read.option("header","true").option("inferSchema","true").csv("D:/bigdata/drivers/products.csv")


# Task 1: Calculate the total revenue for each order
task1 = orders_df.join(products_df, "ProductID").drop(orders_df.Price) \
    .withColumn("TotalRevenue", (col("Quantity") * col("Price") * (1 - col("Discount") / 100)))\
    .groupBy("OrderID")\
    .agg(sum("TotalRevenue").alias("TotalRevenue"))
task1.show()
# Task 2: Find the top-selling product in each category
#window_spec = Window.partitionBy("Category").orderBy(desc("TotalQuantitySold"))
window_spec = Window.partitionBy("Category").orderBy(desc("TotalQuantitySold"))
task2 = orders_df.join(products_df, "ProductID").drop(orders_df.Price) \
    .groupBy("Category", "ProductName") \
    .agg(sum("Quantity").alias("TotalQuantitySold")) \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("Category", "ProductName", "TotalQuantitySold")

task2.show()
# Task 3: Identify the customer with the highest total spending
task3 = orders_df.join(products_df, "ProductID").drop(orders_df.Price) \
    .withColumn("TotalSpending", (col("Quantity") * col("Price") * (1 - col("Discount") / 100))) \
    .groupBy("ProductID") \
    .agg(sum("TotalSpending").alias("TotalSpending")) \
    .orderBy(desc("TotalSpending")) \
    .limit(4)

task3.show()
# Task 4: Calculate the average discount percentage for each category
task4 = orders_df.join(products_df, "ProductID").drop(orders_df.Price) \
    .groupBy("Category").agg(avg("Discount").alias("AverageDiscountPercentage"))
task4.show()
# Task 5: Find the day with the highest total revenue
orders_df = orders_df.withColumn("Date", to_date("Date"))
task5 = orders_df.join(products_df, "ProductID").drop(orders_df.Price) \
    .withColumn("TotalRevenue", (col("Quantity") * col("Price") * (1 - col("Discount") / 100)))\
    .groupBy("Date").agg(sum("TotalRevenue").alias("TotalRevenue")).orderBy(desc("TotalRevenue")).limit(5)
task5.show()