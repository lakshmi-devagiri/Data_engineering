from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\bank-full.csv"
df = spark.read.format("csv").option("sep",";").option("header", "true").option("inferSchema","true").load(data)
df.show()

# 1. % of people with loans across education levels
t1 = df.groupBy("education").agg(
    count("*").alias("total"),
    count(when(col("loan") == "yes", True)).alias("loan_count")
).withColumn("loan_pct", round((col("loan_count") / col("total")) * 100, 2))

t1.show()

# 2. People who have a house but no loan
t2 = df.filter((col("housing") == "yes") & (col("loan") == "no"))
t2.show()

# 3. Loan Recommendation Column
df3 = df.withColumn("loan_recommendation", when(
    (col("balance") > 20000) & (col("housing") == "no") & (col("loan") == "yes"), "eligible"
).when(
    (col("balance") > 20000) & (col("housing") == "yes") & (col("loan") == "yes"), "eligible"
).when(
    (col("balance") > 10000) & (col("housing") == "no") & (col("loan") == "no") & (col("age").between(25, 45)), "eligible"
).otherwise("not eligible"))

task3=df3.select("age", "balance", "housing", "loan", "loan_recommendation")
task3.show()
print("Task 4: What age range has the most people with loans")
# What age range has the most people with housing loans
# Define age bins using CASE WHEN
df = df.withColumn("age_range", when(col("age") < 30, "20-29")
                                .when((col("age") >= 30) & (col("age") < 40), "30-39")
                                .when((col("age") >= 40) & (col("age") < 50), "40-49")
                                .when((col("age") >= 50) & (col("age") < 60), "50-59")
                                .otherwise("60+"))

# Group by age_range for loans
task4 = df.groupBy("age_range").agg(
    count(when(col("loan") == "yes", True)).alias("loan_count"),
    count(when(col("housing") == "yes", True)).alias("housing_count")
).orderBy(col("loan_count").desc())
print("task 4  Group by age_range for loans and housing")
task4.show()
print("Task 5: Loan Ratio by Age Band")
# Step 1: Create Age Bands
df = df.withColumn("age_band", when(col("age") < 30, "20-29")
                               .when((col("age") >= 30) & (col("age") < 40), "30-39")
                               .when((col("age") >= 40) & (col("age") < 50), "40-49")
                               .when((col("age") >= 50) & (col("age") < 60), "50-59")
                               .otherwise("60+"))
task5 = df.groupBy("age_band").agg(
    count("*").alias("total_customers"),
    count(when(col("loan") == "yes", True)).alias("loan_count")
).withColumn("loan_pct", round((col("loan_count") / col("total_customers")) * 100, 2))

task5.show()

print(" Task 6: Conversion Ratio by Marital Status and Age Band")
task6 = df.groupBy("marital", "age_band").agg(
    count("*").alias("total_customers"),
    count(when(col("y") == "yes", True)).alias("conversions")
).withColumn("conversion_pct", round((col("conversions") / col("total_customers")) * 100, 2))

task6.show()