from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\namesalcity.txt"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
res=df.groupBy("city").pivot("name").agg(avg(col("sal"))).na.fill(0)

'''res=df.groupBy("name").pivot("city").agg(sum(col("sal")))
|name| blr| del| hyd| mas|
+----+----+----+----+----+
| jyo|1200|null|null|null|
|venu|null|6666|7644|null|
|koti|2900|null|null|null|
| anu|null|null|null|4900|
+----+----+----+----+----+'''
res.show()
#pivot most frequently used with group by ... used to convert rows to columns etc
from pyspark.sql.types import *
data = r"D:\bigdata\drivers\pivot-eg-data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df = df.withColumn("date", to_date(col("date")))
pivoted_df = df.groupBy("name") \
               .pivot("city") \
               .agg(sum("profit").alias("total_profit"),
                    round(avg("profit"), 2).alias("avg_profit"))

pivoted_df.show()
### 2. Time-series analysis with window functions
'''Analyze trends in profit over time. This task requires students to handle date-based aggregations and use window functions to find performance indicators.

**Task:**
1.  Calculate the monthly profit for `venu`.
2.  Calculate the month-over-month (MoM) profit change as a percentage.
3.  Use a window function to find the profit from the previous month.

**Code:**
```python'''
# Extract year and month
monthly_df = df.withColumn("year", year("date")) \
               .withColumn("month", month("date"))

# Aggregate profit by year and month
monthly_summary_df = monthly_df.groupBy("name", "year", "month") \
                               .agg(sum("profit").alias("monthly_profit")) \
                               .orderBy("year", "month")

# Use a window function to get the previous month's profit
window_spec = Window.partitionBy("name").orderBy("year", "month")
mom_df = monthly_summary_df.withColumn("prev_month_profit", lag("monthly_profit", 1).over(window_spec))

# Calculate month-over-month percentage change
mom_final_df = mom_df.withColumn(
    "mom_profit_change",
    round(((col("monthly_profit") - col("prev_month_profit")) / col("prev_month_profit")) * 100, 2)
)

mom_final_df.show()
#Combine a pivot and window function in a single problem to create a more complex report.
#Task:
#Create a pivot table showing monthly profit by city, and then use a window function to rank the cities by profit for each year.

# Step 1: Create the base data with year, month, city, and profit
monthly_city_df = df.withColumn("year", year("date")) \
                    .groupBy("name", "year", "city") \
                    .agg(sum("profit").alias("annual_profit"))


pivoted_city_df = monthly_city_df.groupBy("name", "year") \
                                 .pivot("city") \
                                 .agg(sum("annual_profit")) \
                                 .fillna(0)

# Get the list of city columns dynamically
city_columns = [c for c in pivoted_city_df.columns if c not in ["name", "year"]]

# Use the stack function to un-pivot the data
stack_expr = "stack(" + str(len(city_columns)) + ", " + ", ".join([f"'{c}', `{c}`" for c in city_columns]) + ") as (city, annual_profit)"
unpivoted_df = pivoted_city_df.select("name", "year", expr(stack_expr))

# Define the window specification and rank
window_spec_unpivoted = Window.partitionBy("year").orderBy(col("annual_profit").desc())
ranked_unpivoted_df = unpivoted_df.withColumn("rank", row_number().over(window_spec_unpivoted))

# To display the final ranked list
ranked_unpivoted_df.orderBy("year", "rank").show()


#Combine a pivot and window function in a single problem to create a more complex report.
#Task:
#Create a pivot table showing monthly profit by city, and then use a window function to rank the cities by profit for each year
# Step 1: Create the base data with year, month, city, and profit
# Create pivoted DataFrame (same as before)
monthly_city_df = df.withColumn("year", year("date")) \
                    .groupBy("name", "year", "city") \
                    .agg(sum("profit").alias("annual_profit"))

pivoted_city_df = monthly_city_df.groupBy("name", "year") \
                                 .pivot("city") \
                                 .agg(sum("annual_profit")) \
                                 .fillna(0)

pivoted_city_df.show()
# Get the list of city columns dynamically
city_columns = [c for c in pivoted_city_df.columns if c not in ["name", "year"]]

# Use the stack function to un-pivot the data
stack_expr = "stack(" + str(len(city_columns)) + ", " + ", ".join([f"'{c}', `{c}`" for c in city_columns]) + ") as (city, annual_profit)"
unpivoted_df = pivoted_city_df.select("name", "year", expr(stack_expr))

# Define the window specification and rank
window_spec_unpivoted = Window.partitionBy("year").orderBy(col("annual_profit").desc())
ranked_unpivoted_df = unpivoted_df.withColumn("rank", row_number().over(window_spec_unpivoted))

# To display the final ranked list
ranked_unpivoted_df.orderBy("year", "rank").show()

# (Optional) Re-pivot the data to get the rank and profit side-by-side for each city
ranked_final_pivoted = ranked_unpivoted_df.groupBy("name", "year") \
                                          .pivot("city") \
                                          .agg(expr("first(annual_profit)").alias("annual_profit"), expr("first(rank)").alias("rank")) \
                                          .fillna(0)
ranked_final_pivoted.orderBy("year").show()

# Step 3: Rank cities by annual profit using a window function

# Assuming 'df' is your initial DataFrame
# Correctly calculate the average profit per city using groupBy and agg
avg_profit_df_correct = df.groupBy("name", "city").agg(
    avg("profit").alias("avg_profit")
)

avg_profit_df_correct.show()

# To use higher-order functions specifically, which is less efficient but possible:
from pyspark.sql.functions import collect_list, sum as array_sum, size, col

# Aggregate profits into a list (this step is inefficient but demonstrates the HOF method)
# Aggregate profits into a list for each city
grouped_df_agg = df.groupBy("name", "city").agg(
    collect_list("profit").alias("profit_list")
)

# Use aggregate() to sum the array elements
avg_profit_agg_df = grouped_df_agg.withColumn(
    "total_profit",
    expr("aggregate(profit_list, 0, (acc, x) -> acc + x)")
).withColumn(
    "avg_profit",
    (col("total_profit") / size(col("profit_list"))).cast("double")
).drop("total_profit")

avg_profit_agg_df.show()