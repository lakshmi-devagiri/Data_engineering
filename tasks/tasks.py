from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel()
data = r"D:\bigdata\drivers\name_age_dept_sal.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df.show()
# Group by department and calculate average salary
avg_salary_df = df.groupBy("dept").agg(avg(col("sal")).alias("avg_salary"))
avg_salary_df.show()
# Add a new column "bonus" which is 10% of salary
df_with_bonus = df.withColumn("bonus", col("sal") * 0.10)
df_with_bonus.show()

# Window function to rank salaries within each department
windowSpec = Window.partitionBy("dept").orderBy(col("sal").desc())

# Add rank based on salary, filter the top-ranked employees per department
highest_salary_df = df.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1)
highest_salary_df.show()
# Window function to rank salaries within each department
windowSpec = Window.partitionBy("dept").orderBy(col("sal").desc())

# Add rank based on salary, filter the top-ranked employees per department
highest_salary_df = df.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1)
highest_salary_df.show()
# Group by department and sum salaries, then sort to get the top 3
top_departments_df = df.groupBy("dept").agg(sum(col("sal")).alias("total_sal")).orderBy(col("total_sal").desc()).limit(3)
top_departments_df.show()
# Step 1: Sum the salaries per department
salary_sum_df = df.groupBy("dept").agg(sum(col("sal")).alias("total_sal"))

# Step 2: Window function to rank departments based on total salary
windowSpec = Window.orderBy(col("total_sal").desc())

# Step 3: Add a row number to get the top department
top_dept_df = salary_sum_df.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).select("dept")
top_dept_df.show()
# Step 1: Sum the salaries per department
salary_sum_df = df.groupBy("dept").agg(sum(col("sal")).alias("total_sal"))

# Step 2: Window function to rank departments based on total salary
windowSpec = Window.orderBy(col("total_sal").desc())

# Step 3: Add a row number to get the top department
top_dept_df = salary_sum_df.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).select("dept")
top_dept_df.show()
# Apply filters for age >= 30 and department is "Sales"
filtered_df = df.filter((col("age") >= 30) & (col("dept") == "Sales"))
filtered_df.show()
# Step 1: Calculate average salary per department
avg_salary_df = df.groupBy("dept").agg(avg(col("sal")).alias("avg_sal"))

# Step 2: Join with original DataFrame and calculate the difference
salary_diff_df = df.join(avg_salary_df, "dept").withColumn("salary_diff", col("sal") - col("avg_sal"))
salary_diff_df.show()
# Filter employees whose names start with "J" and calculate sum of their salaries
sum_salaries_j = df.filter(col("name").startswith("J")).agg(sum(col("sal")).alias("sum_salaries")).show()
# Sort by age ascending and salary descending
sorted_df = df.orderBy(col("age").asc(), col("sal").desc())
sorted_df.show()
# Replace "Finance" with "Financial Services"
updated_df = df.withColumn("dept", when(col("dept") == "Finance", "Financial Services").otherwise(col("dept")))
updated_df.show()
# Step 1: Calculate total salary per department
total_salary_df = df.groupBy("dept").agg(sum("sal").alias("total_sal"))

# Step 2: Join with the original DataFrame and calculate the percentage
percentage_df = df.join(total_salary_df, "dept").withColumn("percentage_contribution", round((col("sal") / col("total_sal")) * 100,2))
percentage_df.show()

