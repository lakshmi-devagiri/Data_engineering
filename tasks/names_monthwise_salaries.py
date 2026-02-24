from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\names_monthwise_salaries.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df.show()

# 3. Clean and transform month-wise salary into array
df = df.withColumn("monthwisesal", regexp_replace("monthwisesal", "[\\[\\]]", "")) \
       .withColumn("salary", split(col("monthwisesal"), ", "))

# 4. Explode the array to get row per month
df_exploded = df.withColumn("month_salary", explode(col("salary")))

# Cast salary to integer
df_exploded = df_exploded.withColumn("month_salary", col("month_salary").cast("int"))

# 5. Add month number (1-12)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window = Window.partitionBy("name").orderBy("month_salary")
df_with_month = df_exploded.withColumn("month", row_number().over(window))

# ===============================
# Example Tasks
# ===============================

# (A) Average salary per employee
avg_salary = df_exploded.groupBy("name").agg(avg("month_salary").alias("avg_salary"))
avg_salary.show()

# (B) Total yearly salary per employee
total_salary = df_exploded.groupBy("name").agg(sum("month_salary").alias("total_salary"))
total_salary.show()

# (C) Average salary per month (across employees)
monthly_avg = df_with_month.groupBy("month").agg(avg("month_salary").alias("avg_salary_month"))
monthly_avg.show()

# (D) Highest salary earned by each employee in any month
highest_salary = df_exploded.groupBy("name").agg({"month_salary": "max"})
highest_salary.show()

df = (
    df
    .withColumn("clean", F.regexp_replace("monthwisesal", r"[\[\]\s]", ""))
    .withColumn("salary_arr_str", F.split("clean", ","))
    .withColumn("salary_arr", F.expr("transform(salary_arr_str, x -> cast(x as int))"))
    .selectExpr("name", "posexplode(salary_arr) as (pos, salary)")
    .withColumn("month", (F.col("pos") + 1).cast("int"))
    .drop("pos")
)


# Useful windows
w_by_name_month = Window.partitionBy("name").orderBy("month")
w_by_name = Window.partitionBy("name")
w_by_month = Window.partitionBy("month")

df = df.cache()

print("\n=== Base long-format data (name, month, salary) ===")
df.orderBy(col("name").desc(), col("month").desc()).show(60, truncate=False)

# -----------------------------------
# 3) Core tasks
# -----------------------------------

# A) Average salary per employee
avg_salary = df.groupBy("name").agg(F.avg("salary").alias("avg_salary"))
print("\n=== A) Average salary per employee ===")
avg_salary.orderBy("name").show(truncate=False)

# B) Total yearly salary per employee
total_salary = df.groupBy("name").agg(F.sum("salary").alias("total_salary"))
print("\n=== B) Total yearly salary per employee ===")
total_salary.orderBy(F.desc("total_salary")).show(truncate=False)

# C) Average salary per month (across employees)
monthly_avg = df.groupBy("month").agg(F.avg("salary").alias("avg_salary_month"))
print("\n=== C) Average salary per month across employees ===")
monthly_avg.orderBy("month").show(12, truncate=False)

# D) Highest salary by employee (value + month)
rn_desc = F.row_number().over(Window.partitionBy("name").orderBy(F.desc("salary"), F.asc("month")))
highest_by_emp = (
    df.withColumn("rn", rn_desc)
      .where(F.col("rn") == 1)
      .select("name", F.col("salary").alias("max_salary"), "month")
)
print("\n=== D) Highest monthly salary per employee (with month) ===")
highest_by_emp.orderBy("name").show(truncate=False)

# -----------------------------------
# 4) Extra tasks you requested
# -----------------------------------

# E) Top-N earners by total salary
N = 3
top_n = total_salary.orderBy(F.desc("total_salary"), F.asc("name")).limit(N)
print(f"\n=== E) Top {N} earners by total yearly salary ===")
top_n.show(truncate=False)

# F) Month with the highest expense (sum of all salaries)
month_expense = df.groupBy("month").agg(F.sum("salary").alias("total_monthly_expense"))
top_month = month_expense.orderBy(F.desc("total_monthly_expense"), F.asc("month")).limit(1)
print("\n=== F) Month with the highest total expense ===")
month_expense.orderBy(F.desc("total_monthly_expense"), F.asc("month")).show(12, truncate=False)
print(">>> Highest expense month:")
top_month.show(truncate=False)

# G) Employee with the most salary growth (last month − first month) + % growth
first_sal = F.first("salary").over(w_by_name_month.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
last_sal  = F.last("salary").over(w_by_name_month.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
growth_df = (
    df.withColumn("first_salary", first_sal)
      .withColumn("last_salary",  last_sal)
      .withColumn("growth_abs", F.col("last_salary") - F.col("first_salary"))
      .withColumn("growth_pct", F.when(F.col("first_salary") != 0, (F.col("growth_abs")/F.col("first_salary"))*100).otherwise(F.lit(None)))
      .select("name", "first_salary", "last_salary", "growth_abs", "growth_pct")
      .distinct()
)
print("\n=== G) Salary growth per employee (last − first) and % growth ===")
growth_df.orderBy(F.desc("growth_abs"), F.asc("name")).show(truncate=False)

most_growth = growth_df.orderBy(F.desc("growth_abs"), F.asc("name")).limit(1)
print(">>> Employee with highest absolute growth:")
most_growth.show(truncate=False)

# H) Trend analysis (time-series table): salary progression for each employee
print("\n=== H) Trend analysis: salary progression (name, month, salary) ===")
df.orderBy("name", "month").show(200, truncate=False)

# -----------------------------------
# 5) More complex analytics
# -----------------------------------

# I) Variability: stddev & variance per employee
var_df = df.groupBy("name").agg(
    F.stddev_samp("salary").alias("stddev_salary"),
    F.variance("salary").alias("variance_salary")
)
print("\n=== I) Variability per employee (stddev, variance) ===")
var_df.orderBy(F.desc("stddev_salary")).show(truncate=False)

# J) Linear trend per employee (slope & intercept via covariance/variance)
# slope = Cov(month, salary) / Var(month)
stats = df.groupBy("name").agg(
    F.avg("month").alias("m_mean"),
    F.avg("salary").alias("s_mean"),
    F.avg(F.col("month")*F.col("month")).alias("m2_mean"),
    F.avg(F.col("month")*F.col("salary")).alias("ms_mean")
).withColumn(
    "var_m", F.col("m2_mean") - F.col("m_mean")*F.col("m_mean")
).withColumn(
    "cov_ms", F.col("ms_mean") - F.col("m_mean")*F.col("s_mean")
).withColumn(
    "slope", F.when(F.col("var_m") != 0, F.col("cov_ms")/F.col("var_m")).otherwise(F.lit(0.0))
).withColumn(
    "intercept", F.col("s_mean") - F.col("slope")*F.col("m_mean")
).select("name", "slope", "intercept")

print("\n=== J) Linear trend per employee (slope of salary vs month) ===")
stats.orderBy(F.desc("slope")).show(truncate=False)

# K) Anomaly detection: months where |salary - mean| > 2*stddev (per employee)
mean_std = df.groupBy("name").agg(
    F.avg("salary").alias("mean_salary"),
    F.stddev_samp("salary").alias("std_salary")
)

anomalies = (
    df.join(mean_std, "name")
      .withColumn("z_abs", F.abs(F.col("salary") - F.col("mean_salary")) / F.col("std_salary"))
      .where(F.col("std_salary").isNotNull() & (F.col("z_abs") > 2))
      .select("name", "month", "salary", "mean_salary", "std_salary", "z_abs")
      .orderBy("name", "month")
)
print("\n=== K) Anomaly months (> 2σ from employee mean) ===")
anomalies.show(truncate=False)

# L) Cumulative sum (YTD) per employee
cum_df = df.withColumn("cum_salary", F.sum("salary").over(w_by_name_month.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
print("\n=== L) Cumulative salary by month per employee (YTD) ===")
cum_df.orderBy("name", "month").show(200, truncate=False)

# M) Month-over-month (MoM) change & average MoM change per employee
prev_salary = F.lag("salary", 1).over(w_by_name_month)
mom_df = (
    df.withColumn("prev_salary", prev_salary)
      .withColumn("mom_change", F.when(F.col("prev_salary").isNull(), F.lit(0)).otherwise(F.col("salary") - F.col("prev_salary")))
      .withColumn("mom_pct", F.when(F.col("prev_salary").isNull(), F.lit(None)).otherwise((F.col("mom_change")/F.col("prev_salary"))*100))
)
avg_mom = mom_df.groupBy("name").agg(
    F.avg(F.when(F.col("mom_pct").isNotNull(), F.col("mom_pct"))).alias("avg_mom_pct")
)
print("\n=== M) Month-over-month change table ===")
mom_df.orderBy("name", "month").show(200, truncate=False)
print("\n>>> Average MoM % change per employee:")
avg_mom.orderBy(F.desc("avg_mom_pct")).show(truncate=False)

# N) Company-wide totals & averages
company_year_total = df.groupBy().agg(F.sum("salary").alias("company_total_year"), F.avg("salary").alias("company_mean_month_salary"))
print("\n=== N) Company-wide totals ===")
company_year_total.show(truncate=False)

# O) Per-month ranking of employees (1 = highest salary in that month)
rank_window = Window.partitionBy("month").orderBy(F.desc("salary"), F.asc("name"))
ranks = df.withColumn("rank_in_month", F.row_number().over(rank_window))
print("\n=== O) Per-month rankings (top 5 rows per month shown) ===")
ranks.filter(F.col("rank_in_month") <= 5).orderBy("month", "rank_in_month").show(200, truncate=False)

# P) Overall ranking by total salary (full leaderboard)
leaderboard = total_salary.orderBy(F.desc("total_salary"), F.asc("name"))
print("\n=== P) Overall leaderboard by total yearly salary ===")
leaderboard.show(truncate=False)
