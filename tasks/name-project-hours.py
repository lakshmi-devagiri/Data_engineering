from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\emp-project-hours.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df.show()
#1) Add workload status (API version of your SQL)
workload_df = (
    df
    .withColumn("name", initcap("name"))
    .withColumn(
        "workload_status",
        when(col("hours") > 200, lit("Overloaded"))
         .when((col("hours") >= 100) & (col("hours") <= 200), lit("Balanced"))
         .otherwise(lit("Underutilized"))
    )
)
workload_df.show(truncate=False)

#2) Status counts (groupBy + agg)
status_counts = workload_df.groupBy("workload_status").agg(count("*").alias("status_count"))
status_counts.show()

#3) Average hours per project and per status
avg_hours = workload_df.groupBy("project").agg(avg("hours").alias("avg_hours"))
avg_by_status = workload_df.groupBy("project", "workload_status").agg(avg("hours").alias("avg_hours"))
avg_by_status.show()
#4) Top-3 heaviest workloads per project (Window + dense_rank)
w = Window.partitionBy("project").orderBy(col("hours").desc())
top3_per_project = (
    workload_df
    .withColumn("rnk", dense_rank().over(w))
    .filter(col("rnk") <= 3)
    .drop("rnk")
)
top3_per_project.show()
#5) Pivot: count of employees by project × workload_status
pivot_counts = (
    workload_df
    .groupBy("project")
    .pivot("workload_status", ["Underutilized","Balanced","Overloaded"])
    .agg(count("*"))
    .fillna(0)
)
pivot_counts.show()
#6) Bin hours into buckets (e.g., 0–99, 100–199, 200+)
bucketed = workload_df.withColumn(
    "hours_bucket",
    when(col("hours") < 100, lit("0-99"))
     .when(col("hours") < 200, lit("100-199"))
     .otherwise(lit("200+"))
)
bucket_dist = bucketed.groupBy("project", "hours_bucket").count()
bucket_dist.show()
#7) Normalize hours within each project (z-score per project)
w_proj = Window.partitionBy("project")
stats_df = (
    workload_df
    .withColumn("mean_h", avg("hours").over(w_proj))
    .withColumn("std_h", stddev_pop("hours").over(w_proj))
    .withColumn("z_hours", (col("hours") - col("mean_h")) / col("std_h"))
)

#8) Percentile bands per project (approxQuantile + join)
# Compute per-project 25/50/75 percentiles, then tag rows
percentiles = (
    workload_df
    .groupBy("project")
    .agg(
        expr("percentile_approx(hours, array(0.25,0.5,0.75), 100)").alias("q")
    )
    .select(
        "project",
        col("q")[0].alias("p25"),
        col("q")[1].alias("p50"),
        col("q")[2].alias("p75"),
    )
)
percentiles.show()
with_p = workload_df.join(percentiles, "project")
banded = with_p.withColumn(
    "hours_band",
    when(col("hours") <= col("p25"), "0–25th")
     .when(col("hours") <= col("p50"), "25–50th")
     .when(col("hours") <= col("p75"), "50–75th")
     .otherwise("75–100th")
)
banded.show()
#9) Find duplicates by (name, project) and keep latest/heaviest
w_dupe = Window.partitionBy("name", "project").orderBy(col("hours").desc())
dedup = (
    workload_df
    .withColumn("rn", row_number().over(w_dupe))
    .filter(col("rn") == 1)
    .drop("rn")
)
dedup.show()
#10) Crosstab: quick contingency table of project vs status
crosstab = workload_df.crosstab("project", "workload_status")  # column name becomes 'project_workload_status'
crosstab.show()

#11) Rollup/Cube: multi-level aggregates (all-project totals too)
rollup_hours = workload_df.rollup("project", "workload_status").agg(sum("hours").alias("total_hours"))
cube_counts  = workload_df.cube("project", "workload_status").agg(count("*").alias("cnt"))
cube_counts.show()
#12) Flag extreme outliers using IQR per project
# compute Q1/Q3 per project
iqr = (
    workload_df.groupBy("project")
      .agg(expr("percentile_approx(hours, 0.25, 100)").alias("q1"),
           expr("percentile_approx(hours, 0.75, 100)").alias("q3"))
      .withColumn("iqr", col("q3") - col("q1"))
)
with_iqr = workload_df.join(iqr, "project")
outliers = with_iqr.withColumn(
    "is_outlier",
    (col("hours") > col("q3") + 1.5*col("iqr")) | (col("hours") < col("q1") - 1.5*col("iqr"))
)
outliers.show()

#13) Per-person summary and rank across all projects
per_person = (
    workload_df.groupBy("name")
    .agg(sum("hours").alias("total_hours"), count("*").alias("assignments"))
)
w_total = Window.orderBy(col("total_hours").desc())
ranked_people = per_person.withColumn("rank_by_hours", dense_rank().over(w_total))
ranked_people.show()
#14) Even distribution check: share of category within total
total_hours = workload_df.agg(sum("hours").alias("grand_total")).collect()[0]["grand_total"]
share = workload_df.groupBy("project").agg((sum("hours")/lit(total_hours)).alias("share_of_hours"))
share.show()

#15) Label ordering for tidy sorts (status priority)
status_order = create_map(
    [lit("Underutilized"), lit(1),
     lit("Balanced"), lit(2),
     lit("Overloaded"), lit(3)]
)
ordered = workload_df.orderBy(status_order[col("workload_status")], col("hours").desc())
ordered.show()
