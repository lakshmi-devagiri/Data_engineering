from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\office_users_in_out_data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()
# 2) Types + derive date
df = (df
    .withColumn("user_in_ts",  to_timestamp("user_in"))
    .withColumn("user_out_ts", to_timestamp("user_out"))
    .withColumn("work_date",   to_date("user_in_ts"))
)


# 3) Per-session duration (seconds)
df = df.withColumn("session_secs", unix_timestamp("user_out_ts") - unix_timestamp("user_in_ts"))

# (Optional DQ) keep only valid positive sessions
df_valid = df.where(col("session_secs") > 0)
df_valid.show()
# 4) Per-user-per-day aggregates
daily = (df_valid.groupBy("user_id", "user_name", "work_date")
    .agg(
        sum("session_secs").alias("net_secs"),
        min("user_in_ts").alias("first_in"),
        max("user_out_ts").alias("last_out"),
        count(lit(1)).alias("sessions")
    )
    .withColumn("total_secs", unix_timestamp("last_out") - unix_timestamp("first_in"))
    .withColumn("break_secs", col("total_secs") - col("net_secs"))
)
daily.show()
# 5) Pretty HH:MM:SS formatting
def fmt(col_secs):
    # convert seconds to HH:MM:SS using Spark SQL
    return concat_ws(":",
        lpad((col(col_secs) / 3600).cast("int"), 2, "0"),
        lpad(((col(col_secs) % 3600) / 60).cast("int"), 2, "0"),
        lpad((col(col_secs) % 60).cast("int"), 2, "0")
    )

report = (daily
    .select(
        "user_id", "user_name", "work_date",
        fmt("net_secs").alias("net_time"),
        fmt("total_secs").alias("total_time"),
        fmt("break_secs").alias("break_time"),
        "sessions", "first_in", "last_out"
    )
    .orderBy("work_date", "user_id")
)

# Show
report.show(truncate=False)


print("\n" + "=" * 80)
print("TASK 1–5 REPORT: Work / Total / Break Time per User per Day")
print("=" * 80)
report.show(truncate=False)

# =============================================================================
# EXTRA REAL-TIME STYLE TASKS (6–10)
# =============================================================================

# -----------------------------------------------------------------------------
# TASK 6: Flag overtime / under-time per day per user
# -----------------------------------------------------------------------------
# Business rule (example):
#   - Standard work day = 8 hours (8 * 3600 sec)
#   - Overtime          = net_secs > 9 hours
#   - Under-time        = net_secs < 7 hours
#
# This helps HR/operations quickly see who is overworking or underworking.
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("TASK 6: Overtime / Under-time Flags per User per Day")
print("=" * 80)

daily_flags = (
    daily
    .withColumn(
        "overtime_flag",
        when(col("net_secs") > 9 * 3600, lit("Yes")).otherwise(lit("No"))
    )
    .withColumn(
        "under_time_flag",
        when(col("net_secs") < 7 * 3600, lit("Yes")).otherwise(lit("No"))
    )
)

task6 = (
    daily_flags
    .select(
        "user_id",
        "user_name",
        "work_date",
        fmt("net_secs").alias("net_time"),
        "sessions",
        "overtime_flag",
        "under_time_flag"
    )
    .orderBy("work_date", "user_id")
)

task6.show(truncate=False)

# -----------------------------------------------------------------------------
# TASK 7: Top 5 longest-working days (user + date)
# -----------------------------------------------------------------------------
# Question:
#   "Which 5 user-days had the maximum net working time?"
#
# This is useful for:
#   - spotting potential burnout
#   - investigating days with very long hours
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("TASK 7: Top 5 Longest Working Days (by net_secs)")
print("=" * 80)

task7 = (
    daily
    .orderBy(col("net_secs").desc())
    .limit(5)
    .select(
        "user_id",
        "user_name",
        "work_date",
        fmt("net_secs").alias("net_time"),
        fmt("break_secs").alias("break_time"),
        "sessions",
        "first_in",
        "last_out"
    )
)

task7.show(truncate=False)

# -----------------------------------------------------------------------------
# TASK 8: Longest break per user per day
# -----------------------------------------------------------------------------
# Question:
#   "Within a day, what is the longest gap between a user's OUT and next IN?"
#
# Steps:
#   1) Sort sessions by user_id + work_date + user_in_ts
#   2) Use LEAD(user_in_ts) to get the next session's IN time
#   3) gap_secs = next_in_ts - current user_out_ts
#   4) Take max gap_secs per user per date
#
# This helps to:
#   - detect long lunch breaks or extended time outside
#   - enforce break policies
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("TASK 8: Longest Break per User per Day")
print("=" * 80)

w_gap = Window.partitionBy("user_id", "work_date").orderBy("user_in_ts")

df_gaps = (
    df_valid
    .withColumn("next_in_ts", lead("user_in_ts").over(w_gap))
    .withColumn(
        "gap_secs",
        unix_timestamp("next_in_ts") - unix_timestamp("user_out_ts")
    )
)

# Keep only positive gaps (where a next session exists and is after user_out_ts)
df_gaps_valid = df_gaps.where(col("gap_secs") > 0)

longest_break = (
    df_gaps_valid
    .groupBy("user_id", "user_name", "work_date")
    .agg(max("gap_secs").alias("max_break_secs"))
)

task8 = (
    longest_break
    .select(
        "user_id",
        "user_name",
        "work_date",
        fmt("max_break_secs").alias("max_break_time")
    )
    .orderBy("work_date", "user_id")
)

task8.show(truncate=False)

# -----------------------------------------------------------------------------
# TASK 9: Average check-in time per user (early vs late)
# -----------------------------------------------------------------------------
# Question:
#   "What is the typical first IN time for each user?"
#
# Idea:
#   - Convert first_in (timestamp) to "seconds since midnight"
#   - Average that per user to get typical check-in time
#   - Optionally categorize:
#       * < 09:00 → Early Bird
#       * 09:00–10:00 → Regular
#       * > 10:00 → Late Comer
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("TASK 9: Average Check-in Time per User (and Category)")
print("=" * 80)

daily_with_first_in_sec = (
    daily
    .withColumn(
        "first_in_sec",
        hour("first_in") * 3600 + minute("first_in") * 60 + second("first_in")
    )
)

avg_in = (
    daily_with_first_in_sec
    .groupBy("user_id", "user_name")
    .agg(avg("first_in_sec").alias("avg_first_in_sec"))
)

# Helper formatter for seconds since midnight (HH:MM:SS)
def fmt_time_of_day(col_secs: str):
    return concat_ws(
        ":",
        lpad((col(col_secs) / 3600).cast("int"), 2, "0"),
        lpad(((col(col_secs) % 3600) / 60).cast("int"), 2, "0"),
        lpad((col(col_secs) % 60).cast("int"), 2, "0")
    )

task9 = (
    avg_in
    .withColumn("avg_first_in_time", fmt_time_of_day("avg_first_in_sec"))
    .withColumn(
        "arrival_category",
        when(col("avg_first_in_sec") < 9 * 3600, "Early Bird")
        .when(col("avg_first_in_sec") <= 10 * 3600, "Regular")
        .otherwise("Late Comer")
    )
    .select("user_id", "user_name", "avg_first_in_time", "arrival_category")
    .orderBy("user_id")
)

task9.show(truncate=False)

# -----------------------------------------------------------------------------
# TASK 10: Daily office utilization summary
# -----------------------------------------------------------------------------
# Question:
#   "Per date, how many unique users came, and what was the total & average
#    net working time?"
#
# Steps:
#   - From `daily`, group by work_date
#   - unique_users  = countDistinct(user_id)
#   - total_net_secs = sum(net_secs)
#   - avg_net_secs   = avg(net_secs)
#
# Use HH:MM:SS formatting for total & average.
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("TASK 10: Daily Office Utilization Summary")
print("=" * 80)

daily_summary = (
    daily
    .groupBy("work_date")
    .agg(
        countDistinct("user_id").alias("unique_users"),
        sum("net_secs").alias("total_net_secs"),
        avg("net_secs").alias("avg_net_secs")
    )
)

task10 = (
    daily_summary
    .select(
        "work_date",
        "unique_users",
        fmt("total_net_secs").alias("total_net_time"),
        fmt("avg_net_secs").alias("avg_net_time_per_user")
    )
    .orderBy("work_date")
)

task10.show(truncate=False)

print("\n" + "=" * 80)
print("✓ All tasks (1–10) completed successfully")
print("=" * 80)