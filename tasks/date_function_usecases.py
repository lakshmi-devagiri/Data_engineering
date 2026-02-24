# python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.master("local[*]").appName("date_examples").getOrCreate()

# Sample inputs
df_emp = spark.read.format("csv").option("header","true").option("inferSchema","true").load(r"D:\bigdata\drivers\emp_joining_formalities.csv") \
  .withColumn("joining_date", F.to_date("joining_date")) \
  .withColumn("offer_date", F.to_date("offer_date"))

df_hol = spark.createDataFrame([
    ("2025-01-26","Republic Day")
], schema=["holiday_date","holiday_name"]).withColumn("holiday_date", F.to_date("holiday_date"))
logs=r"D:\bigdata\drivers\emp_billable_time_logs.csv"
df_logs = spark.read.format("csv").option("header","true").option("inferSchema","true").load(logs) \
  .withColumn("work_date", F.to_date("work_date")) \
  .withColumn("clock_in_ts", F.to_timestamp("clock_in_ts"))

# 1) datediff + rule-based labels
emp_gap = df_emp.withColumn("offer_to_join_gap", F.datediff("joining_date","offer_date")) \
    .withColumn("joining_status",
        F.when(F.col("offer_to_join_gap") < 0, "Invalid (Joined before offer!)")
         .when(F.col("offer_to_join_gap") <= 20, "OnTime")
         .otherwise("Late Join")
    )
emp_gap.select("emp_id","joining_date","offer_date","offer_to_join_gap","joining_status").show(truncate=False)

# 2) date_add + business-day shift using generated calendar
# build bounds and calendar
bounds = df_emp.select(
    F.date_sub(F.min("joining_date"), 1).alias("min_d"),
    F.date_add(F.max("joining_date"), 120).alias("max_d")
).first()

calendar = (spark.range(1)
    .select(F.sequence(F.lit(bounds["min_d"]), F.lit(bounds["max_d"])).alias("arr"))
    .select(F.explode("arr").alias("d"))
)

workdays = (calendar
    .join(df_hol.withColumnRenamed("holiday_date","d"), ["d"], "left_anti")
    .withColumn("dow", F.date_format("d","E"))
    .filter(~F.col("dow").isin("Sat","Sun"))
    .drop("dow")
    .cache()
)

# probation end + shift forward to next workday (search window of +14 days)
emp_prob = (df_emp.withColumn("probation_end_original",
                F.date_add(F.col("joining_date"), F.col("probation_months")*F.lit(30)))
    .withColumn("cand", F.sequence("probation_end_original", F.date_add("probation_end_original", 14)))
    .withColumn("cand", F.explode("cand"))
    .join(workdays.withColumnRenamed("d","cand"), ["cand"], "inner")
    .withColumn("rn", F.row_number().over(W.partitionBy("emp_id").orderBy("cand")))
    .filter("rn = 1")
    .select("emp_id","probation_end_original", F.col("cand").alias("adjusted_probation_end_date"))
)
emp_prob.show(truncate=False)

# 3) nth business day after a date (no UDF) - example: 5th business day after joining
fifth_biz = (df_emp.select("emp_id","joining_date")
    .withColumn("cand", F.sequence(F.date_add("joining_date",1), F.date_add("joining_date",30)))
    .withColumn("cand", F.explode("cand"))
    .join(workdays.withColumnRenamed("d","cand"), ["cand"], "inner")
    .withColumn("biz_rank", F.row_number().over(W.partitionBy("emp_id","joining_date").orderBy("cand")))
    .filter("biz_rank = 5")
    .select("emp_id","joining_date", F.col("cand").alias("fifth_business_day"))
)
fifth_biz.show(truncate=False)

# 4) date_trunc('month') + grouping (cohorts)
cohorts = df_emp.withColumn("cohort_month", F.date_format(F.date_trunc("month","joining_date"), "yyyy-MM")) \
    .groupBy("cohort_month").count()
cohorts.orderBy("cohort_month").show(truncate=False)

# 5) round, floor, ceil on numeric hours
logs_round = df_logs.withColumn("rounded_half", F.round(F.col("billable_hours_raw")*2)/2) \
    .withColumn("floor_hours", F.floor("billable_hours_raw")) \
    .withColumn("ceil_hours", F.ceil("billable_hours_raw"))
logs_round.select("emp_id","work_date","billable_hours_raw","rounded_half","floor_hours","ceil_hours").show(truncate=False)

# 6) windowing (row_number) + first date after a threshold (find first work_date >= joining_date with hours > 9)
w = W.partitionBy("emp_id").orderBy("l.work_date")
first_over9 = (df_logs.alias("l")
    .join(df_emp.select("emp_id","joining_date").alias("e"), "emp_id")
    .filter((F.col("l.work_date") >= F.col("e.joining_date")) & (F.col("billable_hours_raw") > 9))
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .select("emp_id", F.col("l.work_date").alias("first_over9_date"), "billable_hours_raw")
)
first_over9.show(truncate=False)

# 7) date_format weekday handling (flag weekend joiners)
weekend_flag = df_emp.withColumn("joining_day_name", F.date_format("joining_date","E")) \
    .withColumn("needs_manual_check", F.when(F.col("joining_day_name").isin("Sat","Sun"), "Y").otherwise("N")) \
    .select("emp_id","joining_date","joining_day_name","needs_manual_check")
weekend_flag.show(truncate=False)

# 8) payroll math with ceil (₹500 slabs)
payroll = df_emp.withColumn("monthly_ctc_raw", (F.col("expected_ctc_lpa")*100000)/12) \
    .withColumn("monthly_ctc_rounded", F.ceil(F.col("monthly_ctc_raw")/500)*500) \
    .select("emp_id","expected_ctc_lpa","monthly_ctc_raw","monthly_ctc_rounded")
payroll.show(truncate=False)

# 9) next_day('FRI') semantics (first Friday strictly after joining_date)
induction = df_emp.withColumn("induction_date", F.next_day("joining_date","FRI")) \
    .select("emp_id","joining_date","induction_date")
induction.show(truncate=False)

# 10) time window [join, join+3] + avg + flag (>9h)
avg_first3 = (df_logs.alias("l")
    .join(df_emp.select("emp_id","joining_date").alias("e"), "emp_id")
    .filter((F.col("l.work_date") >= F.col("e.joining_date")) & (F.col("l.work_date") <= F.date_add(F.col("e.joining_date"),3)))
    .groupBy("emp_id")
    .agg(F.round(F.avg("billable_hours_raw"),2).alias("avg_hours_first_3"))
    .withColumn("overworked_flag", F.when(F.col("avg_hours_first_3") > 9, "Y").otherwise("N"))
)
avg_first3.show(truncate=False)

# 11) last_day + backtracking to last working day (payroll cutoff)
emp_calendar = df_emp.withColumn("month_end_join", F.last_day("joining_date"))
cutoff = (emp_calendar
    .withColumn("cand", F.sequence(F.date_sub("month_end_join",7), F.col("month_end_join")))
    .withColumn("cand", F.explode("cand"))
    .join(df_hol.withColumnRenamed("holiday_date","cand"), ["cand"], "left_anti")
    .withColumn("dow", F.date_format("cand","E"))
    .filter(~F.col("dow").isin("Sat","Sun"))
    .withColumn("rn", F.row_number().over(W.partitionBy("emp_id").orderBy(F.col("cand").desc())))
    .filter("rn = 1")
    .select("emp_id", F.col("cand").alias("payroll_cutoff"))
)
cutoff.show(truncate=False)

# stop session if desired
# spark.stop()