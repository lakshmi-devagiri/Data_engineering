# ================================================================
# Databricks/Local PySpark – Advanced Date Tasks (12 in 1)
# SPOONFEEDING STYLE: Each task has a clear problem statement.
# ================================================================

from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.types import *

spark = SparkSession.builder.appName("date_tasks_hr_ops").master("local[*]").getOrCreate()

# ----------------------------------------------------------------
# INPUTS: your 3 CSVs (keep as-is, change paths if needed)
# ----------------------------------------------------------------
df_emp = (spark.read.option("header", True)
    .csv(r"D:\bigdata\drivers\employees_offers.csv"))

df_holidays = (spark.read.option("header", True)
    .csv(r"D:\bigdata\drivers\holidays_in.csv")
    .withColumn("holiday_date", F.to_date("holiday_date", "yyyy-MM-dd"))
)

df_logs = (spark.read.option("header", True)
    .csv(r"D:\bigdata\drivers\time_logs.csv")
    .withColumn("work_date", F.to_date("work_date"))
    .withColumn("clock_in_ts", F.to_timestamp("clock_in_ts"))
    .withColumn("clock_out_ts", F.to_timestamp("clock_out_ts"))
    .withColumn("billable_hours_raw", F.col("billable_hours_raw").cast("double"))
)

# ===== Task 0 (Prep): Parse messy dates, cast numerics, mark parse format =====
# PROBLEM: dob_str / offer_date_str / joining_date_str are in mixed formats.
# GOAL: Produce clean date columns dob, offer_date, joining_date; cast number columns;
#       add parse_format_used to record which pattern matched for joining_date.
patterns = ["dd-MM-yyyy", "MM/dd/yyyy", "yyyy/MM/dd", "yyyy-MM-dd"]
def first_date(colname):
    return F.coalesce(*[F.to_date(F.col(colname), p) for p in patterns])

df_emp = (df_emp
    .withColumn("dob", first_date("dob_str"))
    .withColumn("offer_date", first_date("offer_date_str"))
    .withColumn("joining_date", first_date("joining_date_str"))
    .withColumn("probation_months", F.col("probation_months").cast("int"))
    .withColumn("notice_period_days", F.col("notice_period_days").cast("int"))
    .withColumn("expected_ctc_lpa", F.col("expected_ctc_lpa").cast("double"))
    .withColumn("parse_format_used",
        F.when(F.to_date("joining_date_str","dd-MM-yyyy").isNotNull(),"dd-MM-yyyy")
         .when(F.to_date("joining_date_str","MM/dd/yyyy").isNotNull(),"MM/dd/yyyy")
         .when(F.to_date("joining_date_str","yyyy/MM/dd").isNotNull(),"yyyy/MM/dd")
         .when(F.to_date("joining_date_str","yyyy-MM-dd").isNotNull(),"yyyy-MM-dd")
         .otherwise("UNKNOWN")
    )
)

df_hol = df_holidays.select("holiday_date","holiday_name")  # already parsed above

# ===== Task -1 (Helper): Build a working-days calendar (no weekends/holidays) =====
# PROBLEM: Several tasks need "business days". We need a calendar of workdays.
# STEPS:
#   1) Create a date sequence from (min join - 1) to (max join + 120).
#   2) Remove holidays.
#   3) Remove Saturdays/Sundays.
bounds = df_emp.select(
    F.date_sub(F.min("joining_date"), 1).alias("min_d"),
    F.date_add(F.max("joining_date"), 120).alias("max_d")
).first()

calendar = (spark.range(1)
  .select(F.sequence(F.lit(bounds["min_d"]), F.lit(bounds["max_d"])).alias("arr"))
  .select(F.explode(F.col("arr")).alias("d"))
)

workdays = (calendar
  .join(df_hol.withColumnRenamed("holiday_date","d"), ["d"], "left_anti")
  .withColumn("dow", F.date_format("d","E"))
  .filter(~F.col("dow").isin("Sat","Sun"))
  .drop("dow")
  .cache()
)

# ===== Task 1: Offer → Join gap & status =====
# PROBLEM: Compute days between offer_date and joining_date.
# RULES:
#   gap < 0  => "Invalid (Joined before offer!)"
#   gap <=20 => "OnTime"
#   else     => "Late Join"
emp1 = (df_emp
  .withColumn("offer_to_join_gap", F.datediff("joining_date","offer_date"))
  .withColumn("joining_status",
      F.when(F.col("offer_to_join_gap") < 0, "Invalid (Joined before offer!)")
       .when(F.col("offer_to_join_gap") <= 20, "OnTime")
       .otherwise("Late Join")
  )
)

# ===== Task 2: Probation end date (30-day months) + shift to next workday =====
# PROBLEM: probation_end_date_original = joining_date + probation_months*30.
# If it lands on weekend/holiday, shift forward to the next workday.
emp2 = (emp1
  .withColumn("probation_end_date_original",
              F.date_add(F.col("joining_date"), F.col("probation_months")*F.lit(30)))
)

emp2_adj = (emp2
  .withColumn("cand",
              F.sequence(F.col("probation_end_date_original"),
                         F.date_add(F.col("probation_end_date_original"), 14)))
  .withColumn("cand", F.explode("cand"))
  .join(workdays.withColumnRenamed("d","cand"), ["cand"], "inner")
  .withColumn("rn", F.row_number().over(W.partitionBy("emp_id").orderBy("cand")))
  .filter("rn = 1")
  .withColumnRenamed("cand","adjusted_probation_end_date")
  .drop("rn")
)

# ===== Task 3: 5th business day after joining =====
# PROBLEM: Find the 5th working day (Mon–Fri, not holiday) strictly after joining_date.
fifth_biz = (df_emp
  .select("emp_id","joining_date")
  .withColumn("cand", F.sequence(F.date_add("joining_date",1), F.date_add("joining_date",30)))
  .withColumn("cand", F.explode("cand"))
  .join(workdays.withColumnRenamed("d","cand"), ["cand"], "inner")
  .withColumn("biz_rank", F.row_number().over(W.partitionBy("emp_id","joining_date").orderBy("cand")))
  .filter("biz_rank = 5")
  .select("emp_id","joining_date", F.col("cand").alias("fifth_business_day"))
)

# ===== Task 4: Monthly cohorts (bucket by joining month) =====
# PROBLEM: Group employees by joining month; count how many joined.
cohorts = (df_emp
  .withColumn("cohort_month", F.date_format(F.date_trunc("month","joining_date"), "yyyy-MM"))
  .groupBy("cohort_month").count()
)

# ===== Task 5: Round/floor/ceil billable hours =====
# PROBLEM: From time_logs, create rounded_half (nearest 0.5), floor_hours, ceil_hours.
logs_round = (df_logs
  .withColumn("rounded_half", F.round(F.col("billable_hours_raw")*2)/2)
  .withColumn("floor_hours", F.floor("billable_hours_raw"))
  .withColumn("ceil_hours", F.ceil("billable_hours_raw"))
)

# ===== Task 6: First work_date on/after joining + first clock-in =====
# PROBLEM: For each emp, find the first work_date ≥ joining_date and its first clock-in.
first_work = (df_logs.alias("l")
  .join(df_emp.select("emp_id","joining_date").alias("e"), "emp_id")
  .filter(F.col("l.work_date") >= F.col("e.joining_date"))
  .withColumn("rn", F.row_number().over(W.partitionBy("emp_id").orderBy("l.work_date","l.clock_in_ts")))
  .filter("rn = 1")
  .select(F.col("emp_id"),
          F.col("l.work_date").alias("first_work_date"),
          F.col("l.clock_in_ts").alias("first_clock_in"))
)

# ===== Task 7: Weekend joiners flag =====
# PROBLEM: Mark joinings on Saturday/Sunday for manual HR check.
weekend_joiners = (df_emp
  .withColumn("joining_day_name", F.date_format("joining_date","E"))
  .withColumn("needs_manual_check", F.when(F.col("joining_day_name").isin("Sat","Sun"), "Y").otherwise("N"))
  .select("emp_id","name","joining_date","joining_day_name","needs_manual_check")
)

# ===== Task 8: Monthly CTC & ₹500 slab rounding =====
# PROBLEM: Monthly CTC = expected_ctc_lpa * 100000 / 12; round up to nearest 500.
monthly_pay = (df_emp
  .withColumn("monthly_ctc_raw", (F.col("expected_ctc_lpa")*100000)/12 )
  .withColumn("monthly_ctc_rounded", F.ceil(F.col("monthly_ctc_raw")/500)*500)
  .select("emp_id","name","expected_ctc_lpa","monthly_ctc_raw","monthly_ctc_rounded")
)

# ===== Task 9: Induction date = next Friday after joining =====
# PROBLEM: Company runs induction on the first Friday AFTER joining_date.
# NOTE: Spark next_day returns strictly after the given date (Fri -> next Fri).
induction = (df_emp
  .withColumn("induction_date", F.next_day("joining_date","FRI"))
  .select("emp_id","joining_date","induction_date")
)

# ===== Task 10: Overworked in first 3 work days (> 9h avg) =====
# PROBLEM: For each emp, from logs between [joining_date, joining_date+3], compute avg hours; flag if > 9.
first3 = (df_logs.alias("l")
  .join(df_emp.select("emp_id","joining_date").alias("e"), "emp_id")
  .filter( (F.col("l.work_date") >= F.col("e.joining_date")) &
           (F.col("l.work_date") <= F.date_add(F.col("e.joining_date"), 3)) )
  .groupBy("emp_id")
  .agg(F.round(F.avg("billable_hours_raw"),2).alias("avg_hours_first_3"))
  .withColumn("overworked_flag", F.when(F.col("avg_hours_first_3") > 9, "Y").otherwise("N"))
)

# ===== Task 11: Payroll cutoff = last working day of joining month =====
# PROBLEM: Start with last_day(joining_date). If it is a holiday/weekend,
#          move backwards to the last working weekday (Mon–Fri, not holiday).
emp_calendar = (df_emp
  .withColumn("join_week_start_mon", F.date_trunc("week","joining_date"))  # Monday-start
  .withColumn("join_quarter", F.date_trunc("quarter","joining_date"))
  .withColumn("month_end_join", F.last_day("joining_date"))
)

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

# ===== Task 12: Final rollup – Join everything cleanly =====
# PROBLEM: Produce one wide report with all derived fields; avoid duplicate/ambiguous columns.
monthly_pay_sel = monthly_pay.select("emp_id", "monthly_ctc_raw", "monthly_ctc_rounded")

final = (
  emp2.alias("e")                                      # base has offer_to_join_gap & joining_status
    .join(emp2_adj.select("emp_id","adjusted_probation_end_date"), "emp_id", "left")
    .join(fifth_biz, ["emp_id","joining_date"], "left")
    .join(first_work, "emp_id", "left")
    .join(weekend_joiners.select("emp_id","joining_day_name","needs_manual_check"), "emp_id", "left")
    .join(monthly_pay_sel, "emp_id", "left")
    .join(induction, ["emp_id","joining_date"], "left")
    .join(first3, "emp_id", "left")
    .join(cutoff, "emp_id", "left")
    .select(
        F.col("e.emp_id"),
        F.col("e.name"),
        F.col("e.city"),
        F.col("e.dob"),
        F.col("e.offer_date"),
        F.col("e.joining_date"),
        F.col("e.offer_to_join_gap"),
        F.col("e.joining_status"),
        F.col("e.probation_end_date_original"),
        F.col("adjusted_probation_end_date"),
        F.col("fifth_business_day"),
        F.col("first_work_date"),
        F.col("first_clock_in"),
        F.col("joining_day_name"),
        F.col("needs_manual_check"),
        F.col("e.expected_ctc_lpa"),
        F.col("monthly_ctc_raw"),
        F.col("monthly_ctc_rounded"),
        F.col("induction_date"),
        F.col("avg_hours_first_3"),
        F.col("overworked_flag"),
        F.col("payroll_cutoff")
    )
)

# ----------------------------------------------------------------
# SHOW RESULTS (you can swap to display() on Databricks)
# ----------------------------------------------------------------
print("=== FINAL REPORT ===")
final.orderBy("emp_id").show(truncate=False)

print("=== COHORTS (JOINING MONTH COUNTS) ===")
cohorts.orderBy("cohort_month").show(truncate=False)

print("=== LOGS ROUNDING ===")
logs_round.orderBy("emp_id","work_date").show(truncate=False)
