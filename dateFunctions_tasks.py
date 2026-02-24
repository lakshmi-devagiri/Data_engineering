from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
# python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "IST")

# load sample data (adjust path)
df = spark.read.option("header", True).option("inferSchema", True).csv(r"D:\bigdata\drivers\donations.csv")

# Robust parse: try multiple date formats, keep parse_success flag
df = df.withColumn("dt",
    F.coalesce(
        F.to_date("dt", "d-M-yyyy"),
        F.to_date("dt", "dd-MM-yyyy"),
        F.to_date("dt", "yyyy-MM-dd"),
        F.to_date("dt")  # generic
    )
).withColumn("parse_ok", F.col("dt").isNotNull())

# 1) Calendar & month logic
# EOM and previous EOM (use add_months)
df = df.withColumn("eom", F.last_day("dt")) \
       .withColumn("prev_eom", F.last_day(F.add_months("dt", -1)))

# Same day next month and business-roll to previous Friday when landing on weekend
df = df.withColumn("same_day_next", F.add_months("dt", 1)) \
       .withColumn("same_day_next_roll",
           F.when(F.dayofweek(F.col("same_day_next")).isin(1,7),  # Sun=1 Sat=7
                  F.when(F.dayofweek("same_day_next") == 1, F.date_sub("same_day_next", 2)).otherwise(F.date_sub("same_day_next",1))
                 ).otherwise(F.col("same_day_next"))
       )

# Semi-monthly payroll: 15th and last working day (Fri rollback if weekend)
df = df.withColumn("pay_15", F.make_date(F.year("dt"), F.month("dt"), F.lit(15)))
df = df.withColumn("lastday", F.last_day("dt"))
df = df.withColumn("last_working_day",
    F.when(F.dayofweek("lastday") == 1, F.date_sub("lastday", 2))  # Sunday -> Fri
     .when(F.dayofweek("lastday") == 7, F.date_sub("lastday", 1))  # Saturday -> Fri
     .otherwise(F.col("lastday"))
)

# 2) Nth weekday of month (generalized)
def nth_weekday_expr(date_col, n, wd_name):
    # next_day returns next occurrence AFTER given start; compute month_start then first wd then add 7*(n-1)
    month_start = F.trunc(F.col(date_col), "month")
    first_wd = F.next_day(month_start, wd_name)      # first occurrence after month_start
    nth = F.date_add(first_wd, (n - 1) * 7)
    return F.when(F.month(nth) == F.month(month_start), nth).otherwise(F.lit(None).cast(DateType()))

df = df.withColumn("3rd_wed", nth_weekday_expr("dt", 3, "Wed"))

# 3) Days in month and % progress
df = df.withColumn("days_in_month", F.dayofmonth(F.last_day("dt"))) \
       .withColumn("day_progress", F.col("dt").cast("date").substr(9,2).cast(IntegerType()) / F.col("days_in_month"))
# safer dayofmonth(dt) / days_in_month:
df = df.withColumn("day_progress2", F.dayofmonth("dt") / F.col("days_in_month"))

# 4) ISO week boundaries
df = df.withColumn("iso_week", F.weekofyear("dt")) \
       .withColumn("iso_week_start", F.date_trunc("week", F.col("dt"))) \
       .withColumn("iso_week_end", F.date_add(F.col("iso_week_start"), 6))

# 5) Last Monday of month
df = df.withColumn("last_mon", F.next_day(F.date_add(F.last_day("dt"), -7), "Mon"))

# 6) First business day of next month (shift to Mon if weekend)
next_month_start = F.add_months(F.trunc(F.col("dt"), "month"), 1)
df = df.withColumn("first_next_month",
    F.when(F.dayofweek(next_month_start) == 1, F.date_add(next_month_start, 1))
     .when(F.dayofweek(next_month_start) == 7, F.date_add(next_month_start, 2))
     .otherwise(next_month_start)
)

# 7) Indian FY and FY quarter mapping + quarter start/end & progress
fy_year = F.when(F.month("dt") >= 4, F.year("dt")).otherwise(F.year("dt") - 1)
# FY quarter: map month -> FY quarter (Apr-Jun = Q1, Jul-Sep=Q2, Oct-Dec=Q3, Jan-Mar=Q4)
fy_qtr = ((((F.month("dt") + 8) % 12) / 3).cast(IntegerType()) + 1)
df = df.withColumn("fy_year", fy_year).withColumn("fy_quarter", fy_qtr)

# Quarter start (calendar quarter) and for FY-quarter start adjust months
# compute FY quarter start month: for each month compute start by shifting months to FY base
# find q_start by: compute month offset within FY, then derive start month
def fy_q_start_expr(col):
    # compute year start for FY
    year_start = F.make_date(F.when(F.month(col) >= 4, F.year(col)).otherwise(F.year(col)-1), F.lit(4), F.lit(1))
    # offset months since Apr: months_between(col, year_start) floor to get month index, qindex = floor(idx/3)
    idx = (F.month(col) - 4) % 12
    qidx = (F.floor((F.month(col) + 8) / 3) - 1)  # simpler: reuse fy_qtr - 1
    # compute q_start by add_months to the FY year start: (fy_quarter-1)*3 months
    return F.add_months(F.make_date(F.when(F.month(col) >= 4, F.year(col)).otherwise(F.year(col)-1), F.lit(4), F.lit(1)),
                        ((F.col("fy_quarter") - 1) * 3))

# compute q_start and q_end and q_progress
df = df.withColumn("q_start",
                   F.trunc(F.add_months(F.trunc("dt", "month"), -(F.month("dt") - 1) % 3), "month")
                  )
df = df.withColumn("q_end", F.last_day(F.add_months(F.col("q_start"), 2)))
df = df.withColumn("q_days", F.datediff(F.add_months(F.col("q_start"), 3), F.col("q_start"))) \
       .withColumn("q_progress", F.datediff(F.col("dt"), F.col("q_start")) / F.col("q_days"))

# 8) Holiday calendar and business-day flags + next_business_day (precompute calendar)
holidays = spark.createDataFrame([("2025-01-26",),("2025-08-15",),("2025-10-02",)], ["hd"]) \
                .select(F.to_date("hd").alias("hd"))
minmax = df.select(F.min("dt").alias("min_d"), F.max("dt").alias("max_d")).first()
if minmax.min_d is not None and minmax.max_d is not None:
    cal = (spark.range(1).select(F.sequence(F.lit(minmax.min_d), F.lit(minmax.max_d)).alias("d"))
           .select(F.explode("d").alias("d")))
    cal = cal.withColumn("dow", F.dayofweek("d")) \
             .withColumn("is_weekend", F.col("dow").isin(1,7)) \
             .join(holidays.select(F.col("hd")), cal.d == holidays.hd, "left") \
             .withColumn("is_holiday", F.col("hd").isNotNull()) \
             .drop("hd") \
             .withColumn("is_business_day", ~(F.col("is_weekend") | F.col("is_holiday")))
    # next business day for each dt: join calendar entries where d > dt and is_business_day, pick min(d)
    nxt = cal.filter("is_business_day").select(F.col("d").alias("biz_date"))
    # join using condition and aggregate min
    nxt_join = df.select("dt").join(nxt, nxt.biz_date > F.col("dt"), how="left") \
                 .groupBy("dt").agg(F.min("biz_date").alias("next_business_day"))
    df = df.join(nxt_join, on="dt", how="left")
else:
    df = df.withColumn("next_business_day", F.lit(None).cast(DateType()))

# 9) Business-day difference (count working days between dt and dt2) — example using dt and eom
if minmax.min_d is not None and minmax.max_d is not None:
    cal_bus = cal.filter("is_business_day").select(F.col("d").alias("d"))
    # cross-join count using aggregation: for each row count calendar.d between dt and eom
    cal_bus_count = df.select("dt", "eom").join(cal_bus, (cal_bus.d >= F.col("dt")) & (cal_bus.d <= F.col("eom")), how="left") \
                      .groupBy("dt", "eom").agg(F.count("d").alias("business_days_between"))
    df = df.join(cal_bus_count, on=["dt","eom"], how="left")
else:
    df = df.withColumn("business_days_between", F.lit(None).cast(IntegerType()))

# 10) Rolling 7-day sum per user (example uses a synthetic events table from df)
events = df.select(F.monotonically_increasing_id().alias("user_id"), "dt").withColumn("amount", F.lit(1))
# Order by unix timestamp and rangeBetween in seconds
events = events.withColumn("ts_unix", F.unix_timestamp(F.col("dt")))
w = Window.partitionBy("user_id").orderBy("ts_unix").rangeBetween(-7 * 86400, 0)
events = events.withColumn("rolling_7d_count", F.sum("amount").over(w))

# 11) MTD & QTD cumulative sums example (per user_id)
w_mtd = Window.partitionBy("user_id", F.trunc("dt", "month")).orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)
events = events.withColumn("mtd_count", F.sum("amount").over(w_mtd))
# QTD: compute quarter start then partition by that
events = events.withColumn("q_start", F.trunc(F.add_months(F.trunc("dt", "month"), -(F.month("dt") - 1) % 3), "month"))
w_qtd = Window.partitionBy("user_id", "q_start").orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)
events = events.withColumn("qtd_count", F.sum("amount").over(w_qtd))

# 12) SLA due date (approx): add 12 business hours assuming 8-hour day (10:00-18:00 window)
def sla_add_hours_expr(created_ts_col, sla_hours=12):
    # split into whole business days + remainder hours
    whole_days = F.floor(F.lit(sla_hours) / F.lit(8)).cast(IntegerType())
    rem_hours = F.lit(sla_hours) - whole_days * 8
    # compute base_day by adding whole_days business days via next_business_day repeated approx by calendar
    # For demo: find the date after adding whole_days by selecting calendar.d > created_date and is_business_day and pick nth
    # This is complex without UDF; approximate by adding whole_days calendar days then finding next_business_day
    base_date = F.date_add(F.to_date(created_ts_col), whole_days)
    # set time part by adding rem_hours to business start 10:00
    base_ts = F.to_timestamp(F.concat(F.date_format(base_date, "yyyy-MM-dd"), F.lit(" 10:00:00")))
    due_ts = F.expr(f"timestampadd(HOUR, {int(rem_hours)}, {base_ts._jc.toString()})") if False else base_ts  # placeholder (can't easily compose in API)
    return base_date  # demo placeholder (real implementation would join calendar to roll to next business day and add hours)

# add SLA placeholder column (demo)
df = df.withColumn("sla_due_date_demo", sla_add_hours_expr(F.current_timestamp(), 12))

# 13) Timezones: convert local IST timestamp to UTC and back
df = df.withColumn("ts_local", F.current_timestamp()) \
       .withColumn("ts_utc", F.to_utc_timestamp("ts_local", "Asia/Kolkata")) \
       .withColumn("ts_back_ist", F.from_utc_timestamp("ts_utc", "Asia/Kolkata"))

# 14) Recurrence generation: monthly debit schedule for 12 months with weekend roll rule
start = F.col("dt")
schedule = F.expr("transform(sequence(0,11), i -> add_months(dt, i))")  # using SQL expr for transform
df = df.withColumn("raw_schedule", schedule)
# apply weekend roll to each element (map): use transform with conditional (SQL string)
df = df.withColumn("schedule_safe",
    F.expr(
        "transform(raw_schedule, d -> "
        "case when dayofweek(d) = 1 then date_sub(d,2) "
        "when dayofweek(d) = 7 then date_sub(d,1) else d end)"
    )
)

# 15) Period labels and completeness check (last 6 months per donor)
df = df.withColumn("month_label", F.date_format("dt", "MMM-yy")) \
       .withColumn("fy_label", F.concat(F.lit("FY"), F.col("fy_year"), F.lit("-"), (F.col("fy_year") + 1)))
# Example completeness: build 6-month sequence per row and compare actual months (demo per donor_id if available)
# Build months_seq
df = df.withColumn("months_seq", F.sequence(F.add_months(F.trunc("dt", "month"), -5), F.trunc("dt", "month"), F.expr("interval 1 month")))

# Show a short sample of important columns
print("--- sample output ---")
df.select("dt", "eom", "prev_eom", "same_day_next", "same_day_next_roll", "pay_15", "last_working_day",
          "3rd_wed", "days_in_month", "day_progress2", "iso_week", "iso_week_start", "iso_week_end",
          "last_mon", "first_next_month", "fy_year", "fy_quarter", "q_start", "q_end", "business_days_between",
          "next_business_day").show(5, truncate=False)

# show schedule and payroll examples
df.select("dt", "raw_schedule", "schedule_safe").show(3, truncate=False)

# Events demo
events.select("user_id", "ts_unix", "rolling_7d_count", "mtd_count", "qtd_count").show(5, truncate=False)

# end
spark.stop()