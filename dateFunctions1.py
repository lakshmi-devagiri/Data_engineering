from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "IST")
data = r"D:\bigdata\drivers\donations.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)

# explain the original chain: dt => last_day => date_add(...,-7) => next_day(...,'Fri')
# next_day(date_add(last_day(col("dt")),-7), "Fri")
# Breakdown:
#  - col("dt") : the original date column (we convert strings to date with to_date beforehand)
#  - last_day(col("dt")) : returns the last date of the month for the given date, e.g., 2025-10-31
#  - date_add(..., -7) : subtracts 7 days from that last-day value; this gives a date roughly one week before month-end.
#    We subtract 7 days to ensure the resulting base date is strictly before the final week; this helps when the last day
#    itself falls on the requested weekday, because next_day returns the next occurrence AFTER the given base date.
#  - next_day(base_date, 'Fri') : returns the next Friday AFTER base_date. Since base_date was last_day - 7,
#    the next Friday will fall in the last week of the month — effectively giving the last Friday of the month
#    (or the Friday immediately following base_date which will be in the final 7-day window).
# Why this pattern? next_day returns the next occurrence of the weekday after the provided date (exclusive). If you want
# the last X-day of the month (e.g. last Friday), a reliable pattern is:
#    next_day(date_add(last_day(dt), -7), 'Fri')
# This works across months regardless of which weekday the last day falls on.

# Updated DataFrame with more robust date parsing and additional examples
# Use multiple to_date formats to coerce various input layouts (safe fallback order)
df = (
    df
    .withColumn("dt", coalesce(
        to_date(col("dt"), "d-M-yyyy"),
        to_date(col("dt"), "dd-MM-yyyy"),
        to_date(col("dt"), "yyyy-MM-dd"),
        to_date(col("dt"))
    ))
    .withColumn("today", current_date())
    .withColumn("dtdiff", datediff(col("today"), col("dt")))
    .withColumn("adddate", date_add(col("today"), 100))
    .withColumn("dateadd1", date_add(col("today"), -100))
    .withColumn("dtformat", date_format(col("dt"), "dd-MMMM-yy-EEEE"))
    .withColumn("ts", current_timestamp())
    .withColumn("datetrunc", date_trunc("hour", col("ts")))
    .withColumn("lastday", last_day(col("dt")))
    .withColumn("nextday", next_day(col("today"), "Wed"))
    # original salday logic: last Friday of the month (salary release) explained above
    .withColumn("salday", next_day(date_add(last_day(col("dt")), -7), "Fri"))
    # convert timestamp column to date only (overwrites ts with date component)
    .withColumn("ts_date", to_date(col("ts")))
    .withColumn("dayofyr", dayofyear(col("today")))
    .withColumn("dayofmon", dayofmonth(col("today")))
    .withColumn("dayofweek", dayofweek(col("today")))
    .withColumn("qrt", quarter(col("dt")))
)

# -----------------------------
# Additional practical examples
# -----------------------------
# 1) Last working day of month (if last day is weekend, move back to previous Friday/Saturday rules)
#    Approach: compute last_day and if it falls on Sat(7) or Sun(1) move backward appropriately.
#    dayofweek mapping in Spark: Sunday=1, Monday=2, ..., Saturday=7
from pyspark.sql.functions import when

df = df.withColumn(
    "last_working_day",
    when(dayofweek(col("lastday")).isin(1), date_sub(col("lastday"), 2))  # if Sunday -> last Friday (-2 days)
    .when(dayofweek(col("lastday")).isin(7), date_sub(col("lastday"), 1))   # if Saturday -> last Friday (-1 day)
    .otherwise(col("lastday"))
)

# 2) Nth business day after a date without UDFs (example: 5th business day after dt)
#    Approach: build a date sequence of length N*2 or so, filter weekdays, then pick nth element.
#    Use sequence(start, end, interval 1 day) and higher-order functions (filter, element_at).
N = 5
# ensure sequence covers enough days: use dt + 14 days window (covers weekends)
df = df.withColumn("date_seq", sequence(col("dt"), date_add(col("dt"), 14)))
# filter business days: exclude Sundays(1) and Saturdays(7)
df = df.withColumn("business_days", expr("filter(date_seq, x -> dayofweek(x) NOT IN (1,7))"))
# nth business day (1-based index)
df = df.withColumn(f"{N}th_business_day", element_at(col("business_days"), N))

# 3) Payroll math with rounding to nearest slab (₹500) using ceil
#    Example: round up salary_int to next 500 slab: ceil(salary/500) * 500
#    Ensure salary_int exists (demonstration uses a parse attempt)
from pyspark.sql.types import IntegerType

# Robust salary parsing: some CSVs may not have a 'salary' column. Try common candidates and fall back safely.
salary_candidates = ['salary', 'salary_str', 'salary_int', 'amount', 'donation_amount', 'donation_amt', 'donation']
src_col = None
for cand in salary_candidates:
    if cand in df.columns:
        src_col = cand
        break

# Informational: print which source column we detected so the user can verify at runtime
print(f"Detected salary-like source column: {src_col}")

if src_col is None:
    # no recognizable salary-like column; create an empty integer column to avoid unresolved column errors
    df = df.withColumn("salary_int_demo", lit(None).cast(IntegerType()))
else:
    # remove non-digits from the chosen source column and cast to integer
    df = df.withColumn("salary_int_demo", regexp_replace(col(src_col), r"[^\\d]", "").cast(IntegerType()))

# payroll slab (uses salary_int_demo which may be null)
df = df.withColumn("payroll_slab_500", (ceil(col("salary_int_demo") / 500) * 500).cast(IntegerType()))

# 4) Business-day shift for payroll: if salday falls on weekend, move to previous Friday
#    Combine salday and last_working_day logic to demonstrate two ways (next_day pattern vs backshift pattern)
df = df.withColumn(
    "salday_safe",
    when(dayofweek(col("salday")).isin(1), date_sub(col("salday"), 2))
    .when(dayofweek(col("salday")).isin(7), date_sub(col("salday"), 1))
    .otherwise(col("salday"))
)

# 5) Nth business day of the month (no UDF) - e.g., 2nd business day of the month for scheduling
M = 2
# month_start then sequence until +14 days
from pyspark.sql.functions import trunc

df = df.withColumn("month_start", trunc(col("dt"), "month"))
df = df.withColumn("month_seq", sequence(col("month_start"), date_add(col("month_start"), 14)))
df = df.withColumn("month_business_days", expr("filter(month_seq, x -> dayofweek(x) NOT IN (1,7))"))
df = df.withColumn(f"{M}th_bday_of_month", element_at(col("month_business_days"), M))

# 6) Time window example: find events within [ts, ts + 3 days] (here using donation date as example)
#    Demonstration: create a small synthetic events df with event_date and join on condition
#    For demo we create a tiny DataFrame from the existing df selecting dt as event_date
events = df.select(col("dt").alias("event_date"), col("salary_int_demo").alias("val")).limit(20)
# self-join events where right.event_date between left.event_date and left.event_date + 3
joined = events.alias("a").join(
    events.alias("b"),
    (col("b.event_date") >= col("a.event_date")) & (col("b.event_date") <= date_add(col("a.event_date"), 3)),
    how="inner"
).select(col("a.event_date").alias("start"), col("b.event_date").alias("within"), col("a.val"), col("b.val"))

# show some results of the added examples
print("--- sample: last_working_day, salday, salday_safe ---")
df.select("dt", "lastday", "last_working_day", "salday", "salday_safe").show(10, truncate=False)

print(f"--- sample: {N}th_business_day and payroll slab ---")
df.select("dt", f"{N}th_business_day", "salary_int_demo", "payroll_slab_500").show(10, truncate=False)

print(f"--- sample: {M}th_bday_of_month ---")
df.select("dt", "month_start", f"{M}th_bday_of_month").show(10, truncate=False)

print("--- sample time-window join results (within 3 days) ---")
joined.show(10, truncate=False)

# keep original prints
df.printSchema()
df.show(truncate=False)

# Helpful notes (comments) for users:
# - Use coalesce(to_date(...), to_date(...)) to robustly parse multiple input date formats.
# - next_day(base_date, 'MON') returns the next MONDAY after base_date (exclusive). To get "last Monday of month" pattern,
#   use next_day(date_add(last_day(dt), -7), 'Mon') which returns the Monday falling in the last week of the month.
# - sequence(...)/filter(...)/element_at(...) pattern allows computing "nth business day" without UDFs.
# - Use caching for intermediate big DataFrames when diagnostic counts or multiple re-uses are required; unpersist() when done.
# - For production, prefer explicit schemas and write 'bad' rows to quarantine storage instead of printing them.
# End of enhancements
