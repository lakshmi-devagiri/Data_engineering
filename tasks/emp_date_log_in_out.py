from pyspark.sql import *
from pyspark.sql.functions import *  # pylint: disable=unused-wildcard-import,unused-import

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\time_logs1.csv"
df = spark.read.csv(data, header=True, inferSchema=True)
df.show()

# -----------------------------------------------------------------------------
# Assignments
# -----------------------------------------------------------------------------
# 1) Normalize timestamp columns and compute work-duration hours per row (if clock_in/clock_out exist).
#    - Answer: Convert clock_in_ts/clock_out_ts to timestamp, compute duration in hours and a clean flag.
#
# 2) Daily aggregation: compute total billable hours per employee per work_date and identify days with > 10 hours.
#    - Answer: Group by emp_id and work_date, sum billable_hours_raw and flag overtime days.
#
# 3) Detect missing clock-outs / clock-ins (possible open shifts) and list them for follow-up.
#    - Answer: Filter rows where clock_in_ts is present but clock_out_ts is null (or vice-versa).
#
# 4) Find gaps in attendance: for each emp_id, detect gaps > 2 days between consecutive work_date entries.
#    - Answer: Use window lag on work_date and datediff to find large gaps.
#
# 5) Build a workday calendar (exclude weekends and optional holidays file) and compute the 5th business day
#    after an employee's first recorded work_date (no UDFs).
#    - Answer: Create calendar from min/max work_date, filter weekends/holidays, join and rank to get 5th business day.
#
# 6) Overlapping shifts detection: for employees with both clock_in_ts and clock_out_ts, find any pair of rows
#    where time intervals overlap (same emp_id).
#    - Answer: Self-join per emp and filter interval overlaps where IDs differ.
#
# 7) PII masking: if aadhar/pan/phone/email columns exist, mask them in output (show only last 4 digits or partial mask).
#    - Answer: Use column-existence checks and Spark string functions to produce masked columns.
#
# 8) Bronze/Silver/Gold demonstration (example only): write bronze (raw) parquet, then simple cleaning to silver,
#    and an aggregated gold view. Paths are example placeholders; uncomment to run in your environment.
# -----------------------------------------------------------------------------

# Implementation: defensive column checks so the script works with varying CSV schemas.
cols = set(df.columns)

# 1) Normalize timestamps and compute duration
if {'clock_in_ts', 'clock_out_ts'}.issubset(cols):
    df_times = (
        df
        .withColumn('clock_in_ts', to_timestamp(col('clock_in_ts')))
        .withColumn('clock_out_ts', to_timestamp(col('clock_out_ts')))
        .withColumn('duration_hours',
                    when(col('clock_in_ts').isNotNull() & col('clock_out_ts').isNotNull(),
                         (col('clock_out_ts').cast('long') - col('clock_in_ts').cast('long'))/3600.0)
                    .otherwise(lit(None)))
        .withColumn('duration_valid', when(col('duration_hours') >= 0, lit(True)).otherwise(lit(False)))
    )
    print("== Normalized timestamps and computed duration_hours ==")
    df_times.select(*(c for c in df_times.columns if c in ['emp_id','work_date','clock_in_ts','clock_out_ts','duration_hours','duration_valid'])).show(10, truncate=False)
else:
    df_times = df
    print("== Skipping timestamp normalization: clock_in_ts/clock_out_ts not present in data ==")

# 2) Daily aggregation of billable hours
if 'billable_hours_raw' in cols and 'emp_id' in cols and 'work_date' in cols:
    daily = (
        df.groupBy('emp_id', 'work_date')
          .agg(sum('billable_hours_raw').alias('total_billable_hours'))
          .withColumn('overtime_flag', when(col('total_billable_hours') > 10, lit('Y')).otherwise(lit('N')))
    )
    print("== Daily aggregation (total_billable_hours) ==")
    daily.show(10, truncate=False)
else:
    print("== Skipping daily aggregation: required columns not present ==")

# 3) Detect missing clock-outs / clock-ins
if 'clock_in_ts' in cols or 'clock_out_ts' in cols:
    missing_in = df.filter(col('clock_in_ts').isNull() & col('clock_out_ts').isNotNull()) if 'clock_in_ts' in cols else None
    missing_out = df.filter(col('clock_in_ts').isNotNull() & col('clock_out_ts').isNull()) if 'clock_out_ts' in cols else None
    print("== Missing clock-ins (rows where clock_in is null but clock_out present) ==")
    if missing_in is not None:
        missing_in.show(10, truncate=False)
    else:
        print("(column clock_in_ts not present)")
    print("== Missing clock-outs (rows where clock_out is null but clock_in present) ==")
    if missing_out is not None:
        missing_out.show(10, truncate=False)
    else:
        print("(column clock_out_ts not present)")
else:
    print("== Skipping missing-in/out detection: no clock_in_ts/clock_out_ts columns ==")

# 4) Gaps in attendance per employee (> 2 days)
if 'emp_id' in cols and 'work_date' in cols:
    df_dates = df.select('emp_id', to_date(col('work_date')).alias('work_date'))
    w = Window.partitionBy('emp_id').orderBy('work_date')
    gaps = (
        df_dates.withColumn('prev_date', lag('work_date').over(w))
                .withColumn('gap_days', when(col('prev_date').isNotNull(), datediff(col('work_date'), col('prev_date'))).otherwise(lit(0)))
                .filter(col('gap_days') > 2)
    )
    print("== Attendance gaps (>2 days) ==")
    gaps.show(10, truncate=False)
else:
    print("== Skipping attendance-gap detection: emp_id/work_date missing ==")

# 5) 5th business day after first work_date per emp (no UDFs)
# Optional holidays can be loaded by providing a CSV at D:\bigdata\drivers\holidays.csv with column holiday_date
if 'emp_id' in cols and 'work_date' in cols:
    # compute bounds
    bounds = df.select(min(to_date(col('work_date'))).alias('min_d'), max(to_date(col('work_date'))).alias('max_d')).first()
    if bounds and bounds['min_d'] is not None and bounds['max_d'] is not None:
        min_d = bounds['min_d']
        max_d = bounds['max_d']
        # build calendar
        cal = (spark.range(1)
               .select(sequence(lit(min_d), lit(max_d)).alias('arr'))
               .select(explode(col('arr')).alias('d')) )
        # try to load holidays (optional)
        hol_path = r"D:\bigdata\drivers\holidays.csv"
        try:
            df_hol = spark.read.csv(hol_path, header=True, inferSchema=True).withColumn('holiday_date', to_date(col('holiday_date')))
            cal_work = cal.join(df_hol.withColumnRenamed('holiday_date', 'd'), ['d'], 'left_anti')
        except Exception:
            df_hol = None
            cal_work = cal
        cal_work = cal_work.withColumn('dow', date_format(col('d'), 'E')).filter(~col('dow').isin('Sat','Sun')).drop('dow').cache()

        first_work = (df.select('emp_id', to_date(col('work_date')).alias('work_date'))
                       .groupBy('emp_id').agg(min('work_date').alias('first_work_date')))
        # create candidate dates for each employee and pick 5th business day
        cand = (first_work.withColumn('cand', sequence(date_add(col('first_work_date'), 1), date_add(col('first_work_date'), 30)))
                .withColumn('cand', explode(col('cand')))
                .join(cal_work.withColumnRenamed('d', 'cand'), ['cand'], 'inner')
                .withColumn('biz_rank', row_number().over(Window.partitionBy('emp_id','first_work_date').orderBy('cand')))
                .filter(col('biz_rank') == 5)
                .select('emp_id', 'first_work_date', col('cand').alias('fifth_business_day')) )
        print("== 5th business day after first work_date per emp ==")
        cand.show(10, truncate=False)
    else:
        print("== Not enough date range to build calendar ==")
else:
    print("== Skipping 5th business day computation: emp_id/work_date missing ==")

# 6) Overlapping shifts detection
if {'emp_id', 'clock_in_ts', 'clock_out_ts'}.issubset(cols):
    intervals = df_times.select('emp_id', 'clock_in_ts', 'clock_out_ts').filter(col('clock_in_ts').isNotNull() & col('clock_out_ts').isNotNull())
    i1 = intervals.alias('i1')
    i2 = intervals.alias('i2')
    overlaps = (i1.join(i2, (col('i1.emp_id') == col('i2.emp_id')) & (col('i1.clock_in_ts') < col('i2.clock_out_ts')) & (col('i2.clock_in_ts') < col('i1.clock_out_ts')))
                .filter(col('i1.clock_in_ts') != col('i2.clock_in_ts'))
                .select(col('i1.emp_id').alias('emp_id'),
                        col('i1.clock_in_ts').alias('in_1'), col('i1.clock_out_ts').alias('out_1'),
                        col('i2.clock_in_ts').alias('in_2'), col('i2.clock_out_ts').alias('out_2'))
                .distinct())
    print("== Overlapping shifts (potential data issues) ==")
    overlaps.show(10, truncate=False)
else:
    print("== Skipping overlap detection: clock_in_ts/clock_out_ts or emp_id missing ==")

# 7) PII masking examples (aadhar, pan, phone, email)
masked = df
if 'aadhar' in cols:
    # show only last 4 digits of aadhar
    masked = masked.withColumn('aadhar_masked', concat(lit('****'), substring(col('aadhar'), length(col('aadhar')) - 3, 4)))
if 'pan' in cols:
    # show first 3 and last 1 characters, mask middle
    masked = masked.withColumn('pan_masked', concat(substring(col('pan'), 1, 3), lit('****'), substring(col('pan'), -1, 1)))
if 'phone' in cols:
    masked = masked.withColumn('phone_masked', concat(lit('*****'), substring(col('phone'), length(col('phone')) - 3, 4)))
if 'email' in cols:
    masked = masked.withColumn('email_masked', when(col('email').contains('@'), concat(lit('***'), split(col('email'), '@').getItem(0).substr(length(split(col('email', '@').getItem(0)) - 1, 1), 1)).otherwise(col('email'))))

if masked is not None:
    print("== Sample masked PII columns (if present) ==")
    cols_to_show = [c for c in ('emp_id','aadhar_masked','pan_masked','phone_masked','email_masked') if c in masked.columns]
    if cols_to_show:
        masked.select(*cols_to_show).show(10, truncate=False)
    else:
        print('(no PII columns found to mask)')

# 8) Bronze / Silver / Gold demonstration (example; commented out by default)
# bronze_path = r"D:\bigdata\output\bronze_time_logs"
# silver_path = r"D:\bigdata\output\silver_time_logs"
# gold_path = r"D:\bigdata\output\gold_employee_hours"
#
# # Bronze: write raw CSV as parquet (append)
# df.write.mode('append').parquet(bronze_path)
#
# # Silver: simple cleaning (drop null emp_id, normalize timestamps)
# df_silver = df.filter(col('emp_id').isNotNull())
# if 'clock_in_ts' in cols:
#     df_silver = df_silver.withColumn('clock_in_ts', to_timestamp(col('clock_in_ts')))
# df_silver.write.mode('overwrite').parquet(silver_path)
#
# # Gold: aggregated view - total hours per emp
# if 'billable_hours_raw' in cols:
#     gold = df_silver.groupBy('emp_id').agg(sum('billable_hours_raw').alias('total_hours'))
#     gold.write.mode('overwrite').parquet(gold_path)

print("== Script completed - assignments executed where possible ==")
