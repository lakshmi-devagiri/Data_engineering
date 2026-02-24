from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
# DataFrame-API snippets with detailed inline explanations (comments only).
# Place these comments directly above / beside the original code blocks in your script.
# Load
basepath=r"D:\bigdata\drivers\jan-feb-incrementaldata"
jan = (spark.read.option("header", True).csv(f"{basepath}/transactions_jan.csv"))
feb = (spark.read.option("header", True).csv(f"{basepath}/transactions_feb.csv"))
jan.show()
feb.show()
# -----------------------
# UNION / TYPE TIDYING
# -----------------------
# Combine January + February transactions.
# - unionByName(..., allowMissingColumns=True) aligns columns by name (not position).
# - allowMissingColumns=True lets the smaller/mismatched schema be unioned by filling missing cols with nulls.
# Why unionByName? It is safer than plain union() when CSVs may have columns in different orders or when new columns
# are added in later files (schema evolution).
# Note: after union, some rows may have nulls in newly introduced columns; handle them next (casting / filling).
all_txn = jan.unionByName(feb, allowMissingColumns=True)
print("All transactions schema after union:")
all_txn.show()
# Normalize types and handle missing values.
all_txn = (all_txn
    .withColumn("amount", F.col("amount").cast("double"))
    .na.fill({"amount": 0.0, "coupon_code": "NA", "channel": "UNKNOWN"})
)

# Quarantine rows missing critical keys instead of dropping them outright.
bad = all_txn.filter(F.col("order_date").isNull() | F.col("category").isNull())
good = all_txn.filter(F.col("order_date").isNotNull() & F.col("category").isNotNull())
print("Rows quarantined (missing order_date or category):", bad.count())
if bad.count() > 0:
    print("Sample quarantined rows:")
    bad.show(5, truncate=False)
# continue pipeline with 'good'
all_txn = good
print("All transactions schema after cleaning (good rows):")
all_txn.show()

# -----------------------
# DEDUPLICATION (customers)
# -----------------------

cust_v1 = spark.read.option("header", True).csv(f"{basepath}/customers_v1.csv")
# Exact row dedupe (easy case): dropDuplicates()
# - dropDuplicates() removes complete duplicate rows (all columns equal).
# - Use this when duplicate rows are literal copies, e.g., same CSV file repeated.
# - Cheap and quick, but only handles exact duplicates.

# Diagnostic: show exact duplicate row groups if any
cust_before = cust_v1.count()
# Find exact duplicates by grouping on all columns - build list dynamically
group_cols = cust_v1.columns
cust_dup_groups = cust_v1.groupBy(group_cols).count().filter("count > 1")
cust_dup_group_count = cust_dup_groups.count()
print(f"Exact duplicate full-row groups before dedupe: {cust_dup_group_count}")
if cust_dup_group_count > 0:
    cust_dup_groups.show(5, truncate=False)
    dup_rows = cust_v1.join(cust_dup_groups.select(*group_cols), on=group_cols, how='inner')
    print("Sample exact duplicate rows (before dropDuplicates):")
    dup_rows.show(10, truncate=False)

# perform exact row dedupe

dedup_cust = cust_v1.dropDuplicates()
print("Customer schema after deduplication:")
dedup_cust.show()

cust_after = dedup_cust.count()
print(f"Customer rows before: {cust_before}, after dropDuplicates: {cust_after}, removed: {cust_before - cust_after}")

# -----------------------
# DEDUPLICATION (events by key)
# -----------------------
events = spark.read.option("header", True).csv(f"{basepath}/dirty_events.csv")
print("before dedupe events:")
events.show()

# Diagnostic: show duplicate groups by key (user_id, event_ts) before dedup
# Cache dup_groups to avoid recomputing the grouping multiple times
dup_groups = events.groupBy("user_id", "event_ts").count().filter("count > 1").cache()
dup_group_count = dup_groups.count()
print(f"Duplicate (user_id,event_ts) groups before dedupe: {dup_group_count}")
if dup_group_count > 0:
    dup_groups.show(5, truncate=False)
    # show sample duplicate rows (join back to original events)
    dup_rows = events.join(dup_groups.select("user_id", "event_ts"), on=["user_id", "event_ts"], how="inner")
    print("Sample duplicate rows (before dedupe):")
    dup_rows.show(10, truncate=False)
# unpersist dup_groups after use
dup_groups.unpersist()

# Key-based dedupe: dropDuplicates(["user_id","event_ts"])
# - dropDuplicates(list_of_cols) removes rows that have duplicate values for the listed columns (keeps one arbitrary row).
# - It's non-deterministic which row is kept if other columns differ; Spark does not guarantee which one stays.
# - Use when any row among duplicates is acceptable, or when rows are identical beyond the dedupe key.

dedup_events = events.dropDuplicates(["user_id","event_ts"])
print("Events schema after deduplication:")
dedup_events.show()

# Diagnostic: show how many rows removed by dropDuplicates
before_count = events.count()
after_count = dedup_events.count()
print(f"Events rows before dedupe: {before_count}, after dedupe: {after_count}, removed: {before_count - after_count}")

# If you require deterministic choice (e.g., prefer latest timestamp, or prefer non-null payload), use window + row_number:
#   w = Window.partitionBy("user_id","event_ts").orderBy(F.col("some_preference_col").desc_nulls_last())
#   stable_dedup = events.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
# This guarantees which row is chosen based on the ORDER BY clause.

# -----------------------
# UPSERT / LATEST SNAPSHOT (window + row_number) + latest-per-column improvement
# -----------------------
from pyspark.sql.window import Window
cust_v2 = spark.read.option("header", True).csv(f"{basepath}/customers_v2.csv")
# We have two customer snapshots (cust_v1 and cust_v2). We want one row per customer_id keeping the latest signup_date.
union_cust = cust_v1.unionByName(cust_v2, allowMissingColumns=True)
print("Unioned customer snapshots: ")
union_cust.show()

# Diagnostic: show per-customer snapshot counts before selecting latest
counts_per_customer = union_cust.groupBy("customer_id").count()
multi_snapshot_count = counts_per_customer.filter("count > 1").count()
print(f"Customers with multiple snapshots before upsert logic: {multi_snapshot_count}")
if multi_snapshot_count > 0:
    print("Sample customers with multiple snapshots and their counts:")
    counts_per_customer.filter("count > 1").show(10)

# Build window to rank snapshots per customer by signup_date desc
w = Window.partitionBy("customer_id").orderBy(F.to_date("signup_date").desc())
union_with_rn = union_cust.withColumn("rn", F.row_number().over(w)).cache()

# Rows that will be kept (rn == 1)
kept = union_with_rn.filter("rn = 1").drop("rn")
# Rows that will be removed (rn > 1)
removed = union_with_rn.filter("rn > 1").drop("rn")

# compute counts once to avoid repeated jobs
removed_cnt = removed.count()
kept_cnt = kept.count()
print("Number of rows removed:", removed_cnt)
if removed_cnt > 0:
    print("Sample removed (older) snapshots per customer:")
    removed.show(10, truncate=False)

print("Number of rows kept (latest snapshot per customer):", kept_cnt)

# Now compute improved 'latest per column' using first(..., ignorenulls=True) on the ranked dataset
# This aggregates columns preferring the top-ranked (latest) non-null values per customer
latest_per_col = (union_with_rn
  .groupBy("customer_id")
  .agg(
      first("name", True).alias("name"),
      first("email", True).alias("email"),
      first("city", True).alias("city"),
      first("signup_date", True).alias("signup_date")
  ))

print("Latest per column (after aggregating first non-null from ranked rows):")
latest_per_col.show(10, truncate=False)

# Set latest_cust to this improved result
latest_cust = latest_per_col

# unpersist the cached union_with_rn now that we're done with it
union_with_rn.unpersist()

print("Latest customer snapshots after upsert logic:")
latest_cust.show()

# -----------------------
# PIVOT / AGGREGATIONS
# -----------------------
# Ensure order_date is cast to date before extracting month for pivoting
all_txn = all_txn.withColumn(
    "order_date",
    F.coalesce(
        F.to_date("order_date", "yyyy-MM-dd"),
        F.to_date("order_date", "dd-MM-yyyy"),
        F.to_date("order_date", "MM-dd-yyyy"),
        F.to_date("order_date", "yyyy/MM/dd"),
        F.to_date("order_date", "yyyy.MM.dd"),
        F.to_date("order_date")
    )
)

all_txn2 = all_txn.withColumn("month", F.date_format("order_date", "yyyy-MM"))

pivot_city_cat = (all_txn
    .groupBy("city")
    .pivot("category")                  # pivot values will become columns (one column per distinct category)
    .agg(F.round(F.sum("amount"), 2))  # aggregate per (city, category)
)
print("Pivoted city x category amounts:")
pivot_city_cat.show()

# Notes:
# - pivot() collects distinct pivot keys (categories). If categories are many/high-cardinality, pivot can produce many columns — avoid if unbounded.
# - For stability, you can provide an explicit list to pivot(..., values) to ensure column order and presence.
# - pivot triggers a shuffle; if all_txn is large, consider pre-aggregating or filtering to reduce data.

# Month x status pivot via adding a derived month column:
all_txn2 = all_txn.withColumn("month", F.date_format("order_date", "yyyy-MM"))
pivot_mo_status = (all_txn2
    .groupBy("month")
    .pivot("status")
    .count()
)
# Notes:
# - date_format returns a string month key; ensure order/format consistent for downstream dashboards.
# - .count() yields the count per (month,status). If you need other metrics, replace .count() with specific aggregations.

# -----------------------
# GENERAL BEST PRACTICES (DataFrame API)
# -----------------------
# - Determinism: use window + ORDER BY when you must deterministically choose a row among duplicates.
# - Performance: prefer df.toDF(*cols)/single-schema rewrite for renames; use broadcast joins for small lookup tables; persist intermediate DataFrames if reused.
# - Safety: quarantine (write to audit path) rows dropped by na.drop instead of permanently dropping if recovery is required.
# - Schema: prefer reading CSVs with explicit schema for production (avoid inferSchema for large or heterogeneous files).
# - Testing: add unit tests for transforms using a small SparkSession fixture (pytest) to assert expected outputs for edge cases (nulls, duplicate keys, mixed dates).
#
# Example alternative for deterministic dedupe keeping latest non-null "email" and max "score":
#   w = Window.partitionBy("customer_id").orderBy(F.coalesce(F.col("score"), F.lit(0)).desc(), F.to_date("signup_date").desc())
#   latest = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
#
# Summary:
# - Use dropDuplicates() for quick exact-row dedupe.
# - Use dropDuplicates([cols]) when other columns don't matter and any row for the key suffices (note: non-deterministic).
# - Use Window + row_number() when you need a deterministic choice based on business policy (latest, highest, non-null preference).
# -------------------------
# Spark SQL equivalents
# -------------------------
products = spark.read.option("header", True).csv(f"{basepath}/products.csv")
returns  = spark.read.option("header", True).csv(f"{basepath}/returns.csv")

# Register intermediate DataFrames as temp views so we can run equivalent SQL
jan.createOrReplaceTempView('jan')
feb.createOrReplaceTempView('feb')
all_txn.createOrReplaceTempView('all_txn')
cust_v1.createOrReplaceTempView('cust_v1')
events.createOrReplaceTempView('events')
products.createOrReplaceTempView('products')
returns.createOrReplaceTempView('returns')
cust_v2.createOrReplaceTempView('cust_v2')

# 1) Clean/normalize transaction table (cast amount, fill defaults, drop null keys)
cols = all_txn.columns
select_parts = []
for c in cols:
    if c == 'amount':
        select_parts.append("CAST(`amount` AS DOUBLE) AS amount")
    elif c == 'coupon_code':
        select_parts.append("COALESCE(`coupon_code`, 'NA') AS coupon_code")
    elif c == 'channel':
        select_parts.append("COALESCE(`channel`, 'UNKNOWN') AS channel")
    else:
        select_parts.append(f"`{c}`")
select_sql = ", ".join(select_parts)
cleaned_txn_sql = f"SELECT {select_sql} FROM all_txn WHERE `order_date` IS NOT NULL AND `category` IS NOT NULL"
spark.sql(f"CREATE OR REPLACE TEMP VIEW cleaned_txn AS {cleaned_txn_sql}")
print('Created view cleaned_txn (SQL)')

# 2) Deduplicate customers (exact row dedupe)
spark.sql('CREATE OR REPLACE TEMP VIEW dedup_cust AS SELECT DISTINCT * FROM cust_v1')
print('Created view dedup_cust (SQL distinct)')

# 3) Deduplicate events by (user_id,event_ts) using row_number()
spark.sql("""
CREATE OR REPLACE TEMP VIEW dedup_events AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, event_ts ORDER BY user_id) AS rn
  FROM events
) t WHERE rn = 1
""")
print('Created view dedup_events (SQL row_number)')

# 4) Join with products (get segment) and returns (is_returned flag)
spark.sql("""
CREATE OR REPLACE TEMP VIEW with_seg AS
SELECT a.*, p.segment AS segment
FROM cleaned_txn a
LEFT JOIN products p
  ON a.category = p.category
""")
print('Created view with_seg (SQL left join with products)')

spark.sql("""
CREATE OR REPLACE TEMP VIEW with_ret AS
SELECT w.*, CASE WHEN r.return_date IS NOT NULL THEN 1 ELSE 0 END AS is_returned
FROM with_seg w
LEFT JOIN returns r
  ON w.txn_id = r.txn_id
""")
print('Created view with_ret (SQL left join with returns)')

# 5) UPSERT-style: union customer snapshots and keep latest signup_date per customer_id
spark.sql("CREATE OR REPLACE TEMP VIEW union_cust AS SELECT * FROM cust_v1 UNION ALL SELECT * FROM cust_v2")
# choose latest by parsed date (TO_DATE will try common formats)
spark.sql("""
CREATE OR REPLACE TEMP VIEW latest_cust AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY TO_DATE(signup_date) DESC) AS rn
  FROM union_cust
) t WHERE rn = 1
""")
print('Created view latest_cust (SQL upsert by latest signup_date)')

# 6) Pivot-like aggregations (city x category amounts; month x status counts)
# We will compute aggregates first and then pivot using SQL PIVOT with distinct categories.
cats = [r[0] for r in spark.sql("SELECT DISTINCT category FROM cleaned_txn WHERE category IS NOT NULL").collect()]
if cats:
    in_list = ", ".join([f"'{c}'" for c in cats])
    pivot_query = f"SELECT * FROM (SELECT city, category, amount FROM cleaned_txn) PIVOT (ROUND(SUM(amount),2) FOR category IN ({in_list}))"
    try:
        spark.sql(pivot_query).createOrReplaceTempView('pivot_city_cat')
        print('Created view pivot_city_cat (SQL pivot)')
    except Exception as e:
        print('Pivot failed in SQL (falling back to grouped view):', e)
        spark.sql('CREATE OR REPLACE TEMP VIEW pivot_city_cat AS SELECT city, category, ROUND(SUM(amount),2) AS amount FROM cleaned_txn GROUP BY city, category')
else:
    spark.sql('CREATE OR REPLACE TEMP VIEW pivot_city_cat AS SELECT city, category, ROUND(SUM(amount),2) AS amount FROM cleaned_txn GROUP BY city, category')

# month x status counts
spark.sql("CREATE OR REPLACE TEMP VIEW cleaned_txn_month AS SELECT date_format(order_date, 'yyyy-MM') AS month, status FROM cleaned_txn")
# dynamic pivot for statuses
status_vals = [r[0] for r in spark.sql("SELECT DISTINCT status FROM cleaned_txn_month WHERE status IS NOT NULL").collect()]
if status_vals:
    in_list2 = ", ".join([f"'{s}'" for s in status_vals])
    pivot_mo_status_query = f"SELECT * FROM (SELECT month, status FROM cleaned_txn_month) PIVOT (COUNT(status) FOR status IN ({in_list2}))"
    try:
        spark.sql(pivot_mo_status_query).createOrReplaceTempView('pivot_mo_status')
        print('Created view pivot_mo_status (SQL pivot)')
    except Exception as e:
        print('Month-status pivot failed (falling back to grouped view):', e)
        spark.sql("CREATE OR REPLACE TEMP VIEW pivot_mo_status AS SELECT month, status, COUNT(*) AS cnt FROM cleaned_txn_month GROUP BY month, status")
else:
    spark.sql("CREATE OR REPLACE TEMP VIEW pivot_mo_status AS SELECT month, status, COUNT(*) AS cnt FROM cleaned_txn_month GROUP BY month, status")

# 7) Show SQL view results for parity check
print('=== SQL: dedup_events ===')
spark.sql('SELECT * FROM dedup_events LIMIT 20').show()
print('=== SQL: with_ret (sample) ===')
spark.sql('SELECT txn_id, customer_id, amount, segment, is_returned FROM with_ret LIMIT 20').show()
print('=== SQL: latest_cust (sample) ===')
spark.sql('SELECT * FROM latest_cust LIMIT 20').show()
print('=== SQL: pivot_city_cat (sample) ===')
spark.sql('SELECT * FROM pivot_city_cat LIMIT 20').show()
print('=== SQL: pivot_mo_status (sample) ===')
spark.sql('SELECT * FROM pivot_mo_status LIMIT 20').show()

# End of SQL parity section
