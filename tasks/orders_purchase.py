# ==============================================================================
# TASK SUMMARY
# ==============================================================================
# TASK 0      : Add speed_category based on delivery_days + origin
# TASK 0.1    : Count of orders by product_type × speed_category
#
# CORE ANALYTICS (TASKS 1–10)
# TASK 1      : Average delivery_days by product_type × origin
# TASK 2      : Pivot destination × origin (order counts)
# TASK 3      : SLA flag per order (Domestic ≤5 days, International ≤8 days)
# TASK 4      : On-time rate per destination
# TASK 5      : 90th percentile (p90) delivery_days per product_type
# TASK 6      : Top-3 slowest orders per destination
# TASK 7      : Most common destination (mode) per product_type
# TASK 8      : Intl - Domestic average delivery_days per product_type
# TASK 9      : Crosstab origin × delivery_days bucket (0-3, 4-6, 7-9, 10+)
# TASK 10     : City KPI – on_time_rate with at least 5 orders
#
# ADVANCED ANALYTICS (A1–A8)
# A1          : Robust outliers via MAD per (product_type, origin)
# A2          : SLA “what-if” – reduce International deliveries by 1 day
# A3          : Heatmap metrics per (destination, product_type) – avg & p95
# A4          : HHI route concentration per product_type across destinations
# A5          : Top-2 slowest orders per (destination, origin)
# A6          : Z-score anomalies per destination (z > 2.5)
# A7          : SLA tiers & weighted penalty score per destination
# A8          : Data quality guard + dedupe by order_id
#
# COLLECT / LANE VIEWS (B1–B15)
# B1          : For each (destination, product_type) – list of order_ids
# B2          : For each product_type – unique list of cities (product reach)
# B3          : For each destination – unique product_types sold (assortment)
# B4          : For each destination – unique origins seen (Domestic/International)
# B5          : Top-selling product_type (mode) per city
# B6          : City latency – avg_delivery, p95_delivery, orders
# B7          : Slowest city – highest avg_delivery
# B8          : For each city – (product_type, order_id) pairs as array
# B9          : For each product_type – origins set + list of cities
# B10         : Top-3 slowest orders per city (dense_rank)
# B11         : For each (destination, origin) – unique_products + order_ids
# B12         : For each destination – sorted list of unique product_types
# B13         : Worst origin (highest avg_delivery) per city
# B14         : For each (destination, product_type) – order_ids sorted by delay
# B15         : Rollup – unique_products & order counts per city + grand total
# ==============================================================================

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r"D:\bigdata\drivers\orders_purchase.csv"
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data))

print("\n" + "="*80)
print("RAW DATA: Orders")
print("="*80)
df.show()

# --------------------------------------------------------------------------
# TASK 0: Basic classification: speed_category
# --------------------------------------------------------------------------

classification_df = df.withColumn(
    "speed_category",
    when((col("delivery_days") > 7) & (col("origin") == "International"), "Delayed")
    .when((col("delivery_days") >= 3) & (col("delivery_days") <= 7), "On-Time")
    .otherwise("Fast")
)

print("\n" + "="*80)
print("TASK 0: Speed classification by delivery_days + origin")
print("="*80)
classification_df.show()

count_df = (classification_df
            .groupBy("product_type", "speed_category")
            .agg(count(col("speed_category")).alias("count")))

print("\n" + "="*80)
print("TASK 0.1: Count by product_type × speed_category")
print("="*80)
count_df.show()

# Clean base DF
orders = (
    df
    .withColumn("delivery_days", col("delivery_days").cast("int"))
    .withColumn("destination", initcap(col("destination")))
)

# ==============================================================================
# CORE ANALYTICS TASKS (1–10)
# ==============================================================================

# TASK 1: Avg delivery by product × origin
avg_by_po = (
    orders
    .groupBy("product_type", "origin")
    .agg(avg("delivery_days").alias("avg_delivery"),
         count("*").alias("orders"))
    .orderBy("product_type", "origin")
)

print("\n" + "="*80)
print("TASK 1: Average delivery days by product_type × origin")
print("="*80)
avg_by_po.show(truncate=False)

# TASK 2: Pivot destination × origin (counts)
dest_origin = (
    orders.groupBy("destination")
          .pivot("origin", ["Domestic", "International"])
          .agg(count("*"))
          .fillna(0)
)

print("\n" + "="*80)
print("TASK 2: Destination × origin pivot (counts)")
print("="*80)
dest_origin.show(truncate=False)

# TASK 3: SLA flag (Domestic ≤5, International ≤8)
sla = (
    orders
    .withColumn(
        "on_time",
        when((col("origin") == "Domestic") & (col("delivery_days") <= 5), 1)
        .when((col("origin") == "International") & (col("delivery_days") <= 8), 1)
        .otherwise(0)
    )
)

print("\n" + "="*80)
print("TASK 3: SLA flag per order (on_time)")
print("="*80)
sla.select("order_id", "origin", "delivery_days", "on_time").show(truncate=False)

# TASK 4: On-time rate per destination
on_time_rate_dest = (
    sla.groupBy("destination")
       .agg(avg("on_time").alias("on_time_rate"), count("*").alias("orders"))
       .orderBy(col("on_time_rate").desc(), col("orders").desc())
)

print("\n" + "="*80)
print("TASK 4: On-time rate per destination")
print("="*80)
on_time_rate_dest.show(truncate=False)

# TASK 5: 90th percentile delivery per product
p90 = (
    orders.groupBy("product_type")
          .agg(expr("percentile_approx(delivery_days, 0.90, 100)").alias("p90_delivery"))
)

print("\n" + "="*80)
print("TASK 5: 90th percentile delivery_days per product_type")
print("="*80)
p90.show(truncate=False)

# TASK 6: Top-3 slowest orders in each destination
w_dest = Window.partitionBy("destination").orderBy(col("delivery_days").desc())
slow3 = (
    orders
    .withColumn("r", dense_rank().over(w_dest))
    .filter(col("r") <= 3)
    .drop("r")
)

print("\n" + "="*80)
print("TASK 6: Top-3 slowest orders per destination")
print("="*80)
slow3.show(truncate=False)

# TASK 7: Most common destination per product (mode)
counts_prod_dest = orders.groupBy("product_type", "destination").count()
w_mode_prod = Window.partitionBy("product_type").orderBy(col("count").desc(), col("destination"))
mode_dest = counts_prod_dest.withColumn("rn", row_number().over(w_mode_prod)) \
                            .filter(col("rn") == 1).drop("rn")

print("\n" + "="*80)
print("TASK 7: Most common destination per product_type")
print("="*80)
mode_dest.show(truncate=False)

# TASK 8: Domestic vs International avg delivery delta per product
prod_avg = orders.groupBy("product_type", "origin").agg(avg("delivery_days").alias("avg_d"))
pivot_avg = prod_avg.groupBy("product_type").pivot("origin", ["Domestic", "International"]).agg(first("avg_d"))
delta = pivot_avg.withColumn("intl_minus_domestic", col("International") - col("Domestic"))

print("\n" + "="*80)
print("TASK 8: Intl - Domestic avg delivery_days per product_type")
print("="*80)
delta.show(truncate=False)

# TASK 9: Bucket delivery_days & cross-tab with origin
bkt = orders.withColumn(
    "d_bucket",
    when(col("delivery_days") <= 3, "0-3")
   .when(col("delivery_days") <= 6, "4-6")
   .when(col("delivery_days") <= 9, "7-9")
   .otherwise("10+")
)
crosstab = (
    bkt.groupBy("origin")
       .pivot("d_bucket", ["0-3", "4-6", "7-9", "10+"])
       .agg(count("*"))
       .fillna(0)
)

print("\n" + "="*80)
print("TASK 9: Crosstab origin × delivery_days bucket")
print("="*80)
crosstab.show(truncate=False)

# TASK 10: Per-city leaderboard by on-time rate (min 5 orders)
city_kpi = (
    sla.groupBy("destination")
       .agg(count("*").alias("n"), avg("on_time").alias("on_time_rate"))
       .filter(col("n") >= 5)
       .orderBy(col("on_time_rate").desc(), col("n").desc())
)

print("\n" + "="*80)
print("TASK 10: City KPI (on_time_rate with n >= 5)")
print("="*80)
city_kpi.show(truncate=False)

# ==============================================================================
# ADVANCED TASKS (A1–A8)
# ==============================================================================

# A1) Robust outliers via MAD by product & origin
grp_med = orders.groupBy("product_type", "origin").agg(
    expr("percentile_approx(delivery_days, 0.5, 100)").alias("med")
)
j1 = orders.join(grp_med, ["product_type", "origin"])
grp_mad = j1.groupBy("product_type", "origin").agg(
    expr("percentile_approx(abs(delivery_days - med), 0.5, 100)").alias("mad")
)
j2 = j1.join(grp_mad, ["product_type", "origin"])
mad_out = j2.withColumn(
    "robust_z",
    (col("delivery_days") - col("med")) / col("mad")
).filter((col("mad") > 0) & (col("robust_z") > 3.5))

print("\n" + "="*80)
print("ADVANCED TASK A1: MAD-based outliers per product_type × origin")
print("="*80)
mad_out.select("order_id", "product_type", "origin", "delivery_days", "robust_z").show(truncate=False)

# A2) SLA “what-if”: reduce International by 1 day
what_if = (
    orders
    .withColumn(
        "delivery_new",
        when(col("origin") == "International",
             greatest(lit(1), col("delivery_days") - 1)
        ).otherwise(col("delivery_days"))
    )
    .withColumn(
        "on_time_new",
        when((col("origin") == "Domestic") & (col("delivery_new") <= 5), 1)
       .when((col("origin") == "International") & (col("delivery_new") <= 8), 1)
       .otherwise(0)
    )
)
impact = what_if.agg(
    avg(when(col("origin") == "International", col("on_time_new"))).alias("intl_on_time_new"),
    avg(when(col("origin") == "International", (col("delivery_days") <= 8).cast("int"))).alias("intl_on_time_old")
)

print("\n" + "="*80)
print("ADVANCED TASK A2: What-if impact (International -1 day)")
print("="*80)
impact.show(truncate=False)

# A3) City–product heatmap: average & 95th percentile
heat = (
    orders.groupBy("destination", "product_type")
          .agg(avg("delivery_days").alias("avg_d"),
               expr("percentile_approx(delivery_days, 0.95, 100)").alias("p95_d"))
)

print("\n" + "="*80)
print("ADVANCED TASK A3: Heatmap per destination × product_type (avg, p95)")
print("="*80)
heat.orderBy("destination", "product_type").show(truncate=False)

# A4) Route concentration (HHI) per product across destinations
cnt = orders.groupBy("product_type", "destination").count()
tot = cnt.groupBy("product_type").agg(sum("count").alias("tot"))
shares = cnt.join(tot, "product_type").withColumn("s", (col("count") / col("tot")) ** 2)
hhi = shares.groupBy("product_type").agg(round(sum("s"), 4).alias("HHI"))

print("\n" + "="*80)
print("ADVANCED TASK A4: HHI (concentration) per product_type")
print("="*80)
hhi.orderBy(col("HHI").desc()).show(truncate=False)

# A5) Top-2 slowest orders per (destination, origin)
w_do = Window.partitionBy("destination", "origin").orderBy(col("delivery_days").desc())
top2_by_route = orders.withColumn("r", row_number().over(w_do)).filter(col("r") <= 2).drop("r")

print("\n" + "="*80)
print("ADVANCED TASK A5: Top-2 slowest orders per (destination, origin)")
print("="*80)
top2_by_route.show(truncate=False)

# A6) Z-score anomalies by destination (parametric)
w_z = Window.partitionBy("destination")
z_anom = (
    orders
    .withColumn("mean_d", avg("delivery_days").over(w_z))
    .withColumn("sd_d", stddev_pop("delivery_days").over(w_z))
    .withColumn("z", (col("delivery_days") - col("mean_d")) / col("sd_d"))
    .filter(col("sd_d") > 0)
    .filter(col("z") > 2.5)
)

print("\n" + "="*80)
print("ADVANCED TASK A6: Z-score anomalies per destination (z > 2.5)")
print("="*80)
z_anom.select("order_id", "destination", "delivery_days", "z").show(truncate=False)

# A7) SLA tiers & weighted penalty score
tiers = (
    orders
    .withColumn("sla_limit", when(col("origin") == "Domestic", 5).otherwise(8))
    .withColumn("delay", col("delivery_days") - col("sla_limit"))
    .withColumn(
        "tier",
        when(col("delay") <= 0, "OnTime")
       .when(col("delay") <= 2, "Slight")
       .when(col("delay") <= 4, "Moderate")
       .otherwise("Severe")
    )
    .withColumn(
        "penalty",
        when(col("tier") == "OnTime", 0)
       .when(col("tier") == "Slight", 1)
       .when(col("tier") == "Moderate", 3)
       .otherwise(5)
    )
)
penalty_by_dest = tiers.groupBy("destination").agg(
    sum("penalty").alias("penalty_sum"),
    avg("penalty").alias("penalty_avg")
)

print("\n" + "="*80)
print("ADVANCED TASK A7: Penalty score by destination")
print("="*80)
penalty_by_dest.orderBy(col("penalty_sum").desc()).show(truncate=False)

# A8) Data quality guard + dedupe
w_dup = Window.partitionBy("order_id").orderBy(col("delivery_days").desc())
dedup = orders.withColumn("rn", row_number().over(w_dup)).filter(col("rn") == 1).drop("rn")
dq_issues = orders.filter((col("delivery_days") <= 0) | col("destination").isNull())

print("\n" + "="*80)
print("ADVANCED TASK A8: Dedupe by order_id + DQ issues")
print("="*80)
print("Deduped orders:")
dedup.show(truncate=False)
print("\nData quality issues:")
dq_issues.show(truncate=False)

# ==============================================================================
# COLLECT_LIST / COLLECT_SET & LANE VIEWS (B1–B15)
# ==============================================================================

# B1) For each (city, product), list ALL order_ids
city_product_orders = (
    orders
    .groupBy("destination", "product_type")
    .agg(collect_list("order_id").alias("order_ids"))
)

print("\n" + "="*80)
print("TASK B1: City × product -> list of order_ids")
print("="*80)
city_product_orders.show(truncate=False)

# B2) For each product, list UNIQUE cities it sells in
product_reach = (
    orders
    .groupBy("product_type")
    .agg(collect_set("destination").alias("cities_sold"))
)

print("\n" + "="*80)
print("TASK B2: Product reach (cities per product_type)")
print("="*80)
product_reach.show(truncate=False)

# B3) For each city, list UNIQUE product types sold
city_assortment = (
    orders
    .groupBy("destination")
    .agg(collect_set("product_type").alias("product_types"))
)

print("\n" + "="*80)
print("TASK B3: City assortment (unique product_types per city)")
print("="*80)
city_assortment.show(truncate=False)

# B4) For each city, list UNIQUE origins seen
city_origin_mix = (
    orders
    .groupBy("destination")
    .agg(collect_set("origin").alias("origins_seen"))
)

print("\n" + "="*80)
print("TASK B4: City origin mix (origins per city)")
print("="*80)
city_origin_mix.show(truncate=False)

# B5) MOST SOLD product per city (mode)
counts_city_prod = orders.groupBy("destination", "product_type").count()
w_mode_city = Window.partitionBy("destination").orderBy(col("count").desc(), col("product_type"))
top_product_per_city = counts_city_prod.withColumn("rn", row_number().over(w_mode_city)) \
                                       .filter(col("rn") == 1).drop("rn")

print("\n" + "="*80)
print("TASK B5: Top product per city (mode)")
print("="*80)
top_product_per_city.show(truncate=False)

# B6) Which city takes the most time? (avg / p95 delivery)
city_latency = (
    orders
    .groupBy("destination")
    .agg(
        avg("delivery_days").alias("avg_delivery"),
        expr("percentile_approx(delivery_days, 0.95, 100)").alias("p95_delivery"),
        count("*").alias("orders")
    )
    .orderBy(col("avg_delivery").desc())
)

print("\n" + "="*80)
print("TASK B6: City latency (avg & p95 delivery)")
print("="*80)
city_latency.show(truncate=False)

# B7) City with the HIGHEST avg delivery (single row)
slowest_city = city_latency.orderBy(col("avg_delivery").desc()).limit(1)

print("\n" + "="*80)
print("TASK B7: Slowest city (highest avg_delivery)")
print("="*80)
slowest_city.show(truncate=False)

# B8) For each city: collect_list of (product, order_id) pairs (arrays_zip)
pairs_per_city = (
    orders
    .groupBy("destination")
    .agg(
        arrays_zip(collect_list("product_type"), collect_list("order_id")).alias("product_order_pairs")
    )
)

print("\n" + "="*80)
print("TASK B8: City -> list of (product_type, order_id) pairs")
print("="*80)
pairs_per_city.show(truncate=False)

# B9) For each product: collect_set of origins + collect_list of cities
product_logistics = (
    orders
    .groupBy("product_type")
    .agg(
        collect_set("origin").alias("origins"),
        collect_list("destination").alias("cities_list")
    )
)

print("\n" + "="*80)
print("TASK B9: Product logistics (origins + cities_list)")
print("="*80)
product_logistics.show(truncate=False)

# B10) Top-3 slowest orders within each city
w_slow3 = Window.partitionBy("destination").orderBy(col("delivery_days").desc())
slow3_city = orders.withColumn("r", dense_rank().over(w_slow3)).filter(col("r") <= 3).drop("r")

print("\n" + "="*80)
print("TASK B10: Top-3 slowest orders per city (dense_rank)")
print("="*80)
slow3_city.show(truncate=False)

# B11) Per city × origin: unique products + order_ids
city_origin_basket = (
    orders
    .groupBy("destination", "origin")
    .agg(
        collect_set("product_type").alias("unique_products"),
        collect_list("order_id").alias("order_ids")
    )
)

print("\n" + "="*80)
print("TASK B11: City × origin basket (unique products + order_ids)")
print("="*80)
city_origin_basket.show(truncate=False)

# B12) For each city: sorted unique products
city_sorted_products = (
    orders
    .groupBy("destination")
    .agg(array_sort(collect_set("product_type")).alias("sorted_products"))
)

print("\n" + "="*80)
print("TASK B12: City -> sorted unique products")
print("="*80)
city_sorted_products.show(truncate=False)

# B13) Most delayed ORIGIN per city (avg delivery)
city_origin_stats = orders.groupBy("destination", "origin").agg(
    avg("delivery_days").alias("avg_d"),
    count("*").alias("n")
)
w_bad_origin = Window.partitionBy("destination").orderBy(col("avg_d").desc(), col("n").desc())
worst_origin_per_city = city_origin_stats.withColumn("rn", row_number().over(w_bad_origin)) \
                                         .filter(col("rn") == 1).drop("rn")

print("\n" + "="*80)
print("TASK B13: Worst origin per city by avg delivery")
print("="*80)
worst_origin_per_city.show(truncate=False)

# B14) For each product in a city: list order_ids sorted by delivery_days
by_city_product_sorted_orders = (
    orders
    .withColumn("pair", struct(col("delivery_days"), col("order_id")))
    .groupBy("destination", "product_type")
    .agg(sort_array(collect_list("pair"), asc=False).alias("sorted_pairs"))  # desc by delivery_days
    .withColumn("order_ids_by_delay", transform(col("sorted_pairs"), lambda x: x["order_id"]))
    .drop("sorted_pairs")
)

print("\n" + "="*80)
print("TASK B14: City × product -> order_ids sorted by delay")
print("="*80)
by_city_product_sorted_orders.show(truncate=False)

# B15) City/product rollups with collect_set
roll = (
    orders
    .rollup("destination")
    .agg(collect_set("product_type").alias("unique_products"), count("*").alias("orders"))
    .orderBy(col("destination").asc_nulls_last())
)
roll.show()

print("\n" + "="*80)
print("TASK B15: Rollup - unique products & orders per c")
