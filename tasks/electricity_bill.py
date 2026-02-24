from pyspark.sql import *
from pyspark.sql.functions import *
'''Split column → separate house_number and city_pincode.
Average bill per pincode → see which area consumes more.
Top 3 usage → high-consumption households.
Cost per kWh → efficiency comparison.
High bill flag → anomaly detection.
Collect list → show all houses in each pincode.
Collect set → unique households per pincode.
Max consumption pincode → detect demand-heavy regions.
Below average bill → low-paying households.
Rank households per pincode → relative spend ranking.'''
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\electricity-bill.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()

classification_df=df.withColumn("classification",when(((col("kwh_usage")>500) & (col("total_bill")>200)),"High usage")\
                                                  .when(((col("kwh_usage").between(200,500)) & (col("total_bill").between(100,200))),"Medium Usage")\
                                                  .otherwise("Low Usage"))
classification_df.show()

house_usage_cat_df=classification_df.groupBy("classification").agg(count(col("household")).alias("usg_cate_num"))
house_usage_cat_df.show()

#max_bill_df=classification_df.filter(col("classification")=="High usage").agg(max(col("total_bill")).alias("max_bill_high_usage"))
max_bill_df=classification_df.groupBy(col("classification")).agg(max(col("total_bill")).alias("max_bill_high_usage")).filter(col("classification")=="High usage")
max_bill_df.show()

avg_usage_df=classification_df.groupBy(col("classification")).agg(avg(col("kwh_usage")).alias("avg_power_usage")).filter(col("classification")=="Medium Usage")
avg_usage_df.show()

low_kwh_usage=classification_df.filter((col("kwh_usage")>300) & (col("classification")=="Low Usage")).agg(count(col("household")).alias("count"))
low_kwh_usage.show()

# 🔹 Task 1: Split household into house_number and city_pincode
split_df = df.withColumn("house_number", split(col("household"), "-")[0]) \
             .withColumn("city_pincode", split(col("household"), "-")[1])
split_df.show()

# 🔹 Task 2: Average bill per pincode
avg_bill = split_df.groupBy("city_pincode").agg(avg("total_bill").alias("avg_bill"))
avg_bill.show()

# 🔹 Task 3: Find top 3 houses with highest kWh usage
top3_usage = split_df.orderBy(col("kwh_usage").desc()).limit(3)
top3_usage.show()

# 🔹 Task 4: Calculate cost per kWh for each household
cost_per_kwh = split_df.withColumn("cost_per_kwh", round(col("total_bill")/col("kwh_usage"),2))
cost_per_kwh.show()

# 🔹 Task 5: Flag houses with unusually high bills (>1000)
high_bill = split_df.filter(col("total_bill") > 1000)
high_bill.show()

# 🔹 Task 6: Collect list of households by pincode
house_list = split_df.groupBy("city_pincode").agg(collect_list("house_number").alias("houses"))
house_list.show(truncate=False)

# 🔹 Task 7: Collect set of houses (unique) per pincode
house_set = split_df.groupBy("city_pincode").agg(collect_set("house_number").alias("unique_houses"))
house_set.show(truncate=False)

# 🔹 Task 8: Find pincode with maximum total consumption
max_pincode = split_df.groupBy("city_pincode").agg(sum("kwh_usage").alias("total_kwh")) \
                      .orderBy(col("total_kwh").desc()).limit(1)
max_pincode.show()

# 🔹 Task 9: Find households paying below average bill
overall_avg = split_df.agg(avg("total_bill").alias("overall_avg")).collect()[0]["overall_avg"]
below_avg = split_df.filter(col("total_bill") < overall_avg)
below_avg.show()

# 🔹 Task 10: Rank households by spending within each pincode
from pyspark.sql.window import Window
w = Window.partitionBy("city_pincode").orderBy(col("total_bill").desc())
ranked = split_df.withColumn("rank", row_number().over(w))
ranked.show()
cpdata=r"D:\bigdata\drivers\city-pincode.csv"
city_df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(cpdata)
city_df.show()
elec_df=split_df
# Make sure join keys align in type
elec_df = elec_df.withColumn("city_pincode", col("city_pincode").cast("string"))
city_df = city_df.withColumn("pincode", col("pincode").cast("string"))

# (Optional) If city_df is small (dimension table), broadcast it for speed
# from pyspark.sql.functions import broadcast
city_dim = broadcast(city_df)

# ======================================================================
# 1) INNER JOIN (standard lookup)
# Purpose: keep only households whose pincode exists in the lookup
# ======================================================================
j_inner = (
    elec_df.alias("e")
    .join(city_dim.alias("c"), col("e.city_pincode")==col("c.pincode"), "inner")
    .select("e.*", "c.city")
)
# j_inner.show(truncate=False)

# ======================================================================
# 2) LEFT JOIN
# Purpose: keep all electricity rows, attach city where available
#          (missing city will be NULL)
# ======================================================================
j_left = (
    elec_df.alias("e")
    .join(city_dim.alias("c"), col("e.city_pincode")==col("c.pincode"), "left")
    .select("e.*", "c.city")
)
# j_left.show(truncate=False)

# ======================================================================
# 3) RIGHT JOIN
# Purpose: keep all city pincodes, even if no electricity record exists
# ======================================================================
j_right = (
    elec_df.alias("e")
    .join(city_dim.alias("c"), col("e.city_pincode")==col("c.pincode"), "right")
    .select("c.city","c.pincode","e.household","e.kwh_usage","e.total_bill")
)
# j_right.show(truncate=False)

# ======================================================================
# 4) FULL OUTER JOIN
# Purpose: keep everything from both sides; diagnose coverage gaps
# ======================================================================
j_full = (
    elec_df.alias("e")
    .join(city_dim.alias("c"), col("e.city_pincode")==col("c.pincode"), "full_outer")
    .select("e.*", "c.city", "c.pincode")
)
# j_full.show(truncate=False)

# ======================================================================
# 5) LEFT SEMI JOIN
# Purpose: filter elec_df to only rows whose pincode exists in city_df
#          (returns columns from left only, like an existence filter)
# ======================================================================
j_left_semi = elec_df.join(city_dim, elec_df.city_pincode==city_dim.pincode, "left_semi")
# j_left_semi.show(truncate=False)

# ======================================================================
# 6) LEFT ANTI JOIN
# Purpose: find electricity rows with pincodes NOT present in city_df
#          (great for data quality checks)
# ======================================================================
j_left_anti = elec_df.join(city_dim, elec_df.city_pincode==city_dim.pincode, "left_anti")
# j_left_anti.show(truncate=False)

# ======================================================================
# 7) CROSS JOIN (Cartesian)
# Purpose: build all pairs (use sparingly!). Example: combine each city
#          with delivery SLA bands, or build synthetic scenarios.
# NOTE: You must enable crossJoin or use a safe trick (join on lit(1)).
# ======================================================================
spark.conf.set("spark.sql.crossJoin.enabled", "true")
cities_only = city_dim.select("city").distinct()
# Example: cross cities with 3 synthetic tariff bands
tariff = spark.createDataFrame([("Low",),("Med",),("High",)], ["tariff_band"])
j_cross = cities_only.crossJoin(tariff)
# j_cross.show(truncate=False)

# ======================================================================
# 8) DEDUP / MANY-TO-ONE GUARD
# Purpose: if city_df accidentally has duplicate pincode rows, a join
#          will “explode”. This keeps the first city per pincode.
# ======================================================================
w_pin = Window.partitionBy("pincode").orderBy("city")
city_dedup = (
    city_dim.withColumn("rn", row_number().over(w_pin))
            .filter(col("rn")==1)
            .drop("rn")
)
j_left_dedup = elec_df.join(city_dedup, elec_df.city_pincode==city_dedup.pincode, "left")
# j_left_dedup.show(truncate=False)

# ======================================================================
# 9) POST-JOIN METRICS — Avg bill & total kWh per city
# Purpose: basic aggregation after lookup
# ======================================================================
city_usage = (
    j_left.groupBy("city")
          .agg(
              count("*").alias("households"),
              sum("kwh_usage").alias("total_kwh"),
              avg("total_bill").alias("avg_bill")
          )
          .orderBy(col("total_kwh").desc_nulls_last())
)
# city_usage.show(truncate=False)

# ======================================================================
# 10) COVERAGE KPI — What % of households got a city?
# Purpose: DQ: how good is the mapping?
# ======================================================================
coverage = j_left.select(expr("city is not null").cast("int").alias("mapped")).agg(avg("mapped").alias("coverage"))
# coverage.show()

# ======================================================================
# 11) ANTI + RIGHT to find orphan pincodes on each side
# Purpose: find gaps both ways
#    - elec_orphans: pincodes in electricity but not in city ref
#    - city_orphans: pincodes in city ref with zero electricity rows
# ======================================================================
elec_orphans = j_left_anti.select(col("city_pincode").alias("missing_in_city")).distinct()
city_orphans = j_right.filter(col("household").isNull()).select("pincode").distinct()
# elec_orphans.show(); city_orphans.show()

# ======================================================================
# 12) CONDITIONAL JOIN (subset) — e.g., only high-usage houses
# Purpose: join on a filtered left side to limit cardinality
# ======================================================================
high_usage = elec_df.filter(col("kwh_usage") >= 500)
j_high = high_usage.join(city_dim, high_usage.city_pincode==city_dim.pincode, "left")
# j_high.show(truncate=False)

# ======================================================================
# 13) MULTI-KEY JOIN EXAMPLE (if you add state later)
# Purpose: show pattern for composite keys
# (Assume both DFs have state columns; here we just show the template)
# ======================================================================
# j_multi = elec_df.join(city_dim, (elec_df.city_pincode==city_dim.pincode) & (elec_df.state==city_dim.state), "left")

# ======================================================================
# 14) NULL-SAFE EQUALITY JOIN (eqNullSafe)
# Purpose: if some pincodes are null and you want null==null to match
# ======================================================================
# j_nullsafe = elec_df.join(city_dim, elec_df.city_pincode.eqNullSafe(city_dim.pincode), "inner")

# ======================================================================
# 15) VALIDATION: COUNT PRESERVATION & DUP CHECK
# Purpose: detect accidental row explosion after join
# ======================================================================
cnt_elec = elec_df.count()
cnt_join = j_left.count()
dup_check = (j_left.groupBy("household").count().filter(col("count")>1))
# print("elec rows:", cnt_elec, " joined rows:", cnt_join)
# dup_check.show()  # if any rows appear, your city_df had duplicates for a pincode

# ----------------------------------------------------------------------
# QUICK DISPLAYS — uncomment what you need to see
# ----------------------------------------------------------------------
j_inner.show(truncate=False)
j_left.show(truncate=False)
j_right.show(truncate=False)
j_full.show(truncate=False)
j_left_semi.show(truncate=False)
j_left_anti.show(truncate=False)
j_cross.show(truncate=False)
j_left_dedup.show(truncate=False)
city_usage.show(truncate=False)
coverage.show()
elec_orphans.show()
city_orphans.show()
j_high.show(truncate=False)
dup_check.show(truncate=False)