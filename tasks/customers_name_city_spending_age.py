from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\customers_name_city_spending_age.csv"
csdf = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
csdf.show()
data = r"D:\bigdata\drivers\customer_purchases.csv"
cpdf = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
cpdf.show()


# -------------------------
# 0) Join on normalized name
# -------------------------
cust = csdf.withColumn("name_lc", lower(col("name")))
purch = cpdf.withColumn("name_lc", lower(col("name")))

# inner join so we analyze only customers present in both datasets
joined = (
    cust
    .join(purch.select("name","membership","days_since_last_purchase","total_purchase_amount","name_lc"),
          on="name_lc", how="inner")
    .select(  # prefer original purchase-side casing if available, else customers
        coalesce(purch["name"], cust["name"]).alias("name"),
        "city","spending","age","membership","days_since_last_purchase","total_purchase_amount"
    )
)

# Sanity
# joined.show(truncate=False)

# ================================================================
# 1) Category label (your idea, with fixed parentheses)
#    Purpose: quick spend × membership classification
# ================================================================
category_df = (
    joined.withColumn(
        "category",
        when((col("spending") > 1000) & (col("membership") == "Premium"), "High Spender")
        .when(((col("spending") > 500) & (col("spending") < 1000)) & (col("membership") == "Standard"), "Average Spender")
        .otherwise("Low Spender")
    )
)
category_df.show(truncate=False)

# Also: avg spending by membership (like your example)
avg_spend_df = category_df.groupBy("membership").agg(avg("spending").alias("avg_spend"))
# avg_spend_df.show(truncate=False)

# ================================================================
# 2) City × Membership pivot: counts and average spends
#    Purpose: surface where high-value customers cluster
# ================================================================
city_mem_pivot = (
    joined.groupBy("city")
          .pivot("membership", ["Basic","Standard","Premium"])
          .agg(
              count("*").alias("n"),
              avg("spending").alias("avg_spend"),
              avg("total_purchase_amount").alias("avg_total_purchase")
          )
)
city_mem_pivot.show(truncate=False)

# ================================================================
# 3) Top-3 by spending within each city
#    Purpose: local champions per city
# ================================================================
w_city_top = Window.partitionBy("city").orderBy(col("spending").desc())
top3_city = joined.withColumn("rk", dense_rank().over(w_city_top)).filter(col("rk") <= 3).drop("rk")
top3_city.show(truncate=False)

# ================================================================
# 4) Recency risk label + cross-tab by membership
#    Purpose: combine recency + spend to tag risk
# ================================================================
risk_df = joined.withColumn(
    "risk_status",
    when((col("days_since_last_purchase") > 90) & (col("spending") < 800), "High Risk")
    .when((col("days_since_last_purchase") > 60), "Moderate Risk")
    .otherwise("Low Risk")
)
risk_ct = risk_df.groupBy("membership","risk_status").count().orderBy("membership","risk_status")
risk_ct.show(truncate=False)

# ================================================================
# 5) Age buckets + average total_purchase_amount
#    Purpose: age-cohort monetization view
# ================================================================
age_bucketed = joined.withColumn(
    "age_bucket",
    when(col("age") < 25, "18-24")
    .when(col("age") < 35, "25-34")
    .when(col("age") < 45, "35-44")
    .when(col("age") < 55, "45-54")
    .otherwise("55+")
)
age_stats = age_bucketed.groupBy("age_bucket","membership").agg(
    count("*").alias("n"),
    avg("total_purchase_amount").alias("avg_total_purchase"),
    avg("spending").alias("avg_spending")
).orderBy("age_bucket","membership")
age_stats.show(truncate=False)

# ================================================================
# 6) Per-city robust outliers via MAD on spending
#    Purpose: detect anomalously high spenders per city
# ================================================================
city_med = joined.groupBy("city").agg(expr("percentile_approx(spending, 0.5, 100)").alias("med"))
with_med = joined.join(city_med, "city")
mad_df = with_med.groupBy("city").agg(expr("percentile_approx(abs(spending - med), 0.5, 100)").alias("mad"))
rob = with_med.join(mad_df, "city")
rob_out_spending = rob.withColumn("robust_z", (col("spending") - col("med"))/col("mad")) \
                      .filter((col("mad") > 0) & (col("robust_z") > 6))
rob_out_spending.select("name","city","spending","robust_z").show(truncate=False)

# ================================================================
# 7) Spend z-score within membership
#    Purpose: relative position vs peers in same plan
# ================================================================
w_mem = Window.partitionBy("membership")
z_df = joined.withColumn("mean_m", avg("spending").over(w_mem)) \
             .withColumn("sd_m", stddev_pop("spending").over(w_mem)) \
             .withColumn("z_spending", (col("spending") - col("mean_m")) / col("sd_m"))
z_df.orderBy(col("z_spending").desc()).show(truncate=False)

# ================================================================
# 8) Deciles of total_purchase_amount overall and by membership
#    Purpose: build decile segments for targeting
# ================================================================
w_overall = Window.orderBy(col("total_purchase_amount"))
dec_overall = joined.withColumn("decile_all", ntile(10).over(w_overall))
w_mem_dec = Window.partitionBy("membership").orderBy(col("total_purchase_amount"))
dec_both = dec_overall.withColumn("decile_in_membership", ntile(10).over(w_mem_dec))
dec_both.groupBy("membership","decile_in_membership").agg(avg("total_purchase_amount")).show(truncate=False)

# ================================================================
# 9) City share of value (spend & total_purchase_amount)
#    Purpose: rank cities by per-capita and total value
# ================================================================
city_value = joined.groupBy("city").agg(
    count("*").alias("n"),
    sum("spending").alias("sum_spending"),
    sum("total_purchase_amount").alias("sum_total_purchase"),
    avg("spending").alias("avg_spending"),
    avg("total_purchase_amount").alias("avg_total_purchase")
).orderBy(col("sum_total_purchase").desc())
city_value.show(truncate=False)

# ================================================================
# 10) Consistency check rules
#     Purpose: flag odd combos (e.g., Premium but very low spend)
# ================================================================
inconsistencies = joined.withColumn(
    "flag",
    when((col("membership") == "Premium") & (col("spending") < 400), "Premium-LowSpend")
    .when((col("membership") == "Basic") & (col("spending") > 2000), "Basic-HighSpend")
    .otherwise(lit(None))
).filter(col("flag").isNotNull())
inconsistencies.show(truncate=False)

# ================================================================
# 11) Composite loyalty score & rank
#     Purpose: combine normalized features to rank customers
# ================================================================
w_all = Window.orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
scored = (
    joined
    .withColumn("spend_norm", col("spending") / max("spending").over(w_all))
    .withColumn("total_norm", col("total_purchase_amount") / max("total_purchase_amount").over(w_all))
    .withColumn("recency_norm", 1 - (col("days_since_last_purchase") / max("days_since_last_purchase").over(w_all)))  # lower recency better
    .withColumn("loyalty_score", 0.45*col("spend_norm") + 0.35*col("total_norm") + 0.20*col("recency_norm"))
)
ranked_loyal = scored.orderBy(col("loyalty_score").desc())
ranked_loyal.show(truncate=False)

# ================================================================
# 12) Weekend-like promo simulation and uplift
#     Purpose: A/B style uplift using seeded randomness
# ================================================================
seed = 2025
ab = joined.withColumn("treated", (rand(seed) > 0.5).cast("int")) \
           .withColumn("spend_after", when(col("treated")==1, col("spending")*1.05).otherwise(col("spending")))
uplift = ab.groupBy("treated").agg(avg("spend_after").alias("avg_spend_after"))
uplift.show(truncate=False)

# ================================================================
# 13) Correlation by membership (age vs spending, recency vs spend)
#     Purpose: direction & strength of relationships
# ================================================================
corr_age_spend = joined.groupBy("membership").agg(corr("age","spending").alias("corr_age_spending"))
corr_rec_spend = joined.groupBy("membership").agg(corr("days_since_last_purchase","spending").alias("corr_recency_spending"))
corr_age_spend.show(truncate=False); corr_rec_spend.show(truncate=False)

# ================================================================
# 14) Time-proxy trend: approximate spend trend using recency ranking
#     Purpose: simple trend proxy when dates are absent
# ================================================================
w_city_rec = Window.partitionBy("city").orderBy(col("days_since_last_purchase").asc())
trend_proxy = joined.withColumn("recent_rank_in_city", dense_rank().over(w_city_rec)) \
                    .groupBy("city").agg(avg("recent_rank_in_city").alias("avg_recency_rank"))
trend_proxy.show(truncate=False)

# ================================================================
# 15) Upgrade / Winback recommendations
#     Purpose: action rules for CRM
# ================================================================
actions = (
    risk_df  # reuse with risk_status
    .withColumn(
        "recommendation",
        when((col("membership")=="Basic") & (col("spending") > 1200), "Offer Upgrade to Standard")
        .when((col("membership")=="Standard") & (col("spending") > 1800) & (col("risk_status")=="Low Risk"), "Offer Upgrade to Premium")
        .when((col("membership")=="Premium") & (col("risk_status")!="Low Risk"), "Winback Campaign")
        .otherwise("No Action")
    )
)
actions.groupBy("recommendation").count().orderBy("recommendation").show(truncate=False)

# ---------------------------------------------------------------
# OPTIONAL: display a few key outputs (uncomment as needed)
# ---------------------------------------------------------------
# category_df.show(truncate=False)
# avg_spend_df.show(truncate=False)
# top3_city.show(truncate=False)
# risk_ct.show(truncate=False)
# age_stats.show(truncate=False)
# rob_out_spending.select("name","city","spending","robust_z").show(truncate=False)
# z_df.orderBy(col("z_spending").desc()).show(truncate=False)
# dec_both.groupBy("membership","decile_in_membership").agg(avg("total_purchase_amount")).show(truncate=False)
# city_value.show(truncate=False)
# inconsistencies.show(truncate=False)
# ranked_loyal.select("name","membership","loyalty_score").show(truncate=False)
# uplift.show(truncate=False)
# corr_age_spend.show(truncate=False); corr_rec_spend.show(truncate=False)
# trend_proxy.show(truncate=False)
# actions.groupBy("recommendation").count().orderBy("recommendation").show(truncate=False)