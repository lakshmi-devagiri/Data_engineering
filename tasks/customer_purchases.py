from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\customer_purchases.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()
# Step 1: Create a frequency label for each customer based on recency
# - If last purchase < 30 days → Frequent
# - If 30–60 days → Occasional
# - Else → Rare
frequency_df = df.withColumn(
    "frequency",
    when((col("days_since_last_purchase") < 30), "Frequent")
    .when(((col("days_since_last_purchase") >= 30) & (col("days_since_last_purchase") <= 60)), "Occasional")
    .otherwise("Rare")
)
frequency_df.show()

# Step 2: Filter only "Premium" customers who are "Frequent"
# Purpose: narrow down to loyal premium customers
filter_df = frequency_df.filter(
    (col("membership") == "Premium") & (col("frequency") == "Frequent")
)
filter_df.show()

# (Optional) Step 3: Calculate the average purchase amount for Premium + Frequent
# Same as Step 2 but directly aggregated instead of showing rows
# filter_df = frequency_df.filter((col("membership")=="Premium") & (col("frequency")=="Frequent"))\
#                         .agg(avg(col("total_purchase_amount")).alias("avg_pur_amount"))
# filter_df.show()

# Step 4: Group by frequency × membership → find avg purchase amount
# Here we group only the already filtered rows (Premium + Frequent).
avg_tot_purchase_df = (
    filter_df.groupBy(col("frequency"), col("membership"))
             .agg(avg(col("total_purchase_amount")).alias("avg_pur_amount"))
)
avg_tot_purchase_df.show()

# Step 5: Alternative approach — group all rows by membership × frequency
# Then filter only Premium + Frequent segment in the final result.
avg_tot_purchase_df = (
    frequency_df.groupBy(col("membership"), col("frequency"))
                .agg(avg(col("total_purchase_amount")).alias("avg_pur_amount"))
                .filter((col("membership") == "Premium") & (col("frequency") == "Frequent"))
)
avg_tot_purchase_df.show()

# Step 6: Find the minimum purchase amount for Rare customers (any membership)
# Purpose: identify the weakest spenders in the Rare segment.
min_purc_amn_df = (
    frequency_df.groupBy(col("frequency"), col("membership"))
                .agg(min(col("total_purchase_amount")).alias("min_purc_amn"))
                .filter(col("frequency") == "Rare")
)
min_purc_amn_df.show()

# 1) Average ticket by membership × frequency (generalized)
# Purpose: compare spend intensity across segments.
avg_tot_purchase_df = (
    frequency_df
    .groupBy("membership","frequency")
    .agg(avg("total_purchase_amount").alias("avg_pur_amount"),
         count("*").alias("customers"))
    .orderBy("membership","frequency")
)
avg_tot_purchase_df.show(truncate=False)

# 2) Top-3 customers by spend within each membership
# Purpose: window ranking per cohort.
w_mem_top = Window.partitionBy("membership").orderBy(col("total_purchase_amount").desc())
top3 = (
    df
    .withColumn("rk", dense_rank().over(w_mem_top))
    .filter(col("rk") <= 3)
    .drop("rk")
)
top3.show(truncate=False)

# 3) Spend z-score within membership
# Purpose: flag unusually high/low spenders relative to peers.
w_mem = Window.partitionBy("membership")
z_df = (
    df
    .withColumn("avg_mem", avg("total_purchase_amount").over(w_mem))
    .withColumn("sd_mem",  stddev_pop("total_purchase_amount").over(w_mem))
    .withColumn("z_spend", (col("total_purchase_amount") - col("avg_mem")) / col("sd_mem"))
)
z_df.orderBy(col("z_spend").desc()).show(truncate=False)

# 4) Recency buckets + pivot heatmap
# Purpose: quick “who is active vs dormant” by membership.
rec_bkt = (
    df
    .withColumn("recency_bucket",
        when(col("days_since_last_purchase") < 15, "0-14")
        .when(col("days_since_last_purchase") < 30, "15-29")
        .when(col("days_since_last_purchase") < 60, "30-59")
        .otherwise("60+")
    )
)
pivot_tbl = (
    rec_bkt.groupBy("membership")
           .pivot("recency_bucket", ["0-14","15-29","30-59","60+"])
           .agg(count("*"))
           .fillna(0)
)
pivot_tbl.show(truncate=False)

# 5) Quantile bands (P25/P50/P75) per membership
# Purpose: robust thresholds for targeting.
q = (
    df
    .groupBy("membership")
    .agg(expr("percentile_approx(total_purchase_amount, array(0.25,0.5,0.75), 100)").alias("qs"))
    .select("membership", col("qs")[0].alias("p25"), col("qs")[1].alias("p50"), col("qs")[2].alias("p75"))
)
q.show(truncate=False)

# 6) Synthetic “loyalty_score” and rank
# Purpose: combine recency + spend into one sortable score.
# (Use a whole-dataset window to compute global maxima)
w_all = Window.orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
score_df = (
    df
    .withColumn("loyalty_score",
        0.7 * (col("total_purchase_amount") / max("total_purchase_amount").over(w_all)) +
        0.3 * (1 - col("days_since_last_purchase") / max("days_since_last_purchase").over(w_all))
    )
)
score_df.orderBy(col("loyalty_score").desc()).show(truncate=False)

# 7) “At-risk Premium” customers
# Purpose: Premium + high recency + low spend.
at_risk = (
    df
    .filter((col("membership")=="Premium") &
            (col("days_since_last_purchase") > 60) &
            (col("total_purchase_amount") < 3000))
)
at_risk.show(truncate=False)

# 8) Cohort proxy from recency + average spend
# Purpose: mock cohorts without actual dates.
cohort = (
    df
    .withColumn("cohort_proxy_month",
        when(col("days_since_last_purchase") <= 30,  lit("M0"))
        .when(col("days_since_last_purchase") <= 60, lit("M1"))
        .when(col("days_since_last_purchase") <= 90, lit("M2"))
        .otherwise("M3+")
    )
    .groupBy("membership","cohort_proxy_month")
    .agg(avg("total_purchase_amount").alias("avg_spend"), count("*").alias("n"))
    .orderBy("membership","cohort_proxy_month")
)
cohort.show(truncate=False)

# 9) Price-sensitivity proxy via discount simulation (seeded)
# Purpose: uplift test with reproducible randomness.
seed = 2025
sim = (
    df
    .withColumn("treated", (rand(seed) > 0.5).cast("int"))
    .withColumn("spend_after",
        when(col("treated")==1, col("total_purchase_amount")*1.05)
        .otherwise(col("total_purchase_amount"))
    )
)
uplift = sim.groupBy("treated").agg(avg("spend_after").alias("avg_after"))
uplift.show(truncate=False)

# 10) Spend deciles overall and within membership
# Purpose: segmentation by deciles for targeting.
w_overall = Window.orderBy(col("total_purchase_amount"))
dec_overall = df.withColumn("decile_overall", ntile(10).over(w_overall))
w_mem_dec = Window.partitionBy("membership").orderBy(col("total_purchase_amount"))
dec_mem = dec_overall.withColumn("decile_in_membership", ntile(10).over(w_mem_dec))
dec_mem.groupBy("membership","decile_in_membership").agg(avg("total_purchase_amount").alias("avg_amt")).orderBy("membership","decile_in_membership").show(truncate=False)

# 11) Frequent vs Infrequent spend distribution comparison
# Purpose: compare histograms (KS proxy) across segments.
freq_flag = frequency_df.withColumn("is_freq", (col("frequency")=="Frequent").cast("int"))
bins_all = (
    df
    .select(width_bucket("total_purchase_amount", lit(0), lit(10000), lit(10)).alias("bin"))
    .groupBy("bin").agg(count("*").alias("n_all"))
)
freq_bins = (
    freq_flag
    .select(width_bucket("total_purchase_amount", lit(0), lit(10000), lit(10)).alias("bin"), "is_freq")
    .groupBy("bin","is_freq").count()
    .groupBy("bin").pivot("is_freq", [0,1]).agg(sum("count"))
    .withColumnRenamed("0","n_infreq").withColumnRenamed("1","n_freq")
    .fillna(0)
)
freq_compare = bins_all.join(freq_bins, "bin", "left").fillna(0).orderBy("bin")
freq_compare.show(truncate=False)

# 12) Rule-based upgrade/downgrade recommendation
# Purpose: simulate CRM actions.
reco = (
    frequency_df
    .withColumn(
        "recommendation",
        when((col("membership")=="Basic") & (col("frequency")=="Frequent") & (col("total_purchase_amount") > 6000), "Upgrade to Standard")
        .when((col("membership")=="Standard") & (col("frequency")=="Frequent") & (col("total_purchase_amount") > 8000), "Upgrade to Premium")
        .when((col("membership")=="Premium") & (col("frequency")=="Rare"), "Winback Campaign")
        .otherwise("No Action"))
    )

reco.groupBy("recommendation").count().orderBy("recommendation").show(truncate=False)