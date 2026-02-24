from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\bank-fraud-transactions.csv"
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(data)
#df.show()

possible_formats = ["ddMMyyyy","dd-MM-yyyy","dd/MM/yyyy", "dd-MMM-yyyy", "dd/MMM/yyyy", "d-MMM-yyyy", "yyyy-MMM-d","yyyy-MM-dd"]
def dateto(colname, formats=possible_formats):
    return coalesce(*[to_date(col(colname), fmt) for fmt in formats])
df = df.withColumn("transaction_date", dateto("transaction_date"))
df.show()
transactions_df=df
# ---------------------------------------------------------------------
# 1) Risk Classification (baseline, extended)
# Purpose: label risk tiers using amount + frequency
# ---------------------------------------------------------------------
classify_df = (
    df
    .withColumn(
        "classification",
        when((col("amount") > 10000) & (col("frequency") > 5), "High Risk")
         .when(col("amount").between(5000, 10000) & col("frequency").between(2, 5), "Moderate Risk")
         .otherwise("Low Risk")
    )
)
classify_df.show(truncate=False)

# ---------------------------------------------------------------------
# 2) Daily Total Transactions per Account Type
# Purpose: daily aggregates for monitoring load/volume
# ---------------------------------------------------------------------
daily_totals = (
    transactions_df
    .groupBy("transaction_date", "account_type")
    .agg(sum("amount").alias("total_amount"), count("*").alias("txn_count"))
)
daily_totals.show(truncate=False)

# ---------------------------------------------------------------------
# 3) Rolling Average Amount per Account (Window)
# Purpose: smooth short-term behavior per account (last 3 rows)
# ---------------------------------------------------------------------
w_roll3 = Window.partitionBy("account_id").orderBy("transaction_date").rowsBetween(-2, 0)
rolling_avg = transactions_df.withColumn("rolling_avg_amt", avg("amount").over(w_roll3))
rolling_avg.show(truncate=False)

# ---------------------------------------------------------------------
# 4) Fraud-Like Detection: Multiple High-Frequency Days
# Purpose: accounts with high average frequency overall
# ---------------------------------------------------------------------
fraud_flag = (
    transactions_df
    .groupBy("account_id")
    .agg(avg("frequency").alias("avg_frequency"))
    .filter(col("avg_frequency") > 6)
)
fraud_flag.show(truncate=False)

# ---------------------------------------------------------------------
# 5) Pivot Table: Risk Classification × Account Type
# Purpose: distribution of risk by account type
# ---------------------------------------------------------------------
pivot_df = (
    classify_df
    .groupBy("account_type")
    .pivot("classification", ["Low Risk", "Moderate Risk", "High Risk"])
    .agg(count("*"))
    .fillna(0)
)
pivot_df.show(truncate=False)

# ---------------------------------------------------------------------
# 6) Top 3 Highest Transactions Per Account Type
# Purpose: find extreme transactions per type
# ---------------------------------------------------------------------
w_top = Window.partitionBy("account_type").orderBy(col("amount").desc())
top3 = (
    transactions_df
    .withColumn("rnk", dense_rank().over(w_top))
    .filter(col("rnk") <= 3)
    .drop("rnk")
)
top3.show(truncate=False)

# ---------------------------------------------------------------------
# 7) Month-over-Month Growth in Transaction Amounts
# Purpose: macro trend analysis (MoM % growth)
# ---------------------------------------------------------------------
month_df = transactions_df.withColumn("month", date_format("transaction_date", "yyyy-MM"))
monthly_totals = month_df.groupBy("month").agg(sum("amount").alias("total_amount"))
w_mom = Window.orderBy("month")
mom_growth = (
    monthly_totals
    .withColumn("prev_total", lag("total_amount").over(w_mom))
    .withColumn("growth_pct", when(col("prev_total").isNotNull(),
                                     (col("total_amount") - col("prev_total")) / col("prev_total") * 100))
)
mom_growth.show(truncate=False)

# ---------------------------------------------------------------------
# 8) Detect “Unusual Spikes” (Amount > 2× Avg per Account)
# Purpose: account-specific amount spikes
# ---------------------------------------------------------------------
avg_per_account = transactions_df.groupBy("account_id").agg(avg("amount").alias("avg_amt"))
with_avg = transactions_df.join(avg_per_account, "account_id")
spikes = with_avg.filter(col("amount") > 2 * col("avg_amt"))
spikes.show(truncate=False)

# ---------------------------------------------------------------------
# 9) Rank Accounts by Total Amount Across All Transactions
# Purpose: top-value accounts
# ---------------------------------------------------------------------
account_rank = (
    transactions_df
    .groupBy("account_id")
    .agg(sum("amount").alias("total_amt"))
    .withColumn("rank", dense_rank().over(Window.orderBy(col("total_amt").desc())))
)
account_rank.show(truncate=False)

# ---------------------------------------------------------------------
# 10) Time-of-Year Pattern (Quarterly Aggregation)
# Purpose: seasonal/quarterly distribution by type
# ---------------------------------------------------------------------
quarterly = transactions_df.withColumn("quarter", quarter("transaction_date"))
quarterly_summary = (
    quarterly
    .groupBy("quarter", "account_type")
    .agg(count("*").alias("txn_count"), sum("amount").alias("total_amt"))
    .orderBy("quarter", "account_type")
)
quarterly_summary.show(truncate=False)

# ========================= Medium evel Tasks  ====================

# ---------------------------------------------------------------------
# M1) Velocity day rule: high amount & high frequency same day
# Purpose: same-day spikes in value + count
# ---------------------------------------------------------------------
velocity_flags = (
    transactions_df
    .groupBy("account_id", "transaction_date")
    .agg(sum("amount").alias("day_amount"), sum("frequency").alias("day_freq"))
    .filter((col("day_amount") > 30000) & (col("day_freq") >= 6))
)
velocity_flags.show(truncate=False)

# ---------------------------------------------------------------------
# M2) 3-day rolling surge in frequency (per account)
# Purpose: short-window frequency surges
# ---------------------------------------------------------------------
w_freq3 = Window.partitionBy("account_id").orderBy("transaction_date").rowsBetween(-2, 0)
surge_3d = (
    transactions_df
    .withColumn("freq_3d", sum("frequency").over(w_freq3))
    .filter(col("freq_3d") >= 12)
)
surge_3d.show(truncate=False)

# ---------------------------------------------------------------------
# M3) Spike vs own history (amount > μ + 3σ per account)
# Purpose: statistical outliers per account
# ---------------------------------------------------------------------
w_acc = Window.partitionBy("account_id")
z_flag = (
    transactions_df
    .withColumn("mean_amt", avg("amount").over(w_acc))
    .withColumn("sd_amt", stddev_pop("amount").over(w_acc))
    .withColumn("is_spike", col("amount") > col("mean_amt") + 3*col("sd_amt"))
    .filter(col("is_spike"))
)
z_flag.show(truncate=False)

# ---------------------------------------------------------------------
# M4) Peer group outlier (above P99 by account_type & month)
# Purpose: compare within monthly peer group
# ---------------------------------------------------------------------
month_df2 = transactions_df.withColumn("ym", date_format("transaction_date","yyyy-MM"))
p99 = month_df2.groupBy("account_type","ym").agg(expr("percentile_approx(amount, 0.99, 100)").alias("p99_amt"))
peer_outliers = month_df2.join(p99, ["account_type","ym"]).filter(col("amount") > col("p99_amt"))
peer_outliers.show(truncate=False)

# ---------------------------------------------------------------------
# M5) Weekend anomaly vs weekday typical (per account)
# Purpose: abnormal weekend behavior
# Note: Spark dayofweek(): 1=Sun, 7=Sat
# ---------------------------------------------------------------------
wkd = transactions_df.withColumn("is_weekend", dayofweek("transaction_date").isin([1,7]))
wk_stats = (
    wkd.filter(~col("is_weekend"))
       .groupBy("account_id")
       .agg(avg("amount").alias("wk_avg"), stddev_pop("amount").alias("wk_sd"))
)
weekend_anom = (
    wkd.join(wk_stats, "account_id", "left")
       .filter(col("is_weekend") & (col("wk_sd").isNotNull()) & (col("amount") > col("wk_avg") + 2*col("wk_sd")))
)
weekend_anom.show(truncate=False)

# ---------------------------------------------------------------------
# M6) Structuring near threshold (split just under 10,000)
# Purpose: possible structuring to avoid checks
# ---------------------------------------------------------------------
near_thresh = transactions_df.filter((col("amount") >= 9000) & (col("amount") < 10000))
pattern_accounts = near_thresh.groupBy("account_id").count().filter(col("count") >= 3)
pattern_accounts.show(truncate=False)

# ---------------------------------------------------------------------
# M7) Unusual low-frequency huge transaction (single big hit)
# Purpose: big one-off hits
# ---------------------------------------------------------------------
lh_flags = transactions_df.filter((col("frequency") == 1) & (col("amount") >= 40000))
lh_flags.show(truncate=False)

# ---------------------------------------------------------------------
# M8) Repeated high-risk days (>= 3 days meeting rule)
# Purpose: persistent risky behavior
# ---------------------------------------------------------------------
rule = transactions_df.withColumn("high_day", (col("amount") > 15000) & (col("frequency") >= 5))
repeaters = (
    rule.groupBy("account_id")
        .agg(sum(col("high_day").cast("int")).alias("high_days"))
        .filter(col("high_days") >= 3)
)
repeaters.show(truncate=False)

# ---------------------------------------------------------------------
# M9) Cross-type rarity + intensity (rare type, big value)
# Purpose: rare account types carrying heavy amounts
# ---------------------------------------------------------------------
type_share = transactions_df.groupBy("account_type").count()
total_txn = transactions_df.count()
type_rates = type_share.withColumn("share", col("count")/lit(total_txn))
rare_types = type_rates.filter(col("share") < 0.15).select("account_type")
rare_heavy = transactions_df.join(rare_types, "account_type").filter(col("amount") > 25000)
rare_heavy.show(truncate=False)

# ---------------------------------------------------------------------
# M10) Time-proximity bursts (short gaps with large totals)
# Purpose: frequent hits within very short gaps
# ---------------------------------------------------------------------
w_time = Window.partitionBy("account_id").orderBy("transaction_date")
gaps = (
    transactions_df
    .withColumn("prev_dt", lag("transaction_date").over(w_time))
    .withColumn("gap_days", datediff("transaction_date","prev_dt"))
    .withColumn("is_burst", (col("gap_days") <= 2) & (col("amount") > 12000))
    .filter(col("is_burst"))
)
gaps.show(truncate=False)

# ===================== Very Advanced / Challenging tasks (5) =================

# ---------------------------------------------------------------------
# A) Robust peer-group anomaly with MAD (per account_type & month)
# Purpose: robust outliers using median & MAD
# ---------------------------------------------------------------------
ym_df = transactions_df.withColumn("ym", date_format("transaction_date","yyyy-MM"))
grp = ym_df.groupBy("account_type","ym")
med = grp.agg(expr("percentile_approx(amount, 0.5, 100)").alias("med"))
joined = ym_df.join(med, ["account_type","ym"])
mad = joined.groupBy("account_type","ym").agg(
    expr("percentile_approx(abs(amount - med), 0.5, 100)").alias("mad")
)
rob = joined.join(mad, ["account_type","ym"])
robust_z = (
    rob.withColumn("robust_z", (col("amount") - col("med")) / col("mad"))
       .filter((col("mad") > 0) & (col("robust_z") > 6))  # tune threshold
)
robust_z.show(truncate=False)

# ---------------------------------------------------------------------
# B) Change-point style flag using rolling expectation & residuals
# Purpose: sudden level shifts relative to recent history
# ---------------------------------------------------------------------
w_ord = Window.partitionBy("account_id").orderBy("transaction_date").rowsBetween(-6, -1)
exp = (
    transactions_df
    .withColumn("exp_amt", avg("amount").over(w_ord))
    .withColumn("resid", col("amount") - col("exp_amt"))
    .filter(col("exp_amt").isNotNull() & (abs(col("resid")) > 2.5*abs(col("exp_amt"))))
)
exp.show(truncate=False)

# ---------------------------------------------------------------------
# C) Sequence anomaly: rare transitions between binned amount states
# Purpose: unusual next-state transitions across all accounts
# ---------------------------------------------------------------------
bins = transactions_df.withColumn(
    "amt_bin",
    when(col("amount") < 5000, "A")
     .when(col("amount") < 10000, "B")
     .when(col("amount") < 20000, "C")
     .when(col("amount") < 35000, "D")
     .otherwise("E")
)
w_seq = Window.partitionBy("account_id").orderBy("transaction_date")
trans = bins.withColumn("next_bin", lead("amt_bin").over(w_seq)).filter(col("next_bin").isNotNull())
trans_freq = trans.groupBy("amt_bin","next_bin").count()
total_trans = trans_freq.agg(sum("count").alias("tot")).collect()[0]["tot"]
rare_trans = trans_freq.withColumn("p", col("count")/lit(total_trans)).filter(col("p") < 0.01)
seq_anoms = trans.join(rare_trans, ["amt_bin","next_bin"])
seq_anoms.show(truncate=False)

# ---------------------------------------------------------------------
# D) Benford’s Law deviation per account/month (chi-square flag)
# Purpose: forensic pattern check on leading digits
# ---------------------------------------------------------------------
with_ld = (
    transactions_df
    .withColumn("ld", substring(col("amount").cast("string"), 1, 1).cast("int"))
    .filter((col("ld") >= 1) & (col("ld") <= 9))
    .withColumn("ym", date_format("transaction_date","yyyy-MM"))
)
obs = with_ld.groupBy("account_id","ym","ld").count()
benford = [(1,0.3010),(2,0.1761),(3,0.1249),(4,0.0969),(5,0.0792),(6,0.0669),(7,0.0580),(8,0.0512),(9,0.0458)]
benford_df = spark.createDataFrame(benford, ["ld","p_exp"])
totals = with_ld.groupBy("account_id","ym").count().withColumnRenamed("count","n")
exp_b = (obs.join(totals, ["account_id","ym"]).join(benford_df, "ld").withColumn("E", col("n")*col("p_exp")))
chisq = exp_b.withColumn("term", (col("count") - col("E"))**2 / col("E")) \
             .groupBy("account_id","ym").agg(sum("term").alias("chi2"))
flags_benford = chisq.filter(col("chi2") > 20)  # tune threshold (df=8)
flags_benford.show(truncate=False)

# ---------------------------------------------------------------------
# E) Composite fraud score (feature engineering + weighted score)
# Purpose: aggregate weak signals into a single ranking
# ---------------------------------------------------------------------
w_acc2 = Window.partitionBy("account_id")
feat = (
    transactions_df
    .withColumn("is_weekend", dayofweek("transaction_date").isin([1,7]).cast("int"))
    .withColumn("z_amt",
                (col("amount") - avg("amount").over(w_acc2)) / stddev_pop("amount").over(w_acc2))
    .withColumn("high_freq", (col("frequency") >= 6).cast("int"))
    .withColumn("near_10k", ((col("amount") >= 9000) & (col("amount") < 10000)).cast("int"))
)
score = feat.withColumn(
    "fraud_score",
    1.2*coalesce(col("z_amt"), lit(0.0)) + 0.8*col("high_freq") + 0.6*col("near_10k") + 0.4*col("is_weekend")
)
top_suspects = score.orderBy(col("fraud_score").desc()).limit(50)
top_suspects.show(truncate=False)

# ---------------------------------------------------------------------
# Bonus: small fixes pattern (reference)
# Purpose: correct alias/filter pattern style
# ---------------------------------------------------------------------
# from pyspark.sql.functions import col, sum as Fsum, when
# classify_df = transactions_df.withColumn(
#     "classification",
#     when((col("amount")>10000) & (col("frequency")>5), "High Risk")
#     .when(col("amount").between(5000,10000) & col("frequency").between(2,5), "Moderate Risk")
#     .otherwise("Low Risk")
# )
# trans_df   = classify_df.groupBy("classification").agg(Fsum(col("frequency")).alias("trans_count"))
# tot_tran_df= classify_df.filter(col("classification")=="High Risk").agg(Fsum(col("amount")).alias("total_tran_amt"))
# mod_risk_df= classify_df.filter((col("classification")=="Moderate Risk") & (col("account_type")=="Savings") & (col("amount")>7500))



classify_df=transactions_df.withColumn("classification",when(((col("amount")>10000) & (col("frequency")>5)),"High Risk")\
                                       .when(col("amount").between(5000,10000) & (col("frequency").between(2,5)),"Moderate Risk")\
                                       .otherwise("Low Risk"))
classify_df.show()

trans_df=classify_df.groupBy(col("classification")).agg(sum(col("frequency")).alias("trans_count"))
trans_df.show()

tot_tran_df=classify_df.filter(col("classification")=="High Risk").agg(sum(col("amount")).alias("total_tran_amn"))
tot_tran_df.show()

mod_risk_df=classify_df.filter((col("classification")=="Moderate Risk") & (col("account_type")=="Savings") & (col("amount")>7500).alias(",mod_risk_transaction"))
mod_risk_df.show()