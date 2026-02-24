from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\loan_applicants.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()

risk_df=df.withColumn("risk",when(((col("loan_amount")>=(2*col("income"))) & (col("credit_score")<600)),"High Risk")\
                                      .when(((col("loan_amount")>col("income")) & (col("loan_amount")<=2*col("income"))) & col("credit_score").between(600,700),"Moderate Risk")\
                                      .otherwise("Low Risk"))

income_range_df=risk_df.withColumn("income-range",when((col("income")<=50000),"less_50k")\
    .when((col("income")>50000) & (col("income")<=100000),"btwn_50_100k")\
    .otherwise("more_than_100k")
   )
income_range_df.show()

avg_loan_df=income_range_df.filter(col("risk")=="High Risk").groupBy(col("income-range")).agg(avg(col("loan_amount")).alias("avg_loan_amount"))
avg_loan_df.show()

#avg_cred_score_df=income_range_df.groupBy(col("income-range"),col("risk")).agg(avg(col("credit_score")).alias("avg_cred_score"))
avg_cred_score_df=income_range_df.groupBy(col("income-range"),col("risk")).agg(avg(col("credit_score")).alias("avg_cred_score")).filter(col("avg_cred_score")<650)
avg_cred_score_df.show()

#1) Risk classification (your logic)
#    High:    loan_amount >= 2*income AND credit_score < 600
#    Moderate: loan_amount in (income, 2*income] AND credit_score 600..700
#    Else:    Low
# ----------------------------------------------------------------------
risk_df = (
    df.withColumn(
        "risk",
        when((col("loan_amount") >= 2*col("income")) & (col("credit_score") < 600), "High Risk")
        .when((col("loan_amount") > col("income")) & (col("loan_amount") <= 2*col("income")) & col("credit_score").between(600,700), "Moderate Risk")
        .otherwise("Low Risk")
    )
)

# ----------------------------------------------------------------------
# 2) Income range buckets (your logic)
# ----------------------------------------------------------------------
income_range_df = (
    risk_df
    .withColumn(
        "income-range",
        when(col("income") <= 50000, "less_50k")
        .when((col("income") > 50000) & (col("income") <= 100000), "btwn_50_100k")
        .otherwise("more_than_100k")
    )
)
# income_range_df.show(truncate=False)

# ----------------------------------------------------------------------
# 3) Average loan for High Risk by income-range (your logic)
# ----------------------------------------------------------------------
avg_loan_df = (
    income_range_df
    .filter(col("risk") == "High Risk")
    .groupBy(col("income-range"))
    .agg(avg(col("loan_amount")).alias("avg_loan_amount"))
)
# avg_loan_df.show(truncate=False)

# ----------------------------------------------------------------------
# 4) Avg credit score by income-range × risk; filter where avg < 650 (your idea)
# ----------------------------------------------------------------------
avg_cred_score_df = (
    income_range_df
    .groupBy(col("income-range"), col("risk"))
    .agg(avg(col("credit_score")).alias("avg_cred_score"))
    .filter(col("avg_cred_score") < 650)
)
# avg_cred_score_df.show(truncate=False)

# ======================= EXTRA TASKS (MED/ADV) ========================

# ----------------------------------------------------------------------
# 5) Debt-to-Income (DTI) & buckets + pivot risk × DTI bucket
#    Purpose: standard risk ratio view and intersection with risk categories.
# ----------------------------------------------------------------------
with_dti = income_range_df.withColumn("dti", col("loan_amount")/col("income"))
with_dti = with_dti.withColumn(
    "dti_bucket",
    when(col("dti") <= 0.8, "<=0.8")
    .when(col("dti") <= 1.0, "0.8-1.0")
    .when(col("dti") <= 1.5, "1.0-1.5")
    .when(col("dti") <= 2.0, "1.5-2.0")
    .otherwise(">2.0")
)
risk_dti_pivot = (
    with_dti.groupBy("dti_bucket")
            .pivot("risk", ["Low Risk","Moderate Risk","High Risk"])
            .agg(count("*"))
            .fillna(0)
)
# risk_dti_pivot.show(truncate=False)

# ----------------------------------------------------------------------
# 6) Top-N over-leveraged applicants (highest DTI), show name/income/loan/score
#    Purpose: shortlist for manual review.
# ----------------------------------------------------------------------
top_overleveraged = with_dti.orderBy(col("dti").desc()).select("name","income","loan_amount","credit_score","dti").limit(15)
# top_overleveraged.show(truncate=False)

# ----------------------------------------------------------------------
# 7) Window ranking: rank each applicant by loan_amount within their income-range
#    Purpose: peer comparison inside similar income bands.
# ----------------------------------------------------------------------
w_income_band = Window.partitionBy("income-range").orderBy(col("loan_amount").desc())
ranked_by_band = with_dti.withColumn("rank_in_band", dense_rank().over(w_income_band))
# ranked_by_band.orderBy("income-range","rank_in_band").show(truncate=False)

# ----------------------------------------------------------------------
# 8) Outlier flags:
#    A) Extreme leverage: loan_amount > 3 * income
#    B) Very low credit score: credit_score < 520
# ----------------------------------------------------------------------
flags = with_dti.withColumn(
    "extreme_leverage", (col("loan_amount") > 3*col("income")).cast("int")
).withColumn(
    "very_low_score", (col("credit_score") < 520).cast("int")
)
# flags.filter((col("extreme_leverage")==1) | (col("very_low_score")==1)).show(truncate=False)

# ----------------------------------------------------------------------
# 9) Approval suggestion (toy rules):
#    Approve if Low Risk & score >= 680 & DTI<=1.0
#    Review if Moderate Risk OR (score 640..679)
#    Decline if High Risk OR score < 580
# ----------------------------------------------------------------------
suggestion = (
    with_dti.withColumn(
        "decision",
        when((col("risk")=="Low Risk") & (col("credit_score")>=680) & (col("dti")<=1.0), "Approve")
        .when((col("risk")=="High Risk") | (col("credit_score")<580), "Decline")
        .otherwise("Review")
    )
)
# suggestion.groupBy("decision").count().show(truncate=False)

# ----------------------------------------------------------------------
# 10) Risk distribution with shares
#     Purpose: how many and what percent in each risk bucket.
# ----------------------------------------------------------------------
risk_counts = risk_df.groupBy("risk").count()
total_n = risk_df.count()
risk_share = risk_counts.withColumn("share", round(col("count")/lit(total_n), 3))
# risk_share.show(truncate=False)

# ----------------------------------------------------------------------
# 11) Weighted risk score (0..100) combining DTI and credit score
#     Purpose: single score to rank applicants (tunable weights).
# ----------------------------------------------------------------------
# Normalize DTI to 0..1 by clipping at 3.0; normalize credit score to 0..1 from 300..850 (typical range).
scored = (
    with_dti
    .withColumn("dti_cap", least(col("dti"), lit(3.0)))
    .withColumn("dti_norm", col("dti_cap")/lit(3.0))                      # higher worse
    .withColumn("cs_norm", (col("credit_score")-lit(300))/lit(850-300))   # higher better
    .withColumn("risk_score", round(100*(0.65*col("dti_norm") + 0.35*(1-col("cs_norm"))), 1))  # higher = riskier
)
# scored.select("name","income","loan_amount","credit_score","dti","risk_score").orderBy(col("risk_score").desc()).show(truncate=False)

# ----------------------------------------------------------------------
# 12) Credit score bands + cross-tab with risk
#     Purpose: classic score segmentation vs. derived risk categories.
# ----------------------------------------------------------------------
banded = with_dti.withColumn(
    "score_band",
    when(col("credit_score")<580, "Poor")
    .when(col("credit_score")<640, "Fair")
    .when(col("credit_score")<700, "Good")
    .when(col("credit_score")<750, "Very Good")
    .otherwise("Excellent")
)
score_risk_ct = (
    banded.groupBy("score_band")
          .pivot("risk", ["Low Risk","Moderate Risk","High Risk"])
          .agg(count("*"))
          .fillna(0)
)
# score_risk_ct.show(truncate=False)

# ----------------------------------------------------------------------
# 13) Winsorize extreme loan amounts at p99 for robustness, then recompute avg by risk
#     Purpose: robust aggregation that reduces impact of outliers.
# ----------------------------------------------------------------------
p99 = df.agg(expr("percentile_approx(loan_amount, 0.99, 100) as p99")).collect()[0]["p99"]
wins = with_dti.withColumn("loan_wins", when(col("loan_amount") > lit(p99), lit(p99)).otherwise(col("loan_amount")))
winsorized_avg = wins.groupBy("risk").agg(avg("loan_wins").alias("avg_loan_wins"))
# winsorized_avg.show(truncate=False)

# ----------------------------------------------------------------------
# 14) Correlations (overall and by risk bucket)
#     Purpose: relationships among features.
# ----------------------------------------------------------------------
corr_overall_income_loan = df.select(corr("income","loan_amount").alias("corr_income_loan"))
# corr_overall_income_loan.show()
corr_by_risk = with_dti.groupBy("risk").agg(corr("income","loan_amount").alias("corr_income_loan"))
# corr_by_risk.show(truncate=False)

# ----------------------------------------------------------------------
# 15) Z-score of credit_score within income-range (window)
#     Purpose: identify unusually low/high credit scores among similar-income peers.
# ----------------------------------------------------------------------
w_band = Window.partitionBy("income-range")
z_in_band = (
    with_dti
    .withColumn("cs_mu", avg("credit_score").over(w_band))
    .withColumn("cs_sd", stddev_pop("credit_score").over(w_band))
    .withColumn("cs_z", when(col("cs_sd")>0, (col("credit_score")-col("cs_mu"))/col("cs_sd")).otherwise(lit(0.0)))
)
# z_in_band.orderBy(col("cs_z")).show(truncate=False)  # most negative first (worse than peers)

# ----------------------------------------------------------------------
# 16) Collect applicants by high-risk signals (list names per signal)
#     Purpose: compact lists for manual review queues.
# ----------------------------------------------------------------------
signals = flags.withColumn(
    "signal",
    when(col("extreme_leverage")==1, "Extreme Leverage")
    .when(col("very_low_score")==1, "Very Low Score")
    .otherwise(lit(None))
).filter(col("signal").isNotNull())
review_lists = signals.groupBy("signal").agg(collect_list("name").alias("applicants"))
# review_lists.show(truncate=False)

# ----------------------------------------------------------------------
# 17) Most risky applicants per income-range (top-3 by risk_score)
#     Purpose: targeted review inside each income band.
# ----------------------------------------------------------------------
w_risk = Window.partitionBy("income-range").orderBy(col("risk_score").desc(), col("dti").desc())
top3_by_band = scored.withColumn("r", row_number().over(w_risk)).filter(col("r")<=3).drop("r")
# top3_by_band.select("income-range","name","income","loan_amount","credit_score","dti","risk_score").show(truncate=False)

# ----------------------------------------------------------------------
# 18) Aggregated dashboard snapshot
#     Purpose: compact KPI table for reporting.
# ----------------------------------------------------------------------
dashboard = (
    scored.groupBy("risk")
          .agg(
              count("*").alias("n"),
              round(avg("income"),0).alias("avg_income"),
              round(avg("loan_amount"),0).alias("avg_loan"),
              round(avg("credit_score"),0).alias("avg_score"),
              round(avg("dti"),2).alias("avg_dti"),
              round(avg("risk_score"),1).alias("avg_risk_score")
          )
          .orderBy("risk")
)
# dashboard.show(truncate=False)

# --------------------------- QUICK DISPLAYS ----------------------------
# Uncomment the outputs you want to view immediately.

income_range_df.show(truncate=False)
avg_loan_df.show(truncate=False)
avg_cred_score_df.show(truncate=False)

risk_dti_pivot.show(truncate=False)
top_overleveraged.show(truncate=False)
ranked_by_band.orderBy("income-range","rank_in_band").show(truncate=False)
flags.filter((col("extreme_leverage")==1) | (col("very_low_score")==1)).show(truncate=False)
suggestion.groupBy("decision").count().show(truncate=False)
risk_share.show(truncate=False)
scored.select("name","income","loan_amount","credit_score","dti","risk_score").orderBy(col("risk_score").desc()).show(truncate=False)
score_risk_ct.show(truncate=False)
winsorized_avg.show(truncate=False)
corr_overall_income_loan.show()
corr_by_risk.show(truncate=False)
z_in_band.orderBy(col("cs_z")).show(truncate=False)
review_lists.show(truncate=False)
top3_by_band.select("income-range","name","income","loan_amount","credit_score","dti","risk_score").show(truncate=False)
dashboard.show(truncate=False)