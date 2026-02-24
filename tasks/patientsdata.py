from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\Patient_Data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()
#1) Risk categorization (API version of your SQL)
risk_category_df = (
    df.withColumn(
        "risk_status",
        when((col("readmission_interval") < 15) & (col("age") > 60), "High Readmission Risk")
         .when((col("readmission_interval") >= 15) & (col("readmission_interval") <= 30), "Moderate Risk")
         .otherwise("Low Risk")
    )
)
risk_category_df.show(truncate=False)

#2) Count patients in each risk category
patient_count_df = (
    risk_category_df.groupBy("risk_status").agg(count("patient_id").alias("patient_count"))
)
patient_count_df.show()

#3) Average readmission interval per risk category
avg_readmin_interval_df = (
    risk_category_df.groupBy("risk_status")
    .agg(avg("readmission_interval").alias("readmin_interval"))
)
avg_readmin_interval_df.show()

#4) “Moderate Risk” with ICU admissions > 2
icu_admin_count_df = (
    risk_category_df
    .filter((col("risk_status") == "Moderate Risk") & (col("icu_admissions") > 2))
)
icu_admin_count_df.show()


#5) Pivot: ICU vs General counts per risk status
pivot_df = (
    risk_category_df
    .groupBy("risk_status")
    .pivot("admission_type", ["ICU", "General"])
    .agg(count("*"))
    .fillna(0)
)
pivot_df.show()

#6) Top-3 oldest patients in each risk category
w_top_age = Window.partitionBy("risk_status").orderBy(col("age").desc())
top3_oldest = (
    risk_category_df
    .withColumn("rnk", dense_rank().over(w_top_age))
    .filter(col("rnk") <= 3)
    .drop("rnk")
)
top3_oldest.show()

#7) Outlier flag on readmission_interval per admission_type (IQR)
iqr = (
    risk_category_df.groupBy("admission_type")
      .agg(
          expr("percentile_approx(readmission_interval, 0.25, 100)").alias("q1"),
          expr("percentile_approx(readmission_interval, 0.75, 100)").alias("q3")
      )
      .withColumn("iqr", col("q3") - col("q1"))
)
with_iqr = risk_category_df.join(iqr, "admission_type")
outliers_df = with_iqr.withColumn(
    "is_outlier",
    (col("readmission_interval") > col("q3") + 1.5*col("iqr")) |
    (col("readmission_interval") < col("q1") - 1.5*col("iqr"))
)
outliers_df.select("patient_id","admission_type","readmission_interval","is_outlier").show()

#8) Risk mix share (percent of patients per category)
total_patients = risk_category_df.count()
risk_share = (
    risk_category_df.groupBy("risk_status")
    .agg((count("*") / lit(total_patients)).alias("share"))
)
risk_share.show()

#9) Weighted risk score (example feature engineering)
# Example: higher age and lower interval => higher score; ICU admissions add weight
scored_df = (
    risk_category_df
    .withColumn("risk_score",
        0.5*col("age") +
        1.0*(50 - col("readmission_interval")) +   # lower interval increases score
        5.0*col("icu_admissions") +
        when(col("admission_type") == "ICU", 10).otherwise(0)
    )
)
scored_df.select("patient_id","risk_status","risk_score").show()

#10) Percentile bands of readmission_interval per admission_type
pcts = (
    risk_category_df.groupBy("admission_type")
    .agg(expr("percentile_approx(readmission_interval, array(0.25,0.5,0.75), 100)").alias("q"))
    .select("admission_type", col("q")[0].alias("p25"), col("q")[1].alias("p50"), col("q")[2].alias("p75"))
)
banded = (
    risk_category_df.join(pcts, "admission_type")
    .withColumn(
        "interval_band",
        when(col("readmission_interval") <= col("p25"), "0–25th")
         .when(col("readmission_interval") <= col("p50"), "25–50th")
         .when(col("readmission_interval") <= col("p75"), "50–75th")
         .otherwise("75–100th")
    )
)
banded.groupBy("admission_type","interval_band").count().orderBy("admission_type","interval_band").show()

#11) Deduplicate by patient_id keeping the highest risk then highest ICU admissions
# Priority: High Readmission Risk > Moderate > Low, then icu_admissions desc
priority = create_map(
    [lit("High Readmission Risk"), lit(3),
     lit("Moderate Risk"), lit(2),
     lit("Low Risk"), lit(1)]
)
w_dedup = Window.partitionBy("patient_id").orderBy(priority[col("risk_status")].desc(), col("icu_admissions").desc())
dedup = (
    risk_category_df
    .withColumn("rn", row_number().over(w_dedup))
    .filter(col("rn") == 1)
    .drop("rn")
)
dedup.show()

#12) Cohort-style summary: mean metrics by (admission_type, risk_status)
cohort_summary = (
    risk_category_df
    .groupBy("admission_type","risk_status")
    .agg(
        count("*").alias("patients"),
        avg("age").alias("avg_age"),
        avg("icu_admissions").alias("avg_icu_adm"),
        avg("readmission_interval").alias("avg_interval")
    )
    .orderBy("admission_type","risk_status")
)
cohort_summary.show()

#13) Rank patients by risk_score within each admission_type
w_type = Window.partitionBy("admission_type").orderBy(col("risk_score").desc())
ranked = scored_df.withColumn("rank_in_type", dense_rank().over(w_type))
ranked.select("patient_id","admission_type","risk_score","rank_in_type").show()

#14) Crosstab (quick contingency) of risk vs admission_type
risk_category_df.crosstab("risk_status","admission_type").show()

#15) Label ordering for tidy sorting (risk_status priority)
order_map = create_map(
    [lit("Low Risk"), lit(1),
     lit("Moderate Risk"), lit(2),
     lit("High Readmission Risk"), lit(3)]
)
ordered_view = risk_category_df.orderBy(order_map[col("risk_status")], col("age").desc())
ordered_view.show()