from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\customer_loyalty_data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()
classify_df=df.withColumn("classification",when((col("purchase_frequency")>20) & (col("average_spending")>500),"Highly Loyal")\
                                           .when(col("purchase_frequency").between(10,20),"Moderately Loyal")\
                                           .otherwise("Low Loyalty"))
classify_df.show()

cust_count_df=classify_df.groupBy(col("classification")).agg(count(col("customer_name")).alias("cust_count"))
cust_count_df.show()

avg_spend_df=classify_df.groupBy(col("classification")).agg(avg(col("average_spending")).alias("avg_spending"))\
    .filter(col("classification")=="Highly Loyal")
avg_spend_df.show()

min_spend_df=classify_df.groupBy(col("classification")).agg(min(col("average_spending")).alias("min_spending"))\
    .filter(col("classification")=="Moderately Loyal")
min_spend_df.show()

spending_range_df=classify_df.filter(col("classification")=="Low Loyalty").filter((col("average_spending")<100) & (col("purchase_frequency")<5))
spending_range_df.show()
# 2) Cohort retention (by cohort month) using tenure + recency
# Purpose: estimate active vs dormant by months_since_first.
cohort = (
    df
    .withColumn("cohort_month", date_format("first_purchase_date", "yyyy-MM"))
    .withColumn("months_since_first", (months_between(current_date(), col("first_purchase_date"))).cast("int"))
    .withColumn("active_flag", (col("recency_days") <= 60).cast("int"))
)
cohort_ret = (cohort.groupBy("cohort_month","months_since_first").agg(avg("active_flag").alias("retention_rate"))
                     .orderBy("cohort_month","months_since_first"))
cohort_ret.show(truncate=False)

# 3) Rolling 3-period spend velocity proxy
# Purpose: trend/growth of total_spend per tier (windowed average over sorted names for demo).
w_tier = Window.partitionBy("membership_tier").orderBy("customer_name").rowsBetween(-2, 0)
tier_vel = (df.withColumn("tier_avg_spend_roll3", avg("total_spend").over(w_tier)))
tier_vel.select("customer_name","membership_tier","total_spend","tier_avg_spend_roll3").show(truncate=False)

# 4) Churn-risk label (rule-based)
# Purpose: combine recency + decline proxies into risk label.
churn_label = (
    df.withColumn(
        "churn_risk",
        when((col("recency_days") > 90) & (col("purchase_frequency") <= 5), "High")
         .when((col("recency_days") > 60) & (col("discount_rate") < 0.10), "Medium")
         .otherwise("Low")
    )
)
churn_label.groupBy("churn_risk").count().show()
