from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
pdata = r"D:\bigdata\drivers\products1.csv"
print("product dataframe")
products_df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(pdata)
products_df.show()
return_df=products_df.withColumn("return_score",when((col("return_count")>100) & (col("satisfaction_score")<50),"High Return Rate")\
                                 .when(((col("return_count")>50) & (col("return_count")<100)) & ((col("satisfaction_score")>50) & (col("satisfaction_score")<70)),"Moderate Return Rat")\
                                 .otherwise("Low Return Rate")\
                                 )
return_df.show()

return_count_df=return_df.groupBy("return_score").agg(count(col("return_score")).alias("return_count"))
return_count_df.show()
psdata = r"D:\bigdata\drivers\product_sales.csv"
product_sales_df= spark.read.format("csv").option("header", "true").option("inferSchema","true").load(psdata)

print("product sales data")
product_sales_df.show()
category_df=product_sales_df.select(col("product_name"),col("total_sales"),col("discount"),\
                                    when(((col("total_sales")>200000) & (col("discount")<10)),"Top Seller")\
                                    .when(col("total_sales").between(100000,200000),"Moderate Seller")\
                                    .otherwise("Low Seller")\
                                    .alias("category")
                                    )

category_df.show()

prod_count_df=category_df.groupBy(col("category")).agg(count(col("product_name")).alias("prod_count"))
prod_count_df.show()

max_sales_df=category_df.filter(col("category")=="Top Seller").agg(max(col("total_sales")).alias("max_sales"))
max_sales_df.show()

min_dis_df=category_df.filter(col("category")=="Moderate Seller").agg(min(col("discount")).alias("min_discount"))
min_dis_df.show()

tot_sales_range_df=category_df.filter(col("category")=="Low Seller").filter((col("total_sales")<50000) & (col("discount")>15).alias("total_sales_range"))
tot_sales_range_df.show()

df = (
    products_df.alias("p")
    .join(product_sales_df.alias("s"), on="product_name", how="inner")
)

# 3.a Plain Python functions (pure Python, testable without Spark)
def py_satisfaction_bucket(score: int | float | None) -> str:
    if score is None:
        return "Unknown"
    if score < 50:
        return "Low"
    if score < 70:
        return "Medium"
    return "High"

def py_risk_score(return_count: int | float | None,
                  satisfaction_score: int | float | None) -> float | None:
    """
    Composite risk: higher if returns are high and satisfaction is low.
    Scale to 0..100 (cap at 100).
    """
    if return_count is None or satisfaction_score is None:
        return None
    base = (return_count * 0.6) + ((100 - satisfaction_score) * 0.4)
    val = round(float(base), 2)
    return 100.0 if val > 100.0 else val

def py_action_reco(discount: int | float | None,
                   risk: float | None,
                   sat_bucket: str | None) -> str:
    """
    Simple decision rule to turn metrics into actions.
    """
    if risk is None:
        return "Review data quality"
    if risk >= 70 and sat_bucket == "Low":
        return "Investigate returns root-cause; improve quality/support"
    if risk >= 50 and (discount or 0) < 10:
        return "Test better promo/packaging; add post-purchase guidance"
    if sat_bucket == "High" and (discount or 0) >= 15:
        return "Trim discount and protect margin"
    return "Monitor"

# 3.b Wrap as UDFs with explicit return types
udf_satisfaction_bucket = udf(py_satisfaction_bucket, StringType())
udf_risk_score          = udf(py_risk_score,          DoubleType())
udf_action_reco         = udf(py_action_reco,         StringType())
spark.udf.register("satisfaction_bucket", py_satisfaction_bucket, StringType())
spark.udf.register("risk_score",          py_risk_score,          DoubleType())
spark.udf.register("action_reco",         py_action_reco,         StringType())

df.createOrReplaceTempView("product_insights")
spark.sql("""
  SELECT product_name,
         satisfaction_bucket(satisfaction_score) AS sat_bucket_sql,
         risk_score(return_count, satisfaction_score) AS risk_sql,
         action_reco(discount,
                     risk_score(return_count, satisfaction_score),
                     satisfaction_bucket(satisfaction_score)) AS action_sql
  FROM product_insights
""").show(truncate=False)

# 3.c Use them when building columns
df_feat = (
    df
    .withColumn("discount_pct", col("discount") / 100.0)
    .withColumn("gross_sales", col("total_sales").cast("double"))
    .withColumn("net_sales", round(col("gross_sales") * (1 - col("discount_pct")), 2))
    .withColumn("returns_per_100k", round(col("return_count") / (col("gross_sales")/100000.0), 2))
    .withColumn("satisfaction_bucket", udf_satisfaction_bucket(col("satisfaction_score")))
    .withColumn("risk_score", udf_risk_score(col("return_count"), col("satisfaction_score")))
    .withColumn("action", udf_action_reco(col("discount"), col("risk_score"), col("satisfaction_bucket")))
)

df_feat.select(
    "product_name","total_sales","discount","net_sales",
    "return_count","returns_per_100k",
    "satisfaction_score","satisfaction_bucket",
    "risk_score","action"
).show(truncate=False)

# -----------------------------
# 4) Feature engineering
# -----------------------------
df_feat = (
    df
    .withColumn("discount_pct", col("discount") / lit(100.0))
    .withColumn("gross_sales", col("total_sales").cast("double"))
    .withColumn("net_sales", round(col("gross_sales") * (1 - col("discount_pct")), 2))
    # Returns per ₹100k sales (proxy intensity so we can compare across price bands)
    .withColumn("returns_per_100k", round(col("return_count") / (col("gross_sales")/100000.0), 2))

)

# -----------------------------
# 5) Ranking / ordering
# -----------------------------
w = Window.orderBy(desc("risk_score"), desc("returns_per_100k"))
df_ranked = df_feat.withColumn("risk_rank", row_number().over(w))

df_ranked.select(
    "risk_rank","product_name","category",
    "total_sales","discount","net_sales",
    "return_count","returns_per_100k",
    "satisfaction_score","satisfaction_bucket",
    "risk_score","action"
).show(truncate=False)

# -----------------------------
# 6) Aggregations / rollups
# -----------------------------
# Category-level view (even if single category now)
df_cat = (
    df_feat.groupBy("category")
    .agg(
        sum("gross_sales").alias("sum_gross_sales"),
        sum("net_sales").alias("sum_net_sales"),
        sum("return_count").alias("sum_returns"),
        round(avg("satisfaction_score"), 2).alias("avg_satisfaction")
    )
)
df_cat.show(truncate=False)

# Top risky products (e.g., top 2)
df_ranked.orderBy(desc("risk_score")).limit(2).show(truncate=False)

# -----------------------------
# 7) Optional: quick correlations (numeric only)
# -----------------------------
# Are higher discounts linked to satisfaction here?
corr_discount_satisfaction = df_feat.stat.corr("discount", "satisfaction_score")
corr_returns_satisfaction  = df_feat.stat.corr("return_count", "satisfaction_score")
print("corr(discount, satisfaction) =", corr_discount_satisfaction)
print("corr(return_count, satisfaction) =", corr_returns_satisfaction)

# -----------------------------
# 8) Optional: SQL access
# -----------------------------
df_ranked.createOrReplaceTempView("product_insights")
spark.sql("""
SELECT product_name,
       net_sales,
       return_count,
       satisfaction_score,
       risk_score,
       action
FROM product_insights
ORDER BY risk_score DESC, returns_per_100k DESC
""").show(truncate=False)