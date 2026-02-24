# Databricks notebook source
# MAGIC %md
# MAGIC # Olist E‑commerce Project (PySpark) — Trainer Notebook
# MAGIC *Goal:* End‑to‑end mini‑project using the Brazilian E‑Commerce Public Dataset by Olist.
# MAGIC *Layers:* Bronze → Silver → Gold (Delta)
# MAGIC *Skills:* Joins, aggregations, windows, dates, data quality, dimensional modeling, simple marts.
# MAGIC
# MAGIC *How to use:* Upload the 9 Kaggle CSVs to DBFS (or mounted cloud storage). Set the base input/output paths below and run the cells in order.
# MAGIC
# MAGIC *Tables used:* orders, order_items, customers, payments, reviews, products, sellers, geolocation, product_category_name_translation.
# MAGIC
# MAGIC > Tip for trainers: run on a small cluster with Photon; point out performance differences when using Delta.
# COMMAND ----------
import dbutils
import spark

# MAGIC %md
# MAGIC ## 0) Parameters & Imports

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("raw_base", "/mnt/olist-raw", "Raw CSV base path")
dbutils.widgets.text("bronze_base", "/mnt/olist-bronze", "Bronze Delta base path")
dbutils.widgets.text("silver_base", "/mnt/olist-silver", "Silver Delta base path")
dbutils.widgets.text("gold_base", "/mnt/olist-gold", "Gold Delta base path")

raw_base = dbutils.widgets.get("raw_base")
bronze_base = dbutils.widgets.get("bronze_base")
silver_base = dbutils.widgets.get("silver_base")
gold_base = dbutils.widgets.get("gold_base")

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark.sql("SET spark.databricks.delta.optimizeWrite.enabled=true")
spark.sql("SET spark.databricks.delta.autoCompact.enabled=true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Bronze Ingestion — CSV → Delta (raw shape, light typing only)

# COMMAND ----------

def read_csv(name):
    return (spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(f"{raw_base}/{name}.csv"))

orders       = read_csv("olist_orders_dataset")
order_items  = read_csv("olist_order_items_dataset")
customers    = read_csv("olist_customers_dataset")
payments     = read_csv("olist_order_payments_dataset")
reviews      = read_csv("olist_order_reviews_dataset")
products     = read_csv("olist_products_dataset")
sellers      = read_csv("olist_sellers_dataset")
geos         = read_csv("olist_geolocation_dataset")
cat_tr       = read_csv("product_category_name_translation")

(orders.write.format("delta").mode("overwrite").save(f"{bronze_base}/orders"))
(order_items.write.format("delta").mode("overwrite").save(f"{bronze_base}/order_items"))
(customers.write.format("delta").mode("overwrite").save(f"{bronze_base}/customers"))
(payments.write.format("delta").mode("overwrite").save(f"{bronze_base}/payments"))
(reviews.write.format("delta").mode("overwrite").save(f"{bronze_base}/reviews"))
(products.write.format("delta").mode("overwrite").save(f"{bronze_base}/products"))
(sellers.write.format("delta").mode("overwrite").save(f"{bronze_base}/sellers"))
(geos.write.format("delta").mode("overwrite").save(f"{bronze_base}/geos"))
(cat_tr.write.format("delta").mode("overwrite").save(f"{bronze_base}/cat_tr"))

print("Bronze written to:", bronze_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Silver — Clean & Conform (types, dates, light QA)

# COMMAND ----------

orders_s = (spark.read.format("delta").load(f"{bronze_base}/orders")
  .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp"))
  .withColumn("order_approved_at", to_timestamp("order_approved_at"))
  .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
  .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
  .withColumn("order_estimated_delivery_date", to_date("order_estimated_delivery_date"))
)

order_items_s = (spark.read.format("delta").load(f"{bronze_base}/order_items")
  .withColumn("price", col("price").cast("double"))
  .withColumn("freight_value", col("freight_value").cast("double"))
)

payments_s = (spark.read.format("delta").load(f"{bronze_base}/payments")
  .withColumn("payment_sequential", col("payment_sequential").cast("int"))
  .withColumn("payment_installments", col("payment_installments").cast("int"))
  .withColumn("payment_value", col("payment_value").cast("double"))
)

reviews_s = (spark.read.format("delta").load(f"{bronze_base}/reviews")
  .withColumn("review_score", col("review_score").cast("int"))
  .withColumn("review_creation_date", to_timestamp("review_creation_date"))
  .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))
)

products_s = spark.read.format("delta").load(f"{bronze_base}/products")
customers_s = spark.read.format("delta").load(f"{bronze_base}/customers")
sellers_s   = spark.read.format("delta").load(f"{bronze_base}/sellers")
cat_tr_s    = spark.read.format("delta").load(f"{bronze_base}/cat_tr")

# Geo centroids
geo = spark.read.format("delta").load(f"{bronze_base}/geos")
geo_sel = (geo
  .select(col("geolocation_zip_code_prefix").alias("zip_prefix"),
          col("geolocation_city").alias("city"),
          col("geolocation_state").alias("state"),
          col("geolocation_lat").cast("double").alias("lat"),
          col("geolocation_lng").cast("double").alias("lon"))
)
geo_centroid = (geo_sel.groupBy("zip_prefix","city","state")
  .agg(avg("lat").alias("lat"), avg("lon").alias("lon"))
)
geo_centroid.write.format("delta").mode("overwrite").save(f"{silver_base}/geo_centroid")

# Dimensions
dim_customer = (customers_s
  .select("customer_id", col("customer_unique_id").alias("customer_uid"),
          col("customer_zip_code_prefix").alias("zip_prefix"),
          "customer_city","customer_state")
)
dim_seller = (sellers_s
  .select("seller_id", col("seller_zip_code_prefix").alias("zip_prefix"),
          "seller_city","seller_state")
)
dim_product = (products_s.alias("p")
  .join(cat_tr_s.alias("t"), col("p.product_category_name")==col("t.product_category_name"), "left")
  .select(col("p.product_id"),
          col("p.product_category_name").alias("category_pt"),
          col("t.product_category_name_english").alias("category_en"),
          "product_name_lenght","product_description_lenght","product_photos_qty",
          "product_weight_g","product_length_cm","product_height_cm","product_width_cm")
)
dim_date = (orders_s
  .select(to_date("order_purchase_timestamp").alias("date"))
  .where(col("date").isNotNull())
  .distinct()
  .withColumn("year", year("date")).withColumn("quarter", quarter("date"))
  .withColumn("month", month("date")).withColumn("day", dayofmonth("date"))
  .withColumn("dow", date_format(col("date"), "E"))
)

for name, df in {
  "dim_customer": dim_customer,
  "dim_seller": dim_seller,
  "dim_product": dim_product,
  "dim_date": dim_date
}.items():
    df.write.format("delta").mode("overwrite").save(f"{silver_base}/{name}")

# Facts
ord_hdr = orders_s.select("order_id","customer_id","order_status",
                          "order_purchase_timestamp","order_approved_at",
                          "order_delivered_customer_date","order_estimated_delivery_date")

foi = (order_items_s.alias("i")
  .join(ord_hdr.alias("o"), "order_id")
  .select("order_id", "customer_id", "o.order_status", "o.order_purchase_timestamp",
          "product_id", "seller_id", "shipping_limit_date", "price", "freight_value")
)

rev_by_order = (reviews_s.groupBy("order_id")
  .agg(avg("review_score").alias("avg_review_score"),
       max("review_score").alias("max_review_score"))
)

fact_order_items = foi.join(rev_by_order, "order_id", "left")
fact_payments = payments_s

fact_order_items.write.format("delta").mode("overwrite").save(f"{silver_base}/fact_order_items")
fact_payments.write.format("delta").mode("overwrite").save(f"{silver_base}/fact_payments")

print("Silver written to:", silver_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Gold — Business Marts

# COMMAND ----------

# Sales mart (daily)
foi = spark.read.format("delta").load(f"{silver_base}/fact_order_items")
sales_mart = (foi.withColumn("order_date", to_date("order_purchase_timestamp"))
  .groupBy("order_date")
  .agg(
      sum(col("price")+col("freight_value")).alias("gmv"),
      countDistinct("order_id").alias("orders"),
      count("product_id").alias("items")
  ).withColumn("aov", col("gmv")/col("orders"))
)
sales_mart.write.format("delta").mode("overwrite").save(f"{gold_base}/sales_mart_daily")

# Category performance
dp = spark.read.format("delta").load(f"{silver_base}/dim_product")
cat_perf = (foi.alias("f").join(dp.alias("p"), "product_id")
  .groupBy("category_en")
  .agg(countDistinct("order_id").alias("orders"),
       sum("price").alias("sales"),
       avg("avg_review_score").alias("avg_review"))
  .orderBy(col("sales").desc())
)
cat_perf.write.format("delta").mode("overwrite").save(f"{gold_base}/category_performance")

# RFM
last_date = foi.agg(max(to_date("order_purchase_timestamp"))).first()[0]
rfm = (foi.withColumn("order_date", to_date("order_purchase_timestamp"))
  .groupBy("customer_id")
  .agg(
    datediff(lit(last_date), max("order_date")).alias("recency"),
    countDistinct("order_id").alias("frequency"),
    sum(col("price")+col("freight_value")).alias("monetary")
  )
)
def quintile(df, colname, asc=True):
    q = df.approxQuantile(colname, [0.2,0.4,0.6,0.8], 0.01)
    b1,b2,b3,b4 = q
    if asc:
        return when(col(colname)<=b1,1).when(col(colname)<=b2,2).when(col(colname)<=b3,3).when(col(colname)<=b4,4).otherwise(5)
    else:
        return when(col(colname)<=b1,5).when(col(colname)<=b2,4).when(col(colname)<=b3,3).when(col(colname)<=b4,2).otherwise(1)

rfm = (rfm
  .withColumn("r_score", quintile(rfm,"recency", asc=False))
  .withColumn("f_score", quintile(rfm,"frequency", asc=True))
  .withColumn("m_score", quintile(rfm,"monetary", asc=True))
  .withColumn("rfm_segment", concat_ws("-", col("r_score"),col("f_score"),col("m_score")))
)
rfm.write.format("delta").mode("overwrite").save(f"{gold_base}/rfm")

print("Gold written to:", gold_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Analysis — Ready-to-run SQL Snippets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly GMV / Orders / AOV
# MAGIC WITH daily AS (
# MAGIC   SELECT to_date(order_purchase_timestamp) as order_date,
# MAGIC          SUM(price + freight_value) as gmv,
# MAGIC          COUNT(DISTINCT order_id) as orders
# MAGIC   FROM delta.${silver_base}/fact_order_items
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC SELECT date_trunc('month', order_date) AS month,
# MAGIC        SUM(gmv) AS gmv,
# MAGIC        SUM(orders) AS orders,
# MAGIC        SUM(gmv)/SUM(orders) AS aov
# MAGIC FROM daily GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 categories by sales and review
# MAGIC SELECT p.category_en, SUM(f.price) AS sales, AVG(f.avg_review_score) AS avg_review
# MAGIC FROM delta.${silver_base}/fact_order_items f
# MAGIC JOIN delta.${silver_base}/dim_product p USING(product_id)
# MAGIC GROUP BY 1 ORDER BY sales DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Late delivery rate by customer state
# MAGIC WITH base AS (
# MAGIC   SELECT f.order_id, c.customer_state,
# MAGIC     DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS days_late
# MAGIC   FROM delta.${silver_base}/fact_order_items f
# MAGIC   JOIN delta.${silver_base}/dim_customer c USING(customer_id)
# MAGIC   JOIN delta.${bronze_base}/orders o USING(order_id)
# MAGIC )
# MAGIC SELECT customer_state, AVG(CASE WHEN days_late>0 THEN 1 ELSE 0 END) AS late_rate
# MAGIC FROM base GROUP BY 1 ORDER BY late_rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Payment mix & average installments by month
# MAGIC SELECT date_trunc('month', to_timestamp(o.order_purchase_timestamp)) AS month,
# MAGIC        p.payment_type,
# MAGIC        SUM(p.payment_value) AS amount,
# MAGIC        AVG(p.payment_installments) AS avg_inst
# MAGIC FROM delta.${bronze_base}/orders o
# MAGIC JOIN delta.${silver_base}/fact_payments p USING(order_id)
# MAGIC GROUP BY 1,2 ORDER BY 1,3 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RFM segments distribution
# MAGIC SELECT rfm_segment, COUNT(*) AS customers
# MAGIC FROM delta.${gold_base}/rfm GROUP BY 1 ORDER BY customers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) (Optional) Geo Distance vs Freight Sanity Check

# COMMAND ----------

geo_centroid = spark.read.format("delta").load(f"{silver_base}/geo_centroid")
dim_customer = spark.read.format("delta").load(f"{silver_base}/dim_customer")
dim_seller   = spark.read.format("delta").load(f"{silver_base}/dim_seller")

cust = (dim_customer.alias("c")
  .join(geo_centroid.alias("gc"), col("c.zip_prefix")==col("gc.zip_prefix"), "left")
  .select(col("c.customer_id"), col("gc.lat").alias("clat"), col("gc.lon").alias("clon"))
)
sell = (dim_seller.alias("s")
  .join(geo_centroid.alias("gs"), col("s.zip_prefix")==col("gs.zip_prefix"), "left")
  .select(col("s.seller_id"), col("gs.lat").alias("slat"), col("gs.lon").alias("slon"))
)

foi_geo = (spark.read.format("delta").load(f"{silver_base}/fact_order_items").alias("f")
  .join(cust, "customer_id", "left")
  .join(sell, "seller_id", "left")
)

from pyspark.sql.functions import radians, asin, sqrt, sin, cos, pow
haversine = 6371 * 2 * asin(
    sqrt(
      pow(sin((radians(col("slat")) - radians(col("clat"))) / 2), 2) +
      cos(radians(col("clat"))) * cos(radians(col("slat"))) *
      pow(sin((radians(col("slon")) - radians(col("clon"))) / 2), 2)
    )
)
dist_anal = foi_geo.withColumn("km", haversine)
dist_anal.createOrReplaceTempView("dist_anal")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Avg freight by distance bucket
# MAGIC SELECT width_bucket(km, 0, 2500, 10) AS km_bucket,
# MAGIC        AVG(freight_value) AS avg_freight,
# MAGIC        COUNT(*) AS lines
# MAGIC FROM dist_anal GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Data Quality (DQ) — quick checks

# COMMAND ----------

# Not‑nulls & negatives
neg_counts = (spark.read.format("delta").load(f"{bronze_base}/order_items")
  .selectExpr("SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS neg_price",
              "SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END) AS neg_freight")
).first().asDict()
print("Negatives:", neg_counts)

# Referential integrity: each order_item.order_id exists in orders
missing_orders = (spark.read.format("delta").load(f"{bronze_base}/order_items").alias("i")
  .join(spark.read.format("delta").load(f"{bronze_base}/orders").alias("o"),
        "order_id","left_anti")
).count()
print("order_items without matching order header:", missing_orders)

# Timeline sanity: delivered >= approved >= purchase
timelines_bad = (orders_s
  .where((col("order_delivered_customer_date") < col("order_approved_at")) |
         (col("order_approved_at") < col("order_purchase_timestamp")))
).count()
print("Timeline violations:", timelines_bad)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Homework / Interview‑style Prompts
# MAGIC 1. Compute customer LTV (assume gross margin 20%) and rank deciles.
# MAGIC 2. Find delivery bottlenecks by state and category jointly.
# MAGIC 3. Identify products with high return proxies (low review + high late rate).
# MAGIC 4. Build a seller score = 0.5*review + 0.3*on‑time + 0.2*repeat‑buyer share.
# MAGIC 5. Create a monthly snapshot table for active customers and churn flag.