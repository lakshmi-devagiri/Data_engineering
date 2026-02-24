from pyspark.sql import *
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
base_path ="C:\\Users\\DELL\\Downloads\\archive\\"
#base_path=r"C:\Users\DELL\Downloads\archive\\"

df_customers = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_customers_dataset.csv")

df_geolocation = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_geolocation_dataset.csv")

df_order_items = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_order_items_dataset.csv")

df_order_payments = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_order_payments_dataset.csv")

df_order_reviews = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_order_reviews_dataset.csv")

df_orders = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_orders_dataset.csv")

df_products = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_products_dataset.csv")

df_sellers = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "olist_sellers_dataset.csv")

df_category_translation = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(base_path + "product_category_name_translation.csv")

df_order_payments.show(5)
df_orders.show(5)
jdf=df_order_payments.join(df_orders,"order_id","inner")
res=jdf.groupBy("customer_id").agg(sum(col("payment_value")).alias("total_spend")).orderBy(col("total_spend").desc())
res.show()