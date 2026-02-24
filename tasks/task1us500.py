from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"D:\bigdata\drivers\us-500.csv"
# Load us-500 dataset
#df_us = spark.read.csv(data, header=True, inferSchema=True)

df_us=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


df_us = df_us.withColumn("email", lower(col("email")))
df_us.createOrReplaceTempView("us")

# Task 1: Most Common Email Domain by State
## DataFrame API
df_domains = df_us.withColumn("domain", split("email", "@").getItem(1))
window_state = Window.partitionBy("state").orderBy(col("cnt").desc())
most_common_domain = df_domains.groupBy("state", "domain").count()\
    .withColumnRenamed("count", "cnt")\
    .withColumn("rank", row_number().over(window_state))\
    .filter(col("rank") == 1)
most_common_domain.show()
## Spark SQL
most_common_domain1=spark.sql("""
    SELECT * FROM (
        SELECT state, SPLIT(email, '@')[1] AS domain, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) as rank
        FROM us
        GROUP BY state, SPLIT(email, '@')[1]
    ) tmp
    WHERE rank = 1
""")
most_common_domain1.show()
# Task 2: Top 5 Cities by Business Count
## DataFrame API
top_cities = df_us.groupBy("city").agg(count("company_name").alias("cnt")).orderBy(col("cnt").desc()).limit(5)
top_cities.show()
## SQL
top_cities1=spark.sql("SELECT city, COUNT(company_name) AS cnt FROM us GROUP BY city ORDER BY cnt DESC LIMIT 5")
top_cities1.show()

# Task 3: Duplicate Detection by phone1 or email
## DataFrame API
dup_email = df_us.groupBy("email").count().filter("count > 1")
dup_phone = df_us.groupBy("phone1").count().filter("count > 1")
dup_phone.show()
dup_email.show()
## SQL
spark.sql("SELECT email, COUNT(*) FROM us GROUP BY email HAVING COUNT(*) > 1")
spark.sql("SELECT phone1, COUNT(*) FROM us GROUP BY phone1 HAVING COUNT(*) > 1")

# Deduplication: Keep latest by synthetic timestamp
from pyspark.sql.functions import current_timestamp
us_dedup = df_us.withColumn("ts", current_timestamp())
window_dedup = Window.partitionBy("email").orderBy(col("ts").desc())
us_latest = us_dedup.withColumn("rn", row_number().over(window_dedup)).filter(col("rn")== 1)
us_latest.show()
# Task 4: Find Missing or Null Values (email or zip)
## DataFrame API
missing_df = df_us.filter((col("email").isNull()) | (col("email") == "") | (col("zip").isNull()))
missing_df.show()
## SQL
missing_df1=spark.sql("SELECT * FROM us WHERE email IS NULL OR email = '' OR zip IS NULL")
missing_df1.show()
# Task 5: Count gmails, yahoos, others
## DataFrame API
mail_cat = df_us.withColumn("domain", split("email", "@").getItem(1))\
                .withColumn("mail_type", when(col("domain").like("%gmail.com"), "gmail")\
                                       .when(col("domain").like("%yahoo.com"), "yahoo")\
                                       .otherwise("other"))
mail_summary = mail_cat.groupBy("mail_type").count()
mail_summary.show()
## SQL
mail_summary1=spark.sql("""
    SELECT CASE 
             WHEN email LIKE '%@gmail.com' THEN 'gmail'
             WHEN email LIKE '%@yahoo.com' THEN 'yahoo'
             ELSE 'other' 
           END AS mail_type,
           COUNT(*) 
    FROM us 
    GROUP BY mail_type
""")
mail_summary1.show()