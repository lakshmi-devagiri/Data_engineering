from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"D:\bigdata\drivers\asl.csv"
df=spark.read.format("csv").option("header","true").load(data)

df1= df.withColumn("age_group_base", floor(col("age") / 10) * 10)
# Create the age group range as a string
df2 = df1.withColumn(
    "age_group",
    concat(col("age_group_base"), lit("-"), (col("age_group_base") + 10))
)
# Group by the age group range and count
res = df2.groupBy("age_group").count().orderBy("age_group")
res.show()
print("try in spark sql")
df.createOrReplaceTempView("asl")
qry="""
WITH base AS (
  SELECT
    CAST(age AS INT) AS age
  FROM asl
  WHERE age IS NOT NULL AND age RLIKE '^[0-9]+$'   -- keep numeric ages only
),
binned AS (
  SELECT FLOOR(age/10)*10 AS age_group_base
  FROM base
)
SELECT
  CONCAT(CAST(age_group_base AS STRING), '-', CAST(age_group_base + 10 AS STRING)) AS age_group,
  COUNT(*) AS count
FROM binned
GROUP BY age_group_base
ORDER BY age_group_base"""
result=spark.sql(qry)
result.show()