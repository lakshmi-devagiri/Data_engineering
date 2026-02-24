from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=r"D:\bigdata\drivers\product_info.csv"
df=spark.read.format("csv").option("header","true").load(data)
df = df.withColumn("transaction_date", col("transaction_date").cast("timestamp"))

holiday_data = [
    ("2023-01-01", "New Year's Day"),
    ("2023-12-25", "Christmas Day"),
    ("2023-07-04", "Independence Day"),
    ("2023-06-19", "Juneteenth"),
    ("2023-11-23", "Thanksgiving Day"),
    ("2023-05-01", "Pngal"),
    ("2023-02-20", "Presidents' Day"),
    ("2023-05-21","MayDay")
]

holiday_schema = ["holiday_date", "holiday_name"]
holiday_df = spark.createDataFrame(holiday_data, holiday_schema)

# Convert string columns to date
transaction_df = df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
holiday_df = holiday_df.withColumn("holiday_date", to_date(col("holiday_date"), "yyyy-MM-dd"))

# Join transactions with holidays on the transaction date
transactions_on_holidays = transaction_df.join(holiday_df, transaction_df.transaction_date == holiday_df.holiday_date, "inner")

# anti joins
transactions_on_holidays.select("transaction_id", "transaction_date", "holiday_name")
res = transaction_df.join(holiday_df, transaction_df.transaction_date == holiday_df.holiday_date, "leftanti")
res.show()