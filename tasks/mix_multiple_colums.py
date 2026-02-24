from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = [
    (1, "John",  "HR",      40, 42, 38),
    (2, "Jane",  "Finance", 39, 44, 41),
    (3, "Alice", "IT",      45, 40, 43),
]
df = spark.createDataFrame(data, ["empid","name","dept","hours_week1","hours_week2","hours_week3"])

# Unpivot with stack: (num_rows, label1, value1, label2, value2, ...)
week_cols   = ["hours_week1", "hours_week2", "hours_week3"]
week_labels = ["week1", "week2", "week3"]

out = (
    df.select("empid","name","dept",
              array(*[col(c) for c in week_cols]).alias("hours_arr"))
    .select("*", posexplode("hours_arr").alias("idx","hours"))
    .withColumn("week", element_at(array(*[lit(l) for l in week_labels]), col("idx")+1))
    .drop("hours_arr","idx","hours")
)

out.show()
