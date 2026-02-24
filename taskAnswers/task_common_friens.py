from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, array, collect_list

# Create a Spark session
spark = SparkSession.builder.appName("MutualFriends").getOrCreate()

# Sample data
data = [
    (1, [2, 3, 4]),
    (2, [1, 3, 4]),
    (3, [1, 2]),
    (4, [1, 2])
]

# Column names
columns = ["user_id", "friend_list"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Explode the friend_list
exploded_df = df.withColumn("friend_id", explode(col("friend_list")))

# Self join to find mutual friends
mutual_friends_df = exploded_df.alias("df1").join(
    exploded_df.alias("df2"),
    (col("df1.friend_id") == col("df2.friend_id")) & (col("df1.user_id") != col("df2.user_id"))
)


# Create user pairs and mutual friends
result_df = mutual_friends_df.select(
    array(col("df1.user_id"), col("df2.user_id")).alias("user_pair"),
    col("df1.friend_id").alias("mutual_friend")
)


final_df = result_df.groupBy("user_pair").agg(collect_list("mutual_friend").alias("mutual_friends"))


# Ensure user pairs are always in a consistent order
final_df = final_df.withColumn(
    "user_pair",
    array_sort(col("user_pair"))
)

# Convert user_pair to list of user_ids and mutual_friends
formatted_df = final_df.select(
    col("user_pair").alias("user_ids"),
    col("mutual_friends")
).dropDuplicates(['user_ids'])

# Show the result
formatted_df.show(truncate=False)
