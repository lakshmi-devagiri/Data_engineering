from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\cricket_match_results.csv"
df = spark.read.format("csv").option("inferSchema","true").option("header", "true").load(data)
df.show()
#
df = df.withColumn(
    "team1_points",
    when(col("winner") == col("team1"), 2)
    .when(col("winner") == "draw", 1)
    .otherwise(0)
)
df = df.withColumn(
    "team2_points",
    when(col("winner") == col("team2"), 2)
    .when(col("winner") == "draw", 1)
    .otherwise(0)
)
# Show the result
df.show()
# Aggregate points for each team
team1_points = df.groupBy("team1").agg(sum(col("team1_points")).alias("points"))
team2_points = df.groupBy("team2").agg(sum(col("team2_points")).alias("points"))
# Rename columns for consistency
team1_points = team1_points.withColumnRenamed("team1", "team")
team2_points = team2_points.withColumnRenamed("team2", "team")
# Union the points and calculate total points for each team
total_points = team1_points.unionByName(team2_points).groupBy("team").agg(expr("sum(points) as total_points"))

# Show the result
total_points.show()