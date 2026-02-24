from pyspark.sql import *
from pyspark.sql.functions import *
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
import re

# Initialize Spark session
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

# Load data
data = r"D:\bigdata\drivers\donations.csv"
drdd = spark.sparkContext.textFile(data)

# Filter header and empty lines
filtered_rdd = drdd.filter(lambda x: "dt" not in x and len(x) > 1)

# Split and extract (key, value) pairs, where key = x[0], value = int(x[2])
pair_rdd = filtered_rdd.map(lambda x: x.split(",")) \
                       .map(lambda x: (x[0], (int(x[2]), 1)))  # (key, (sum, count))

# Reduce by key to aggregate sum and count
sum_count_rdd = pair_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Compute average per key
average_rdd = sum_count_rdd.mapValues(lambda x: x[0] / x[1])

# Optional: sort by average descending
sorted_avg_rdd = average_rdd.sortBy(lambda x: x[1], ascending=False)

# Output
for r in sorted_avg_rdd.collect():
    print(r)
