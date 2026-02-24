from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\world_bank.json"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)

def flatten(df):
 from pyspark.sql.functions import col
 flat_cols = [c[0] for c in df.dtypes if not c[1].startswith('struct')]
 nested_cols = [c[0] for c in df.dtypes if c[1].startswith('struct')]
 flat_df = df.select(flat_cols + [col(n + '.' + f.name).alias(n + '_' + f.name)
 for n in nested_cols
 for f in df.select(n + '.*').schema.fields])
 return flat_df

while len([c[0] for c in df.dtypes if c[1].startswith('struct')]) > 0:
 df = flatten(df)

df.printSchema()