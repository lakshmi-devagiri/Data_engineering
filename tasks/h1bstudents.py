from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
'''data = r"D:\bigdata\drivers\pivot-eg-data.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df.show()

res=df.groupBy("Name").pivot("city").agg(avg(col("profit")))
res.show()
#ndf=res.select("*").where(col("Austin").isNotNull())

ndf=res.selectExpr(
    "Name",
    "stack(5, 'Austin', Austin, 'Dallas', Dallas, 'njc', `New Jersey City`, 'NewYork', NewYork) as (City, Amount)"
)
ndf.show()'''

import pandas as pd
url="https://vincentarelbundock.github.io/Rdatasets/csv/AER/Affairs.csv"
c = pd.read_csv(url)
#print(c)
#pandas convert to spark dataframe
df=spark.createDataFrame(c)
#df.show()
res=df
res.show()
