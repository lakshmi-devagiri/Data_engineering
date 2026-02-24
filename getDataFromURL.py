from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"https://vincentarelbundock.github.io/Rdatasets/csv/AER/CollegeDistance.csv"
spark.conf.set("spark.sql.execution.arrow.enabled","true")

df = pd.read_csv(data)
print(df)
#ull get different datasets from https://vincentarelbundock.github.io/Rdatasets/datasets.html
sdf=spark.createDataFrame(df)
sdf.show()
#if u get this error AttributeError: 'DataFrame' object has no attribute 'iteritems'
#its pandas version problem must use pandas 1.5.3 version only not 2.x
