from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data1 = r"C:\Users\ADMIN\Downloads\nep.csv"
df1 = spark.read.format("csv").option("header", "true").load(data1)
df1.show()
data2 = r"C:\Users\ADMIN\Downloads\asl.csv"
df2 = spark.read.format("csv").option("header", "true").load(data2)
df2.show()
#How do you merge two DataFrames with different schemas?
#here i want to get only common columns info i want at that time use this code
common_columns = list(set(df1.columns) & set(df2.columns))
#df1 = df1.select(common_columns)
#df2 = df2.select(common_columns)

#merged_df = df1.unionByName(df2, allowMissingColumns=True)
#Use unionByName, i want all columns use this code ignore above 4 lines
#with Allow Missing Columns:
merged_df = df1.unionByName(df2, allowMissingColumns=True)

merged_df.show()
