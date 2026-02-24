from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data=pd.read_excel(r"D:\bigdata\drivers\PlotAllottedFinalList.xls",dtype=str, keep_default_na=False)
#data=pd.read_csv("https://vincentarelbundock.github.io/Rdatasets/csv/AER/Affairs.csv")
print(data)

header=["S.No","OptionID","AllotmentID","FarmerName","PlotCategory","Quantity","AllocatedPlot","Schedule"]
#df = df.filter(~col("_c0").isin(df.rdd.first()[0]))
df=spark.createDataFrame(data,header).withColumn("id",monotonically_increasing_id()).filter(col("id")>=1).drop("id")
res=df.groupBy("FarmerName").agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
res.show(truncate=False)