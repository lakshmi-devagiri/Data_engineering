from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
'''data=r"D:\bigdata\drivers\exceldata.xlsx"
pdf = pd.read_excel(data)
df=spark.createDataFrame(pdf)
print(pdf)
df.show()'''

url="https://vincentarelbundock.github.io/Rdatasets/csv/AER/CreditCard.csv"
pdf = pd.read_csv(url)
print(pdf)
df = spark.createDataFrame(pdf)
df.show()
