import os
os.environ["JAVA_HOME"] = "D:\\bigdata\\Java\\jdk-17"
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data1 = r"D:\bigdata\drivers\attended_students.csv"
print("Java version:", spark._jvm.java.lang.System.getProperty("java.version"))

df1 = spark.read.format("csv").option("header", "true").load(data1)
df1.show(5)

data2 = r"D:\bigdata\drivers\registered_students.csv"
df2 = spark.read.format("csv").option("header", "true").load(data2)
df2.show(5)
jdf=df1.join(df2, df2.EmailAddress==df1.UserEmail).drop(df1.UserEmail)\
    .select(concat_ws(" ",col("FirstName")).alias("fullname"),col("phone"),col("EmailAddress"))
jdf.show()
print("result")
res=df2.join(df1, df1.UserEmail==df2.EmailAddress, how="leftanti")

# Select relevant columns for registered but not attended students
registered_but_not_attended_df = res.select(
    col("rno"),
    col("FirstName"),
    col("LastName"),
    col("EmailAddress"),
    col("phone")
)

# Display the results
registered_but_not_attended_df.show()
res.show(50,truncate=False)
