from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\10000Records.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#df=df.withColumnRenamed("Emp ID","empid").withColumnRenamed("Name Prefix","prefix")
#allcols = ["EMPID","PREFIX","FIRSTNAME","MIDDLENAME"]
import re
#allcols= [re.sub('[^a-zA-Z0-9]','',x) for x in df.columns]
#df=df.toDF('Empid', 'NamePrefix', 'FirstName', 'MiddleInitial', 'LastName', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName', 'Password')
allcols = ['EmpID', 'NamePrefix', 'FirstName', 'MiddleInitial', 'LastName', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName', 'Password']
df=df.toDF(*allcols)
df=df.withColumn("NamePrefix", regexp_replace(col("NamePrefix"),"\.",""))\
    .withColumn("AgeinCompanyYears",col("AgeinCompanyYears").cast(IntegerType()))
df.show()
#regexp_replce is a functin, but cast is a method thats y ur calling like this col(something).method() but function function(columns) like that calling.


#withColumnRenamed(old, new) used to rename only one column.
#toDF() used only 2 puerpose 1) rename all columns 2) rdd convert to dataframe
#toDF() is case sensitive must use dataframe.toDF
#in toDF how many input elements u have (let eg: 30 columns input, ) same number of columns u must rename (30 cols) at that time


