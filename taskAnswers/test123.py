from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\10000Records.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#df.show()
#https://docs.python.org/3/library/re.html
df=df.withColumn("SSN",regexp_replace(col("SSN"),"-",""))
cls=[re.sub('[^0-9a-zA-Z]','',n) for n in df.columns]
#['EmpID', 'NamePrefix', 'FirstName', 'MiddleInitial', 'LastName', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName', 'Password']
df=df.toDF(*cls)
#df=df.toDF('EmpID', 'NamePrefix', 'FirstName', 'MiddleInitial', 'LastName', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName','passw')
#df=df.toDF(*['EmpID', 'NamePrefix', 'FirstName', 'MiddleInitial', 'LastName', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName','passw'])
df.show(5,False)

#df.show() by default show() displaying top 20 rows and first 20 chars only.
# let eg:hal.farrow@cox.net (18 chars), but del.fernandez@hotmail.com (25) characters u have so first 20 showing remaining show ...
#i want to overwrite ... display 5 lines only and all chars i want to show



#dataframe.toDF(*columns_list)...
#toDF() its used only 2 purpose 1) rename all columns (How many input columns u have sasme num of output columns u have) 2) rdd convert to dataframe
