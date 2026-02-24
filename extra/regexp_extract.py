from pyspark.sql import *
from pyspark.sql.functions import *
# task : in this dataset i have messages like this. I want to extract date and name from message add in another column
#let eg: 1, HI, I am Kumar my dob is 21-05-2004.. i want id, message, name, date_of_birth... id|             message|      Date|Date_formate|Day_of_Week|
'''
+---+--------------------+-----------+--------------------+--------+
| id|             message|DateOfBirth|         Day_of_Week|    name|
+---+--------------------+-----------+--------------------+--------+
|  1|Hi, I am Donna, m...| 1974-04-16| 1974-Apr-16-Tuesday|   Donna|
'''

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\facebook-messages.csv"
df = spark.read.format("csv").option("header", "true").load(data)
#df.show()

#Extract the Date from the Senentece
pattern= r'(\d{2}-\d{2}-\d{4})'
df_with_date=df.withColumn( "Date",regexp_extract('message',pattern,0))
#df_with_date.display()


#convert Date in date_formate, from String to date
df_formated_date=df_with_date.withColumn('DateOfBirth',to_date('Date',"dd-MM-yyyy"))

#check schema of Date_formate column
df_formated_date.printSchema()
name_pattern = r"Hi, I am (\w+)"

#Extract day of Week from the Date_formate column
df_day_of_week=df_formated_date.withColumn('Day_of_Week',date_format(col("DateOfBirth"),"yyyy-MMM-dd-EEEE"))\
    .withColumn("name", regexp_extract(col("message"),name_pattern, 1)).drop("Date")
df_day_of_week.show()

#Concate the Day_of_Week and Sentence
#df1=df_day_of_week.withColumn('updated Sentences',concat_ws(' ','message','Day_of_Week')).drop('Senetence','Date','Date_formate','Day_of_Week')
#df1.show()

