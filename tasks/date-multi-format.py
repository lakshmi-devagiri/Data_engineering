from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\datemultiformat.csv"
df = (spark.read.format("csv").option("header", "true")
      .option("inferSchema","true").load(data))
#df=df.withColumn("hiredate", to_date(col("hiredate"),"d-M-yyyy"))
df.show()
possible_formats = ['dd-MM-yyyy', "dd-MMM-yyyy", "dd/MMM/yyyy","d-MMM-yyyy", "dd/MM/yyyy", "yyyy-MMM-d", "yyyy-MM-dd"]
def dateto(col, formats=possible_formats):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])

'''df=(df.withColumn("hiredate", dateto(col("hiredate")))
    .withColumn("hiredate", when(col("hiredate").isNull(),current_date()).otherwise(col("hiredate"))))'''
#df=df.withColumn("hiredate", dateto(col("hiredate"))).na.drop()
df=(df.withColumn("hiredate", dateto(col("hiredate"))).na.fill(0,["mgr"]).na.fill("nodata")
    .withColumn("hiredate", when(col("hiredate").isNull(),current_date()).otherwise(col("hiredate"))))
df.show()
#na.fill(0) .. if any column contains null values like 7654.. all those columns replace with 0
#na.fill("nodata") ... if any column contains string value like "venu" that is replace with somthign value

#.na.drop() ... it accept na.drop('all') or na.drop("any") default.... .. if anywhere u have null ignore that record.
#na.drop("all") ... if u have all records are null values at that time remove that record.


'''df = (spark.read.format("csv").option("header", "true")
      .option("inferSchema","true").load(data))
df=df.withColumn("dt", to_date(col("dt"),"d-M-yyyy")).where(year(col("dt"))>2023)
df.show()
df.printSchema()'''

'''
venu,11-3-2023,5000
if u have data like this by default spark consider as string.
i want data who is in 2024 after june its like a date.. i want to convert to date format..

'''