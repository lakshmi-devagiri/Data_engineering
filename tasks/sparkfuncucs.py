from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r"D:\bigdata\drivers\us-500.csv"
df = spark.read.format("csv").option("mode","DROPMALFORMED").option("header", "true").option("inferSchema","true").load(data)
df=(df.withColumnRenamed("zip","sal")
    .withColumn("sal",col("sal").cast(IntegerType()))
#    .withColumn("sal",lpad(col("sal"),5,'0'))
    )
def testfunc(sal):
    if sal<25000:
        return "low sal"
    elif 24000 <= sal<50000:
        return "good salary"
    else:
        return "high sal"

#python function convert to spark understandable format (udf)
offerfunc=udf(testfunc)
df=df.withColumn("today_offer",offerfunc(col("sal")))

df.show(100)
#withColumn ... used to add another extra new column
#df.withColumnRenamed("old_col","new_col" used to rename existing column
#withColumnRenamed only rename one column at a time.
#by default sal is double value (70116.0) convert to 70116 use cast
