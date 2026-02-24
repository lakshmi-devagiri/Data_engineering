from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\unixTimestampData_namedobcity.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
res=(df.withColumn("uxts",unix_timestamp())
     .withColumn("dateofbirth_UTC", from_unixtime(col("dob")))
     .withColumn("dob_IST_time", to_utc_timestamp(from_utc_timestamp(col("dateofbirth_UTC"), "IST"), "UTC"))
     .orderBy(col("dob_IST_time").desc()))

'''win=Window.partitionBy("country").orderBy(col("dob_IST_time").desc())
ndf = res.withColumn("drank", dense_rank().over(win)).where(col("drank")<=3)'''
#ndf=res.where((col("country")=="UK") & (year(col("dateofbirth_UTC"))>=1996))
#ndf=res.where(((col("country")=="UK") | (col("country")=="USA") )& (year(col("dateofbirth_UTC"))>=1996))
#ndf=res.where((col("country").isin("UK","USA")) & (year(col("dateofbirth_UTC"))>=1996))
#ndf=res.where(~(col("country").isin("UK","USA")) & (year(col("dateofbirth_UTC"))>=1994))
#ndf=res.filter(year(col("dob_IST_time")).between(1995,1997))
ndf=res.withColumn("age",lit(88))
ndf.describe("dob").show() #perticularly one column i want to debug use it., but summary not possible
ndf.summary().show() # its same like describe() but only particular column not possible.
print("number of partitions: ")
print(ndf.rdd.getNumPartitions())
ndf=res.coalesce(1).withColumn("mono",monotonically_increasing_id())
#coalesce(1) ... if u have 32 partitions or many partitions ... minimize to few partitions means 1,3,12 use coalease
#colease(40) not working why? coelease only decrease partitions without shuffle (narrow transformation) but not possibel to increase
print("number of partitions: ")
print(ndf.rdd.getNumPartitions())
#monotonically_increasing_id() used to add numbers from 0 ... but problem is give numbers in seq based on partition
#if u have 32 partitions (java objects) at that time
'''
par1..1,2,3,4,5..
part2..4444,4445,4446
part3...77777,77778,77779
'''

ndf.show()
#res.printSchema()
#unix stamp means from 1970 jan 1 as per UTC time from that time onward how many seconds completed.
#unix_timestamp always point to as per UTC/London time .. but Alice Johnson he born in usa... but in this column (dateofbirth) u have london time not usa time.
#so dateofbirth convert to usa time.