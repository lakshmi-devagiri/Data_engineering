from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ddata = r"D:\bigdata\drivers\donations_details.csv"
ddf = spark.read.format("csv").option("inferSchema","true").option("header", "true").load(ddata).withColumnRenamed("name","dname")
ddf.show()
data=r"D:\bigdata\drivers\donations.csv"
df = spark.read.format("csv").option("inferSchema","true").option("header", "true").load(data)
df.show()
#inner join
#jdf = idf.join(sdf, idf.state==sdf.location)
#jdf = df.join(ddf, col("name")==col("dname"),"inner")
#jdf = df.join(ddf, "name","inner").drop(ddf.name)

#if u not mention "inner" by default it ll tke inner join only.
#jdf = idf.join(sdf, idf["state"]==sdf["location"])
#jdf.show()

df1=df.groupBy("name").agg(sum(col("amount")).alias("amttotal"))
df1.show()
#inner join get what are the common records available?
#valid donors
ijdf = df1.join(ddf, col("name")==col("dname"),"inner").drop("dname")
ijdf.show()

#who donated but no contact details (paid by hand)
#ljdf = df1.join(ddf, col("name")==col("dname"),"left").drop("dname").where(col("phone").isNull()))
ljdf = df1.join(ddf, col("name")==col("dname"),"left_outer").drop("dname")
ljdf.show()

#few members not yet donated, pls call those people
# right side all records u ll get if left dataframe any mismatching records u ll get nulls.

#rjdf = df1.join(ddf, col("name")==col("dname"),"right").drop("name")
rjdf = df1.join(ddf, col("name")==col("dname"),"right_outer").drop("name").where(col("amttotal").isNull())
rjdf.show()

fjdf = df1.join(ddf, col("name")==col("dname"),"full_outer")
nadf=fjdf.na.drop()
#fjdf.show()
nadf.show()
subs = fjdf.subtract(nadf)
subs.show()

#cdf = df1.join(ddf, col("name")==col("dname"),"cross") not correct
cdf = df1.crossJoin(ddf)
cdf.show(int(cdf.count()),truncate=False)

lajdf = df1.join(ddf, col("name")==col("dname"),"left_anti")
lajdf.show()
from pyspark.storagelevel import *
res=df1.join(broadcast(ddf))
#df1 is very large dataframe, ddf is very small (max 8gb) i want to join without any shuffling use broadcast join

res.show()

#soft merge bucketing join

