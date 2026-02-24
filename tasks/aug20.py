from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#data=r"D:\bigdata\drivers\empmysql.csv"
#data=r"D:\bigdata\drivers\marks_rank.csv"
data=r"D:\bigdata\drivers\election_results.csv"
spark.conf.set("spark.sql.session.timeZone", "IST")
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
win=Window.orderBy(col("Total Votes").desc())
ndf=(df.withColumn("lead",lead("Total Votes").over(win))
     .withColumn("lag", lag("Total Votes").over(win))
     .withColumn("fst", first(col("Total Votes")).over(win))
     .withColumn("diff", col("fst")-col("Total Votes")).na.fill(0)
     .withColumn("rnk",rank().over(win))
     .withColumn("result", when(col("rnk")==1,"winner").when(col("rnk")==2,"runner").otherwise("lost"))

     )
ndf.show()