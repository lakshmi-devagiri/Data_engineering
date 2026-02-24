from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data=r"D:\bigdata\drivers\world_bank.json"
df=spark.read.format("json").option("mode","DROPMALFORMED").load(data)
df=df.withColumn("theme_namecode", explode(col("theme_namecode")))\
    .withColumn("sector_namecode", explode(col("sector_namecode")))\
    .withColumn("sector", explode(col("sector")))\
    .withColumn("projectdocs", explode(col("projectdocs")))\
    .withColumn("mjtheme_namecode",explode(col("mjtheme_namecode")))\
    .withColumn("mjtheme",explode(col("mjtheme")))\
    .withColumn("mjsector_namecode",explode(col("mjsector_namecode")))\
    .withColumn("majorsector_percent",explode(col("majorsector_percent")))\
    .withColumn("theme_namecode_name",col("theme_namecode.name")).withColumn("theme_namecode_code", col("theme_namecode.code")).drop("theme_namecode")\
    .withColumn("theme1_Name",col("theme1.name")).withColumn("theme1percent",col("theme1.Percent")).drop("theme1")\
    .withColumn("sector_namecode_name",col("sector_namecode.name")).withColumn("sector_namecode_code",col("sector_namecode.code")).drop("sector_namecode") \
    .withColumn("sector4_Name", col("sector4.Name")).withColumn("sector4_Percent", col("sector4.Percent")).drop("sector4") \
    .withColumn("sector1_Name", col("sector1.Name")).withColumn("sector1_Percent", col("sector1.Percent")).drop("sector1") \
    .withColumn("sector2_Name", col("sector2.Name")).withColumn("sector2_Percent", col("sector2.Percent")).drop("sector2") \
    .withColumn("sector3_Name", col("sector3.Name")).withColumn("sector3_Percent", col("sector3.Percent")).drop("sector3") \
    .withColumn("sectorName", col("sector.name")).drop("sector") \
    .withColumn("projectdocsDoctype", col("projectdocs.DocDate")).drop("projectdocs")

df.printSchema()
df.show()

'''
theme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 
 after explode (explode remove arrays)
 
 theme_namecode: struct (nullable = true)
 |    |-- code: string (nullable = true)
 |    |-- name: string (nullable = true)
 
 
 to remove arrays use explode
 now struct ...if u want to make structure 
 col("parent_column.childColumn")
 
 '''