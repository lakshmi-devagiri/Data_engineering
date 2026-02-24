from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\world_bank.json"
df = spark.read.format("json").option("mode", "DROPMALFORMED").load(data)
#explode simply remove array from a column .. make each element in separate row.
df=(df.withColumn("theme_namecode", explode(col("theme_namecode"))).withColumn("theme_namecode_code", col("theme_namecode.code")).withColumn("theme_namecode_name",col("theme_namecode.name")).drop("theme_namecode")
    .withColumn("theme1Name", col("theme1.Name")).withColumn("theme1Percent", col("theme1.Percent")).drop("theme1")
    .withColumn("projectdocs", explode(col("projectdocs")))
    .withColumn("projectdocs_DocDate", col("projectdocs.DocDate"))
    .withColumn("projectdocs_DocType", col("projectdocs.DocType"))
    .withColumn("projectdocs_DocTypeDesc", col("projectdocs.DocTypeDesc"))
    .withColumn("projectdocs_DocURL", col("projectdocs.DocURL"))
    .withColumn("projectdocs_EntityID", col("projectdocs.EntityID")).drop("projectdocs")
    )
#df=df.select("theme_namecode","theme1","theme1Name","theme1Percent")
#df=df.select("projectdocs","projectdocs_DocDate","projectdocs_DocType","projectdocs_DocTypeDesc","projectdocs_DocURL","projectdocs_EntityID")
df.show(truncate=False)
df.printSchema()

'''
projectdocs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- DocDate: string (nullable = true)
 |    |    |-- DocType: string (nullable = true)
 |    |    |-- DocTypeDesc: string (nullable = true)
 |    |    |-- DocURL: string (nullable = true)
 |    |    |-- EntityID: string (nullable = true)
 after explode
 projectdocs: struct (nullable = true)
 |    |-- DocDate: string (nullable = true)
 |    |-- DocType: string (nullable = true)
 |    |-- DocTypeDesc: string (nullable = true)
 |    |-- DocURL: string (nullable = true)
 |    |-- EntityID: string (nullable = true)
 .withColumn("projectdocs_DocDate", col("projectdocs.DocDate"))
 .withColumn("projectdocs_DocType", col("projectdocs.DocType"))
 .withColumn("projectdocs_DocTypeDesc", col("projectdocs.DocTypeDesc"))
 .withColumn("projectdocs_DocURL", col("projectdocs.DocURL"))
 .withColumn("projectdocs_EntityID", col("projectdocs.EntityID"))
 
 so explode remove array , next struct .. parenent_col.child_column 
 
before explode array input data like [something ] like this.
-- theme_namecode: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 after explode
 |-- theme_namecode: struct (nullable = true)
 |    |-- code: string (nullable = true)
 |    |-- name: string (nullable = true)
 means explode remove array
 another complex json data type called Struct .. how to make structure ... parent_col.child_col 
 theme1: struct (nullable = true)
 |    |-- Name: string (nullable = true)
 |    |-- Percent: long (nullable = true)
 
 df.withColumn("theme1Name", col("theme1.Name")).withColumn("theme1Percent", col("theme1.Percent"))
 
 df.drop("theme1") .. here drop ignore/remove tht column from existing dataframe.
 
'''