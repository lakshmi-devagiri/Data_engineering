from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\us-500.csv"
df = spark.read.format("csv").option("header", "true").option("mode","DROPMALFORMED").option("inferSchema","true").load(data)
#df.show()
res=df.withColumn("age",lit(90))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-",""))\
    .withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name"),lit(" "),col("state")))\
    .withColumn("fname", concat_ws("_",col("first_name"),col("last_name"),col("state")))\
    .withColumn("rnd",rand()*100).withColumn("ceil",ceil(col("rnd")))\
    .withColumn("flr",floor(col("rnd"))).withColumn("rou",round(col("rnd")))\
    .withColumn("mono",monotonically_increasing_id()+1).where(col("mono").between(20,30))\
    .drop("mono","email","web","phone2")

#monotonically_increasing_id ... u ll get unique numbers in seq manner from 0 ....if u want start from 1
#drop its ignore specified columns.

print(df.columns)
from pyspark.sql.types import *
#apply a logic on top of all columns or apply on only string columns
string_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
new_columns = [upper(col(c)).alias(c) if c in string_columns else col(c) for c in df.columns]
res1=df.select(*new_columns)
#With the provided sample DataFrame, all columns are of StringType. But if you had columns of other data types, the code would ensure that upper is applied only to string columns.

#res=df.select(*(upper(col(c)).alias(c) for c in df.columns if isinstance(col(c).dataType,StringType())))

#ceil: after something number any float value u ll get next number. let eg: 63.02204 .. u ll get 64
#floor: after number any float value , it ll ignore that decimal value. let eg: 63.022.. ignore 0.22 u ll get 63 only
#round: if float value is more than 0.5 ull get next value if below 0.5 u ll get previous value let eg: 63.22 u ll get 63 ..... if 63.56 u ll get 64

#res=df.groupBy("state").agg(countDistinct(col("first_name")),count(col("first_name")).alias("cnt"),collect_list(col("first_name")).alias("allnames"),collect_set(col("first_name")))
#res=df.groupBy("state").agg(collect_list(col("first_name")).alias("allnames")).withColumn("allnames",explode(col("allnames")))
'''
u have list of array values like this
AZ   |[Mattie, Mattie, Arminda, Herminia, Christiane, Helene, Regenia, Keneth, Elke, Iluminada]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|SC   |[Sabra, Eun, Jade] 
now i want to remove this list of elements use explode
|state|allnames                  |
+-----+--------------------------+
|AZ   |Mattie                    |
|AZ   |Mattie                    |
|AZ   |Arminda                   |
|AZ   |Herminia                  |
|AZ   |Christiane                |
|AZ   |Helene                    |
|AZ   |Regenia                   |
|AZ   |Keneth                    |
|AZ   |Elke                      |
|AZ   |Iluminada                 |
|SC   |Sabra                     |
|SC   |Eun                       |
|SC   |Jade                      |
|LA   |James                     |
|LA   |Solange    
'''

#	regexp_replace(Column e, String pattern, String replacement)
res.show(100,truncate=False)
