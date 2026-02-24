from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\us-500.csv"
df = spark.read.format("csv").option("header", "true").option("mode","DROPMALFORMED").option("inferSchema","true").load(data)
df.show()
string_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
integer_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]

# Apply transformations based on column type
new_columns = []
for c in df.columns:
    if c in string_columns:
        new_columns.append(upper(col(c)).alias(c))
    elif c in integer_columns:
        new_columns.append((col(c) * col(c)).alias(c))  # Squaring the integer value
    else:
        new_columns.append(col(c))

res=df.select(*new_columns)
res.show()