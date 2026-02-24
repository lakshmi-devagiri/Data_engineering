from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data_sample = [("Banana",1000,"Ecuador"), ("Carrot",1500,"Ecuador"), ("rice",1600,"Ecuador"),
             ("Orange",2000,"Ecuador"), ("Orange",2000,"Ecuador"), ("Banana",400,"Portugal"),
             ("Carrot",1200,"Portugal"), ("rice",1500,"Portugal"), ("Orange",4000,"Portugal"),
             ("Banana",2000,"Argentina"), ("Carrot",2000,"Argentina"), ("rice",2000,"Mexico")]

colunas = ["product", "quantity", "country"]
df = spark.createDataFrame(data = data_sample, schema = colunas)
df.printSchema()
df.show(truncate=False)

# Groups data by product and pivots based on country, adding the quantities of each product by country
pivotDF = df.groupBy("product").pivot("country").sum("quantity")


# Print the pivoted DataFrame schema to show the new column structure
pivotDF.printSchema()

# Displays the pivoted DataFrame
pivotDF.show(truncate=False)

# Group data by product and country, add quantities, group again by product and pivot based on country
# This approach ensures that the sum is done before the pivot to avoid possible problems with multiple inputs
pivotDF = df.groupBy("product","country") \
      .sum("quantity") \
      .groupBy("product") \
      .pivot("country") \
      .sum("sum(quantity)")

# Print the schema of the resulting DataFrame to check the column structure after the pivot
pivotDF.printSchema()
pivotDF.show(truncate=False)

# Defines an expression to unpivot, transforming columns into rows
unpivotExpr = "stack(3, 'Argentina', Argentina, 'Portugal', Portugal, 'Mexico', Mexico) as (country,Total)"

# Selects the product column and applies the unpivot expression, excluding rows where the total is null
unPivotDF = pivotDF.select("product", expr(unpivotExpr)).where("Total is not null")

# Displays the DataFrame after the unpivot operation
unPivotDF.show(truncate=False)
