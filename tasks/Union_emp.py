from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

emp_1 = (spark.read
         .option("header","true")
         .schema(emp_schema)
         .csv("D:\\bigdata\\drivers\\emp_data.csv"))
emp_1.show()
emp_2 = (spark.read
         .option("header","true")
         .schema(emp_schema)
         .csv("D:\\bigdata\\drivers\\emp_data1.csv"))
emp_2.show()
#Exact common rows:
common_exact = emp_1.intersect(emp_2)
common_exact.show()

#Rows unique to A (not in B):
only_in_a = emp_1.subtract(emp_2)
only_in_a.show()

a_sig = emp_1.withColumn("sig", sha2(concat_ws("||", *emp_1.columns), 256))
b_sig = emp_2.withColumn("sig", sha2(concat_ws("||", *emp_2.columns), 256))
#Same employee_id but with different values (e.g., salary updated in emp_data1)
conflicts = (a_sig.alias("a")
               .join(b_sig.alias("b"), on="employee_id", how="inner")
               .where(col("a.sig") != col("b.sig"))
               .select("employee_id",
                       *[col(f"a.{c}").alias(f"a_{c}") for c in emp_1.columns if c!="employee_id"],
                       *[col(f"b.{c}").alias(f"b_{c}") for c in emp_1.columns if c!="employee_id"]))
conflicts.show(truncate=False)

#Removes duplicate rows across both DataFrames.
uni=emp_1.union(emp_2)
uni.show()
#Keeps duplicates.
emp_union_all=emp_1.unionAll(emp_2)
emp_union_all.show()
#7. Deduplication After UnionAll
emp_union_all.distinct().show()

emp_union_all.groupBy("department_id").avg("salary").show()
