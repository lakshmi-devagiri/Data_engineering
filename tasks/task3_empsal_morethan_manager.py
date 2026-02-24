from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\emp_manager_sal.csv"
df = spark.read.format("csv").option("header", "true").option("mode","DROPMALFORMED").option("inferSchema","true").load(data)
#df.show()
df.createOrReplaceTempView("tab")
#1) employees who getting more salary than their manager salary i want their salary as well
print("1) employees who getting more salary than their manager salary i want their salary as well")
qry="""(SELECT e.empname AS employee_name,
       m.empname AS manager_name,
       e.sal AS employee_salary,
       m.sal AS manager_salary
FROM tab e
LEFT JOIN tab m ON e.managerid = m.empid
WHERE e.sal > m.sal)"""

#res=spark.sql(qry)
df_alias = df.alias("e")
df_manager = df.alias("m")

joined_df = df_alias.join(df_manager, col("e.managerid") == col("m.empid"), "left")

task1   = joined_df.select(
    col("e.empname").alias("employee_name"),
    col("m.empname").alias("manager_name"),
    col("e.sal").alias("employee_salary"),
    col("m.sal").alias("manager_salary")
).where(col("e.sal") > col("m.sal"))

# Show the result
task1.show()
print("  Find Employees Without a Manager (Top-level)")
task2=df.filter(df.managerid.isNull())
task2_sql=spark.sql("""
SELECT * FROM tab WHERE managerid IS NULL""")
task2.show()
task2_sql.show()

Task3="Add Bonus Column based on this... if employee number even , 10% bonus if odd number 5% bonus of their salary, if prime number add 15% of their salary"
def calculate_bonus(empid, sal):
    try:
        empid = int(empid)
        sal = float(sal)
    except:
        return 0.0

    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                return False
        return True

    if is_prime(empid):
        return sal * 0.15
    elif empid % 2 == 0:
        return sal * 0.10
    else:
        return sal * 0.05

from pyspark.sql.types import *
df1= df.withColumn("empid", col("empid").cast(IntegerType())) \
       .withColumn("sal", col("sal").cast(DoubleType()))
bonus_udf = udf(calculate_bonus, DoubleType())
spark.udf.register("bonus_calc", bonus_udf)


task3=df1.withColumn("bonus", bonus_udf(col("empid"), col("sal")))
print("task3 anwer: ",Task3)

# Run Spark SQL using the UDF
task3_sql = spark.sql("""
    SELECT *,
           bonus_calc(empid, sal) AS bonus
    FROM tab
""")

task3_sql.show()
task3.show()

t4=" task 4 Running Total of Salary Ordered by Employee ID"
#Definition: Running total is a cumulative sum of salaries as rows progress.
w = Window.orderBy("empid").rowsBetween(Window.unboundedPreceding, Window.currentRow)
task4=df.withColumn("running_total_sal", sum("sal").over(w))
print(t4)
task4_sql=spark.sql("""
SELECT *,
       SUM(sal) OVER (ORDER BY empid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_sal
FROM tab
""")
task4_sql.show()
task4.show()

Task5m="tell me list of manager who has more employees in dec order"
task5=df.groupBy("managerid").count() \
  .filter("managerid IS NOT NULL") \
  .orderBy("count", ascending=False)
print(Task5m)

task5_sql=spark.sql("""
SELECT managerid, COUNT(*) AS num_reports
FROM tab
WHERE managerid IS NOT NULL
GROUP BY managerid
ORDER BY num_reports DESC
""")
task5_sql.show()
task5.show()
Task6=" Task 6 Join Employee with Manager Names (Self-Join)"
task6=df.alias("e").join(df.alias("m"), col("e.managerid") == col("m.empid"), "left") \
  .select(col("e.empname").alias("Employee"), col("m.empname").alias("Manager"))
print(Task6)
task6_sql=spark.sql("""
SELECT e.empname AS employee_name, m.empname AS manager_name
FROM tab e
LEFT JOIN tab m ON e.managerid = m.empid
""")
task6_sql.show()
task6.show()

#Task 7: Calculate % Contribution of Each Salary to Total

total_sal = df.agg(sum("sal")).collect()[0][0]
task7=df.withColumn("percent_share", round((col("sal") / total_sal) * 100, 2))
print("Task 7: Calculate % Contribution of Each Salary to Total")
total_sal = df.agg({"sal": "sum"}).collect()[0][0]

task7_sql=spark.sql(f"""
SELECT *,
       ROUND((sal / {total_sal}) * 100, 2) AS percent_share
FROM tab
""")
task7_sql.show()
task7.show()
#Task8 Find Deep Reporting Hierarchy (Recursive Manager Chain)
# Level 1 join: Employee -> Manager
task8 = df.alias("e").join(df.alias("m"), col("e.managerid") == col("m.empid"), "left") \
  .select(
      col("e.empid").alias("empid"),
      col("e.empname").alias("empname"),
      col("m.empname").alias("manager_name"),
      col("m.managerid").alias("super_managerid")
  )
task8_sql= spark.sql("""
    SELECT 
        e.empid,
        e.empname,
        m.empname AS manager_name,
        sm.empname AS super_manager_name,
        ssm.empname AS super_super_manager_name
    FROM tab e
    LEFT JOIN tab m ON e.managerid = m.empid
    LEFT JOIN tab sm ON m.managerid = sm.empid
    LEFT JOIN tab ssm ON sm.managerid = ssm.empid
""")
task8_sql.show()
print("Task8 Find Deep Reporting Hierarchy (Recursive Manager Chain)")
task8.show()