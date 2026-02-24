from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\employee.csv"

employee_df = spark.read.format("csv").option("header","true").load(data)

# Calculate average salary for each employee
employee_avg_salary = employee_df.groupBy("EmployeeID") \
    .agg(avg(col("Salary")).alias("AvgSalaryForEmployee"))

# Calculate average salary for each department
department_avg_salary = employee_df.groupBy("DepartmentID") \
    .agg(avg("Salary").alias("AvgSalaryForDepartment"))

# Join employee_df with employee_avg_salary and department_avg_salary
result = employee_df.join(employee_avg_salary, "EmployeeID") \
    .join(department_avg_salary, "DepartmentID") \
    .select("EmployeeID", "DepartmentID", "Salary", "HireDate",
            "AvgSalaryForEmployee", "AvgSalaryForDepartment") \
    .orderBy(col("EmployeeID").desc())

# Show the result
result.show()