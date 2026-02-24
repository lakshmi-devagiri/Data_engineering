from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\empmysql1.csv"
df = spark.read.format("csv").option("header", "true").load(data)
df=df.withColumn("hiredate", to_date(col("hiredate"),"d-MMM-yyyy"))
df.show()
df.createOrReplaceTempView("emp")
# Task 6: Longest and Shortest-Serving Employees
## DataFrame API
longest = df.orderBy(col("hiredate")).limit(1)
shortest = df.orderBy(col("hiredate").desc()).limit(1)
window_all = Window.orderBy("hiredate")
ranked = df.withColumn("rn_asc", row_number().over(window_all))\
            .withColumn("rn_desc", row_number().over(window_all.orderBy(col("hiredate").desc())))\
            .filter((col("rn_asc") == 1) | (col("rn_desc") == 1))
ranked.show()
print("# Task 6: Longest and Shortest-Serving Employees")
## Spark SQL
spark.sql("""
    SELECT * FROM emp ORDER BY hiredate ASC LIMIT 1
""")
spark.sql("""
    SELECT * FROM emp ORDER BY hiredate DESC LIMIT 1
""")
ranked1=spark.sql("""
    SELECT * FROM (
        SELECT *, 
               ROW_NUMBER() OVER (ORDER BY hiredate) as rn1,
               ROW_NUMBER() OVER (ORDER BY hiredate DESC) as rn2
        FROM emp
    ) tmp
    WHERE rn1 = 1 OR rn2 = 1
""")
ranked1.show()
# Task 7: Employees Hired in Same Month
## DataFrame API
same_month = df.withColumn("month", month("hiredate")).groupBy("month").count()
same_month.show()
## SQL
print("Task 7: Employees Hired in Same Month")
same_month1=spark.sql("SELECT MONTH(hiredate) AS month, COUNT(*) FROM emp GROUP BY MONTH(hiredate)")
same_month1.show()
# Task 8: Filter Employees in Q1/Q4
## DataFrame API
qtr_df = df.withColumn("month", month(col("hiredate")))\
           .withColumn("quarter",
                      when(col("month").between(1, 3), "Q1")\
                      .when(col("month").between(10, 12), "Q4")\
                      .otherwise("Other"))\
           .filter(col("quarter").isin("Q1", "Q4"))
qtr_df.show()
## SQL
print("Task 8: Filter Employees in Q1/Q4")
qtr_df1=spark.sql("""
    SELECT *,
           CASE 
               WHEN MONTH(hiredate) BETWEEN 1 AND 3 THEN 'Q1'
               WHEN MONTH(hiredate) BETWEEN 10 AND 12 THEN 'Q4'
               ELSE 'Other'
           END AS quarter
    FROM emp
    WHERE MONTH(hiredate) BETWEEN 1 AND 3 OR MONTH(hiredate) BETWEEN 10 AND 12
""")
qtr_df1.show()
# Task 9: Employees Hired on Weekend
## DataFrame API
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

weekend = df.filter(date_format("hiredate", "u").isin("6", "7"))
weekend.show()
## SQL
print("Task 9: Employees Hired on Weekend")
weekend1=spark.sql("""
    SELECT * FROM emp WHERE date_format(hiredate, 'u') IN ('6','7')
""")
weekend1.show()

# Task 10: Calculate Retirement Year (age 60)
## DataFrame API
retirement = df.withColumn("retirement_year", year("hiredate") + (60 - col("age")))
retirement.show()
## SQL
print("Task 10: Calculate Retirement Year (age 60)")
retirement1=spark.sql("SELECT *, YEAR(hiredate) + (60 - age) AS retirement_year FROM emp")
retirement1.show()
# Task 11: Running Total of Salary per Department
## DataFrame API


w1 = Window.partitionBy("deptno").orderBy("hiredate").rowsBetween(Window.unboundedPreceding, Window.currentRow)
running_sal = df.withColumn("running_sal", sum("sal").over(w1))
running_sal.show()
print("Task 11: Running Total of Salary per Department")
## SQL
running_sal1=spark.sql("""
    SELECT *,
           SUM(sal) OVER (PARTITION BY deptno ORDER BY hiredate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sal
    FROM emp
""")
running_sal1.show()
# Task 12: Salary Difference from Previous Employee
## DataFrame API
w2 = Window.partitionBy("deptno").orderBy("hiredate")
sal_diff = df.withColumn("prev_sal", lag("sal", 1).over(w2))\
             .withColumn("salary_change", col("sal") - col("prev_sal"))
sal_diff.show()
## SQL
sal_diff1=spark.sql("""
    SELECT *, sal - LAG(sal) OVER (PARTITION BY deptno ORDER BY hiredate) as salary_change
    FROM emp
""")
sal_diff1.show()
# Task 13: First and Last Hired Employee in Each Job Role
## DataFrame API
w3 = Window.partitionBy("job").orderBy("hiredate")
first_last = df.withColumn("first_hire", first("hiredate").over(w3))\
               .withColumn("last_hire", last("hiredate").over(w3.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
first_last.show()
## SQL
print("Task 13: First and Last Hired Employee in Each Job Role")
first_last.show()
first_last1=spark.sql("""
    SELECT *,
           FIRST_VALUE(hiredate) OVER (PARTITION BY job ORDER BY hiredate) AS first_hire,
           LAST_VALUE(hiredate) OVER (PARTITION BY job ORDER BY hiredate 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hire
    FROM emp
""")
first_last1.show()

# Task 14: Gender Pay Gap in Each Department
## DataFrame API
w4 = Window.partitionBy("deptno", "gender")
pay_gap_df = df.withColumn("avg_sal_gender", avg("sal").over(w4))
pay_gap_pivot = pay_gap_df.groupBy("deptno").pivot("gender").agg(first("avg_sal_gender"))\
                 .withColumn("pay_gap", abs(col("f") - col("m")))
pay_gap_pivot.show()
## SQL
print("Task 14: Gender Pay Gap in Each Department")
pay_gap_pivot1=spark.sql("""
    SELECT deptno, 
           MAX(CASE WHEN gender = 'f' THEN avg_sal END) AS f,
           MAX(CASE WHEN gender = 'm' THEN avg_sal END) AS m,
           ABS(MAX(CASE WHEN gender = 'f' THEN avg_sal END) - MAX(CASE WHEN gender = 'm' THEN avg_sal END)) AS pay_gap
    FROM (
        SELECT deptno, gender, AVG(sal) OVER (PARTITION BY deptno, gender) AS avg_sal
        FROM emp
    )
    GROUP BY deptno
""")
pay_gap_pivot1.show()
# Task 15: Bonus Percentage and Avg Bonus % per Dept
## DataFrame API
bonus_df = df.withColumn("bonus_pct", when(col("comm") > 0, col("comm") / col("sal")))
bonus_avg = bonus_df.withColumn("avg_bonus", avg("bonus_pct").over(Window.partitionBy("deptno")))
bonus_avg.show()
## SQL
print("Task 15: Bonus Percentage and Avg Bonus % per Dept")
bonus_avg1=spark.sql("""
    SELECT *,
           CASE WHEN comm > 0 THEN comm/sal ELSE NULL END AS bonus_pct,
           AVG(CASE WHEN comm > 0 THEN comm/sal ELSE NULL END) OVER (PARTITION BY deptno) AS avg_bonus
    FROM emp
""")
bonus_avg.show()

# Task 16: Senior vs Junior Salary Gap
## DataFrame API
w5 = Window.partitionBy("deptno").orderBy("hiredate")
ranked = df.withColumn("rank_asc", row_number().over(w5))\
           .withColumn("rank_desc", row_number().over(w5.orderBy(col("hiredate").desc())))
senior_df = ranked.filter(col("rank_asc") == 1).select("deptno", col("sal").alias("senior_sal"))
junior_df = ranked.filter(col("rank_desc") == 1).select("deptno", col("sal").alias("junior_sal"))
salary_gap = senior_df.join(junior_df, "deptno").withColumn("gap", col("senior_sal") - col("junior_sal"))
salary_gap.show()
## SQL
print("Task 16: Senior vs Junior Salary Gap")
salary_gap1=spark.sql("""
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY hiredate) AS r1,
               ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY hiredate DESC) AS r2
        FROM emp
    ),
    senior AS (SELECT deptno, sal AS senior_sal FROM ranked WHERE r1 = 1),
    junior AS (SELECT deptno, sal AS junior_sal FROM ranked WHERE r2 = 1)
    SELECT s.deptno, senior_sal, junior_sal, senior_sal - junior_sal AS gap
    FROM senior s JOIN junior j ON s.deptno = j.deptno
""")
salary_gap.show()

