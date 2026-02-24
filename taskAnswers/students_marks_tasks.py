from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\students_marks.csv"
opt = {
    "header":"true",
    "inferSchema":"true",
    "sep":","
}
df = spark.read.format("csv").options(**opt).load(data)


'''
pyspark practical exercise

1) take studentsmarks.csv data from drivers folder 
Sample data like this
studentid,subject,marks
1,Maths,85
1,Computers,98
1,Science,90
1,Social,79
1,English,40
1,Hindi,49
2,Maths,90


PROBLEM STATEMENT:

1.You are given a student dataframe containing columns student_id and marks. 
Your task is to find out best of 3 marks and then avg of that for best of three using Pyspark dataframe api ?
 Get the Average rounded to 2 decimal places
sample output like this
Studentid, average_marks
1,91.00
2,89.00
3,85.33

2) Subject-wise Average: Calculate the average marks for each subject across all students.

+---------+-------------+
|  subject|average_marks|
+---------+-------------+
|  Science|        83.89|
|Computers|        93.95|
|  English|        48.69|
|   Social|        74.99|

3) Top Performing Students: Find the top 10 students with the highest total marks.
+---------+-----------+----+
|studentid|total_marks|rank|
+---------+-----------+----+
|        5|        479|   1|
|        2|        472|   2|
|       27|        468|   3|
4) Subject-wise Performance: Determine the top 3 students in each subject based on their marks.
+---------+---------+-----+----+
|studentid|  subject|marks|rank|
+---------+---------+-----+----+
|        3|  Science|   92|   1|
|        9|  Science|   91|   2|
|        1|  Science|   90|   3|
|        1|Computers|   98|   1|
|       16|Computers|   98|   2|
|       24|Computers|   98|   3|
5)Subject-wise Pass Percentage: Calculate the pass percentage for each subject, considering a pass mark of 50.
+---------+---------------+
|  subject|pass_percentage|
+---------+---------------+
|  Science|           98.0|
|Computers|          100.0|
|  English|           38.0|
|   Social|           98.0|
|    Hindi|           99.0|
|    Maths|          100.0|
+---------+---------------+
6) Subject-wise Rank: Rank students in each subject based on their marks get top 5 students based on subject
|studentid|  subject|marks|rank|
+---------+---------+-----+----+
|        9|  Science|   91|   1|
|       15|  Science|   90|   2|
|       10|  Science|   89|   3|
|        4|  Science|   88|   4|

7) Students failing in Multiple Subjects: Identify students who have failed in more than two subjects.
if student marks <50 consider as failed
+---------+-------------------------------------------+
|studentid|failed_subjects                            |
+---------+-------------------------------------------+
|1        |[{Science, 40}, {English, 40}, {Hindi, 49}]|

'''
#1)find out best of 3 marks and then avg
window_spec = Window.partitionBy("studentid").orderBy(col("marks").desc())

# Add a row number to each student's marks in descending order
df_with_rank = df.withColumn("rank", row_number().over(window_spec))

# Filter the top 3 marks for each student
top_3_marks = df_with_rank.filter(col("rank") <= 3)

# Calculate the average of the top 3 marks for each student
best_of_3_avg = top_3_marks.groupBy("studentid") \
    .agg(round(avg(col("marks")), 2).alias("average_marks")).orderBy(col("studentid").asc())

best_of_3_avg.show()
#2) Calculate the average marks for each subject
subject_wise_avg = df.groupBy("subject") \
    .agg(round(avg(col("marks")), 2).alias("average_marks"))

subject_wise_avg.show()

#3)Top Performing Students: Find the top 10 students with the highest total marks.
total_marks = df.groupBy("studentid") \
    .agg(sum(col("marks")).alias("total_marks"))

# Define a window spec ordered by total marks desc
window_spec_total = Window.orderBy(col("total_marks").desc())

# Add a rank to each student's total marks
top_students = total_marks.withColumn("rank", row_number().over(window_spec_total))

# Filter the top 10 students
top_10_students = top_students.filter(col("rank") <= 10)

top_10_students.show()
#4) subjectwise performance
window_spec_subject = Window.partitionBy("subject").orderBy(col("marks").desc())

# Add a rank to each student's marks in each subject
subject_performance = df.withColumn("rank", row_number().over(window_spec_subject))

# Filter the top 3 students for each subject
top_3_per_subject = subject_performance.filter(col("rank") <= 3)

top_3_per_subject.show()
#5 Subject-wise Pass Percentage:
pass_percentage = df.withColumn("passed", when(col("marks") >= 50, 1).otherwise(0))

subject_pass_percentage = pass_percentage.groupBy("subject") \
    .agg((sum(col("passed")) / count(col("marks")) * 100).alias("pass_percentage"))

subject_pass_percentage.show()
#6)Subject-wise Rank
# Reuse the window spec partitioned by subject and ordered by marks desc
# Add a rank to each student's marks in each subject
subject_ranking = df.withColumn("rank", rank().over(window_spec_subject))

# Filter the top 5 students for each subject
top_5_per_subject = subject_ranking.filter(col("rank") <= 5)

# Show the result
top_5_per_subject.show()
#7) 7. Students Failing in Multiple Subjects
# Mark students who failed in each subject (consider marks < 50 as failed)
df_failed = df.withColumn("failed", when(col("marks") < 50, 1).otherwise(0))

# Count the number of subjects each student has failed
failed_count = df_failed.groupBy("studentid") \
    .agg(count(when(col("failed") == 1, True)).alias("failed_subjects_count"))

# Filter students who failed in more than two subjects
students_multiple_failures = failed_count.filter(col("failed_subjects_count") > 2)

# Join back to get the subject names and marks for the filtered students
failed_subjects_details = df_failed.join(
    students_multiple_failures, "studentid"
).filter(col("failed") == 1).select(
    "studentid", "subject", "marks"
)

# Collect the subject names and marks for each student
result = failed_subjects_details.groupBy("studentid").agg(
    collect_list(struct(col("subject"), col("marks"))).alias("failed_subjects")
)

# Show the result
result.show(truncate=False)