from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Task1) Convert the 'date_of_birth' column to a PySpark DateType and calculate the age of each person when they received the award.

#Task2) Calculate the average age of award recipients by race_ethnicity.

#Task3) Find the person with the highest number of awards and the person with the most awards in each decade.

#Task 4 Rank the award recipients based on the number of awards they received, partitioned by the race_ethnicity column.

#Task 5 Create a UDF to catego rize the awards into 'Early', 'Mid', and 'Late' based on the decade in which they were awarded.


# Task 6: Calculate a running total of awards received per person over time (ordered by year).
# Include columns: person_name, year_of_award, running_total_awards

# Task 7: Find persons who won awards in consecutive decades and identify the gap years.
# Include columns: person_name, first_decade, second_decade, gap_years

# Task 8: Calculate the win rate (awards won / total nominations) for each race_ethnicity
# and rank them. Assume a 'nomination_count' column exists or derive it.
# Include columns: race_ethnicity, awards_count, win_rate, rank

# Task 9: Pivot the data to show the count of awards by race_ethnicity and award category
# (Early/Mid/Late). Create a crosstab-style report.
# Include columns: race_ethnicity, Early, Mid, Late, Total

# Task 10: Identify outliers in age_at_award using IQR (Interquartile Range) method
# and flag persons who are significantly older or younger than peers in their race_ethnicity.
# Include columns: person_name, age_at_award, race_ethnicity, outlier_flag, outlier_reason

# Load the dataset
df = spark.read.option("header", "true").option("sep","\t").option("inferSchema","true").csv("D:/bigdata/drivers/Oscars.txt")
df.show()
# Task 1: Convert the 'date_of_birth' column to a PySpark DateType and calculate the age of each person when they received the award.
# Better: robust parse
df = df.withColumn(
    "date_of_birth_parsed",
    coalesce(
        to_date(col("date_of_birth"), "d-MMM-yyyy"),
        to_date(col("date_of_birth"), "dd-MMM-yyyy"),
        to_date(col("date_of_birth"), "d-MMM-yy"),
        to_date(col("date_of_birth"), "dd-MMM-yy")
    )
)

df = df.withColumn("year_of_award", col("year_of_award").cast("int"))

# Age at time of award, NOT today
df = df.withColumn(
    "age_at_award",
    col("year_of_award") - year(col("date_of_birth_parsed"))
)
df.show()

# Task 2: Calculate the average age of award recipients by race_ethnicity.
avg_age_by_race = df.groupBy("race_ethnicity").agg(avg("age_at_award").alias("average_age"))
avg_age_by_race.show()

# Task 3: Find the person with the highest number of awards and the person with the most awards in each decade.
# Overall counts per person
person_award_counts = df.groupBy("person").agg(count("*").alias("awards_count"))
top_overall = person_award_counts.orderBy(col("awards_count").desc()).limit(1)

# Per decade
df = df.withColumn("decade", (col("year_of_award") / 10).cast("int") * 10)

decade_person_counts = (df.groupBy("decade", "person")
                          .agg(count("*").alias("awards_count")))

w_decade = Window.partitionBy("decade").orderBy(col("awards_count").desc())

top_person_per_decade = (decade_person_counts
                         .withColumn("rank_in_decade", dense_rank().over(w_decade))
                         .filter(col("rank_in_decade") == 1))

top_person_per_decade.show()
# Task 4: Rank the award recipients based on the number of awards they received, partitioned by the race_ethnicity column.
race_person_counts = (df.groupBy("race_ethnicity", "person")
                        .agg(count("*").alias("awards_count")))

w_race = Window.partitionBy("race_ethnicity").orderBy(col("awards_count").desc())

df_ranked_race = race_person_counts.withColumn("rank", rank().over(w_race))


# Task 5: Create a UDF to categorize the awards into 'Early', 'Mid', and 'Late' based on the decade in which they were awarded.
df = df.withColumn("decade", (col("year_of_award") / 10).cast("int") * 10)

def categorize_decade(decade):
    if decade is None:
        return None
    if decade < 1960:
        return "Early"
    elif decade < 1990:
        return "Mid"
    else:
        return "Late"

categorize_udf = udf(categorize_decade, StringType())
df_with_category = df.withColumn("award_category", categorize_udf(col("decade")))
df_with_category.show()


# --------------------------------------------------------------------------------
# Task 6) Running total of awards per person over time (ordered by year)
# --------------------------------------------------------------------------------
print("\n" + "="*80)
print("Task 6: Running Total of Awards Per Person Over Time")
print("="*80)

w_running = Window.partitionBy("person").orderBy("year_of_award") \
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running_total = df.withColumn("running_total_awards", count("*").over(w_running))

df_running_total.select("person", "year_of_award", "award", "running_total_awards") \
                .orderBy("person", "year_of_award", "running_total_awards") \
                .show(50, truncate=False)

# --------------------------------------------------------------------------------
# Task 7) Persons who won awards in multiple decades and the gap in years
# (consecutive decades -> gap_years = 10; if you want at least 10, use >= 10)
# --------------------------------------------------------------------------------
# Ensure usable year and create decade here (safe even if done earlier)
df7 = (
    df.withColumn("year_of_award", col("year_of_award").cast("int"))
      .filter(col("year_of_award").isNotNull())
      .withColumn("decade", (col("year_of_award") / 10).cast("int") * 10)
)

# Unique person–decade combos (avoid counting multiple awards in same decade)
df_decades = df7.select("person", "decade").distinct()

w = Window.partitionBy("person").orderBy("decade")
gaps = (
    df_decades
    .withColumn("prev_decade", lag("decade").over(w))
    .withColumn("gap_years", col("decade") - col("prev_decade"))
)

# Strictly consecutive decades (exactly 10-year gap)
consecutive = gaps.filter(col("prev_decade").isNotNull() & (col("gap_years") == 10))

# If you prefer “won in multiple decades (≥1 decade gap)”, use >= 10 instead:
# consecutive = gaps.filter(col("prev_decade").isNotNull() & (col("gap_years") >= 10))

result7 = (
    consecutive
    .select(
        col("person").alias("person_name"),
        col("prev_decade").alias("first_decade"),
        col("decade").alias("second_decade"),
        "gap_years"
    )
    .orderBy("person_name", "first_decade")
)

result7.show(truncate=False)
# --------------------------------------------------------------------------------
# Task 8) Win rate (awards / nominations) for each race_ethnicity and rank them.
# Here we'll assume each record is 1 nomination + 1 award for simplicity.
# In real data, you'd have a separate nomination_count or is_winner flag.
# --------------------------------------------------------------------------------
print("\n" + "="*80)
print("Task 8: Win Rate by Race/Ethnicity (simplified)")
print("="*80)

# For demo: assume 1 nomination per row
df = df.withColumn("nomination_count", lit(1))

win_rate_df = (df.groupBy("race_ethnicity")
               .agg(
                   count("award").alias("awards_count"),
                   sum("nomination_count").alias("nominations")
               )
               .withColumn("win_rate", col("awards_count") / col("nominations")))

w_win = Window.orderBy(col("win_rate").desc())
win_rate_df = win_rate_df.withColumn("rank", dense_rank().over(w_win))

win_rate_df.orderBy("rank").show(truncate=False)

# --------------------------------------------------------------------------------
# Task 9) Pivot: count of awards by race_ethnicity and award category
# --------------------------------------------------------------------------------
print("\n" + "="*80)
print("Task 9: Pivot - Awards Count by Race/Ethnicity and Award Category")
print("="*80)

# 1) Make sure year_of_award is int
df9 = df.withColumn("year_of_award", col("year_of_award").cast("int"))

# 2) Derive decade
df9 = df9.withColumn("decade", (col("year_of_award") / 10).cast("int") * 10)

# 3) UDF to categorize decade into Early / Mid / Late
def categorize_decade(decade):
    if decade is None:
        return None
    if decade < 1960:
        return "Early"
    elif decade < 1990:
        return "Mid"
    else:
        return "Late"

categorize_udf = udf(categorize_decade, StringType())

# 4) Add award_category column
df9 = df9.withColumn("award_category", categorize_udf(col("decade")))

# 5) Pivot on award_category
pivot_df = (
    df9.groupBy("race_ethnicity")
       .pivot("award_category", ["Early", "Mid", "Late"])
       .agg(count("*").alias("awards_count"))
       .fillna(0)
)

# 6) Add Total column
pivot_df = pivot_df.withColumn(
    "Total",
    col("Early") + col("Mid") + col("Late")
)

pivot_df.orderBy("race_ethnicity").show(truncate=False)

# --------------------------------------------------------------------------------
# Task 10) Outliers in age_at_award using IQR per race_ethnicity
# --------------------------------------------------------------------------------
print("\n" + "="*80)
print("Task 10: Outliers in Age at Award (IQR Method)")
print("="*80)

age_stats = (df.groupBy("race_ethnicity")
             .agg(
                 expr("percentile_approx(age_at_award, 0.25)").alias("Q1"),
                 expr("percentile_approx(age_at_award, 0.75)").alias("Q3")
             ))

age_stats = (age_stats
             .withColumn("IQR", col("Q3") - col("Q1"))
             .withColumn("lower_bound", col("Q1") - 1.5 * col("IQR"))
             .withColumn("upper_bound", col("Q3") + 1.5 * col("IQR")))

df_with_outliers = df.join(age_stats, "race_ethnicity", "left")

df_with_outliers = (df_with_outliers
                    .withColumn(
                        "outlier_flag",
                        when(
                            (col("age_at_award") < col("lower_bound")) |
                            (col("age_at_award") > col("upper_bound")),
                            lit("Yes")
                        ).otherwise(lit("No"))
                    )
                    .withColumn(
                        "outlier_reason",
                        when(col("age_at_award") < col("lower_bound"), lit("Too Young"))
                        .when(col("age_at_award") > col("upper_bound"), lit("Too Old"))
                        .otherwise(lit("Normal"))
                    ))

df_with_outliers.filter(col("outlier_flag") == "Yes") \
    .select("person", "age_at_award", "race_ethnicity", "outlier_flag", "outlier_reason") \
    .show(50, truncate=False)

print("\n✓ All advanced tasks completed successfully!")
