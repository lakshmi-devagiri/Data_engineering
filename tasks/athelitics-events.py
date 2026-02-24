from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\athlete_events.csv"

# Load the data and replace 'NA' with None
df = spark.read.csv(data, header=True, inferSchema=True, nullValue="NA")

# --- Task 1: Data Cleaning and Type Casting ---
# Objective: Replace 'NA' string values in the 'Medal' column with 'No Medal' and cast
# 'Age', 'Height', and 'Weight' columns to appropriate numeric types for analysis.
df_cleaned = df.withColumn('Medal', when(col('Medal').isNull(), 'No Medal').otherwise(col('Medal')))
df_cleaned = df_cleaned.withColumn('Age', col('Age').cast('integer'))
df_cleaned = df_cleaned.withColumn('Height', col('Height').cast('double'))
df_cleaned = df_cleaned.withColumn('Weight', col('Weight').cast('double'))
print("Task 1: Cleaned DataFrame Schema and Sample")
df_cleaned.printSchema()
df_cleaned.show(5)

# --- Task 2: Finding the Oldest and Youngest Athletes per Sport ---
# Objective: Use groupBy and aggregate functions (max and min) to find the oldest and
# youngest athlete for each unique sport.
print("\n--- Task 2: Oldest and Youngest Athletes per Sport ---")
age_stats = df_cleaned.groupBy('Sport').agg(
    max('Age').alias('Oldest_Age'),
    min('Age').alias('Youngest_Age')
).sort('Sport')
age_stats.show()

# --- Task 3: Analyzing Medal Distribution per Country (NOC) ---
# Objective: Count the number of Gold, Silver, and Bronze medals for each country (NOC)
# using a pivot table.
print("\n--- Task 3: Medal Distribution per Country (NOC) ---")
medal_count = df_cleaned.filter(col('Medal') != 'No Medal').groupBy('NOC').pivot('Medal', ['Gold', 'Silver', 'Bronze']).agg(count('Medal')).fillna(0)
medal_count.show()

# --- Task 4: Athletes with Multiple Medals in a Single Games ---
# Objective: Find athletes who won more than one medal in a single Olympic Games by
# grouping on 'ID' and 'Games' and then filtering for a count greater than 1.
print("\n--- Task 4: Athletes with Multiple Medals in a Single Games ---")
multi_medal_athletes = df_cleaned.filter(col('Medal') != 'No Medal').groupBy('ID', 'Name', 'Games').agg(
    count('Medal').alias('Medal_Count')
).filter(col('Medal_Count') > 1).sort('Medal_Count', ascending=False)
multi_medal_athletes.show()

# --- Task 5: Calculating BMI and Classifying Athletes ---
# Objective: Create a User-Defined Function (UDF) to calculate BMI and then use
# conditional logic (when/otherwise) to classify athletes into weight categories.
print("\n--- Task 5: Calculating BMI and Classifying Athletes ---")
def calculate_bmi(height, weight):
    if height is None or weight is None or height == 0:
        return None
    return weight / ((height / 100) ** 2)
from pyspark.sql.types import *
bmi_udf = udf(calculate_bmi, DoubleType())
df_with_bmi = df_cleaned.withColumn('BMI', bmi_udf(col('Height'), col('Weight')))
df_with_bmi_classified = df_with_bmi.withColumn('BMI_Category',
    when(col('BMI') < 18.5, 'Underweight')
    .when((col('BMI') >= 18.5) & (col('BMI') < 24.9), 'Normal weight')
    .when((col('BMI') >= 25.0) & (col('BMI') < 29.9), 'Overweight')
    .when(col('BMI') >= 30.0, 'Obesity')
    .otherwise('Unknown')
)
df_with_bmi_classified.select('Name', 'Height', 'Weight', 'BMI', 'BMI_Category').filter(col('BMI').isNotNull()).show()

# --- Task 6: Finding the Top 5 Teams by Medal Count in Summer Olympics ---
# Objective: Filter for 'Summer' games, count medals for each team, and find the top 5
# using sort and limit.
print("\n--- Task 6: Top 5 Teams by Medal Count in Summer Olympics ---")
top_summer_teams = df_cleaned.filter((col('Season') == 'Summer') & (col('Medal') != 'No Medal')).groupBy('Team').agg(
    count('Medal').alias('Total_Medals')
).sort(col('Total_Medals').desc()).limit(5)
top_summer_teams.show()

# --- Task 7: Analyzing Medal Trends Over Time (Cumulative Medals for a specific NOC) ---
# Objective: Use a window function to calculate the cumulative number of medals won by a
# specific country over the years.
print("\n--- Task 7: Cumulative Medal Count for USA Over Time ---")
df_usa = df_cleaned.filter((col('NOC') == 'USA') & (col('Medal') != 'No Medal'))
window_spec = Window.partitionBy('NOC').orderBy('Year')
df_usa_cum_medals = df_usa.groupBy('NOC', 'Year').agg(
    count('Medal').alias('Yearly_Medals')
).withColumn('Cumulative_Medals', sum('Yearly_Medals').over(window_spec)).sort('Year')
df_usa_cum_medals.show()

# --- Task 8: Self-Join to find athletes from the same sport, same year but different teams ---
# Objective: Join the DataFrame with itself on 'Sport' and 'Year' but with a condition
# that the 'Team' is different to find competing athletes.
print("\n--- Task 8: Athletes from the same Sport/Year but different Teams ---")
df_distinct = df_cleaned.dropDuplicates(['ID', 'Games', 'Event'])
same_event_diff_team = df_distinct.alias('a').join(
    df_distinct.alias('b'),
    (col('a.Event') == col('b.Event')) & (col('a.Year') == col('b.Year')) & (col('a.ID') != col('b.ID')) & (col('a.Team') != col('b.Team'))
).select(
    col('a.Name').alias('Athlete1'),
    col('a.Team').alias('Team1'),
    col('b.Name').alias('Athlete2'),
    col('b.Team').alias('Team2'),
    col('a.Sport').alias('Sport'),
    col('a.Year').alias('Year')
).distinct()
same_event_diff_team.show()

# --- Task 9: Pivot Table for Medal Counts by Year and Team ---
# Objective: Create a pivot table showing medal counts for a few selected teams
# across specific years.
print("\n--- Task 9: Pivot Table for Medal Counts by Year and Team ---")
teams = ['Denmark/Sweden', 'USA', 'Netherlands']
years = [1900, 1988, 1992, 1994]
df_filtered = df_cleaned.filter((col('Team').isin(teams)) & (col('Year').isin(years)) & (col('Medal') != 'No Medal'))
pivot_table = df_filtered.groupBy('Team').pivot('Year').agg(count('Medal')).fillna(0)
pivot_table.show()

# --- Task 10: Finding Athletes Who Competed in Both Summer and Winter Games ---
# Objective: Group by athlete and check if the set of unique 'Season' values contains both
# 'Summer' and 'Winter'.
print("\n--- Task 10: Athletes who competed in both Summer and Winter Games ---")
df_both_seasons = df_cleaned.groupBy('ID', 'Name').agg(collect_set('Season').alias('Seasons_Participated'))
both_seasons_athletes = df_both_seasons.filter(size(col('Seasons_Participated')) == 2).select('ID', 'Name')
both_seasons_athletes.show()
# --- Task 11: Data Cleaning and Type Casting ---
# Objective: Replace 'NA' string values in the 'Medal' column with 'No Medal' and cast
# 'Age', 'Height', and 'Weight' columns to appropriate numeric types for analysis.
df_cleaned = df.withColumn('Medal', when(col('Medal').isNull(), 'No Medal').otherwise(col('Medal')))
df_cleaned = df_cleaned.withColumn('Age', col('Age').cast('integer'))
df_cleaned = df_cleaned.withColumn('Height', col('Height').cast('double'))
df_cleaned = df_cleaned.withColumn('Weight', col('Weight').cast('double'))
print("Task 11: Cleaned DataFrame Schema and Sample")
df_cleaned.printSchema()
df_cleaned.show(5)

# --- Task 12: Finding the Oldest and Youngest Athletes per Sport ---
# Objective: Use groupBy and aggregate functions (max and min) to find the oldest and
# youngest athlete for each unique sport.
print("\n--- Task 2: Oldest and Youngest Athletes per Sport ---")
age_stats = df_cleaned.groupBy('Sport').agg(
    max('Age').alias('Oldest_Age'),
    min('Age').alias('Youngest_Age')
).sort('Sport')
age_stats.show()

# --- Task 13: Analyzing Medal Distribution per Country (NOC) ---
# Objective: Count the number of Gold, Silver, and Bronze medals for each country (NOC)
# using a pivot table.
print("\n--- Task 13: Medal Distribution per Country (NOC) ---")
medal_count = df_cleaned.filter(col('Medal') != 'No Medal').groupBy('NOC').pivot('Medal', ['Gold', 'Silver', 'Bronze']).agg(count('Medal')).fillna(0)
medal_count.show()

# --- Task 4: Athletes with Multiple Medals in a Single Games ---
# Objective: Find athletes who won more than one medal in a single Olympic Games by
# grouping on 'ID' and 'Games' and then filtering for a count greater than 1.
print("\n--- Task 14: Athletes with Multiple Medals in a Single Games ---")
multi_medal_athletes = df_cleaned.filter(col('Medal') != 'No Medal').groupBy('ID', 'Name', 'Games').agg(
    count('Medal').alias('Medal_Count')
).filter(col('Medal_Count') > 1).sort('Medal_Count', ascending=False)
multi_medal_athletes.show()

# --- Task 15: Calculating BMI and Classifying Athletes ---
# Objective: Create a User-Defined Function (UDF) to calculate BMI and then use
# conditional logic (when/otherwise) to classify athletes into weight categories.
print("\n--- Task 15: Calculating BMI and Classifying Athletes ---")
def calculate_bmi(height, weight):
    if height is None or weight is None or height == 0:
        return None
    return weight / ((height / 100) ** 2)

bmi_udf = udf(calculate_bmi, DoubleType())
df_with_bmi = df_cleaned.withColumn('BMI', bmi_udf(col('Height'), col('Weight')))
df_with_bmi_classified = df_with_bmi.withColumn('BMI_Category',
    when(col('BMI') < 18.5, 'Underweight')
    .when((col('BMI') >= 18.5) & (col('BMI') < 24.9), 'Normal weight')
    .when((col('BMI') >= 25.0) & (col('BMI') < 29.9), 'Overweight')
    .when(col('BMI') >= 30.0, 'Obesity')
    .otherwise('Unknown')
)
df_with_bmi_classified.select('Name', 'Height', 'Weight', 'BMI', 'BMI_Category').filter(col('BMI').isNotNull()).show()

# --- Task 16: Finding the Top 5 Teams by Medal Count in Summer Olympics ---
# Objective: Filter for 'Summer' games, count medals for each team, and find the top 5
# using sort and limit.
print("\n--- Task 16: Top 5 Teams by Medal Count in Summer Olympics ---")
top_summer_teams = df_cleaned.filter((col('Season') == 'Summer') & (col('Medal') != 'No Medal')).groupBy('Team').agg(
    count('Medal').alias('Total_Medals')
).sort(col('Total_Medals').desc()).limit(5)
top_summer_teams.show()

# --- Task 17: Analyzing Medal Trends Over Time (Cumulative Medals for a specific NOC) ---
# Objective: Use a window function to calculate the cumulative number of medals won by a
# specific country over the years.
print("\n--- Task 17: Cumulative Medal Count for USA Over Time ---")
df_usa = df_cleaned.filter((col('NOC') == 'USA') & (col('Medal') != 'No Medal'))
window_spec = Window.partitionBy('NOC').orderBy('Year')
df_usa_cum_medals = df_usa.groupBy('NOC', 'Year').agg(
    count('Medal').alias('Yearly_Medals')
).withColumn('Cumulative_Medals', sum('Yearly_Medals').over(window_spec)).sort('Year')
df_usa_cum_medals.show()

# --- Task 18: Self-Join to find athletes from the same sport, same year but different teams ---
# Objective: Join the DataFrame with itself on 'Sport' and 'Year' but with a condition
# that the 'Team' is different to find competing athletes.
print("\n--- Task 18: Athletes from the same Sport/Year but different Teams ---")
df_distinct = df_cleaned.dropDuplicates(['ID', 'Games', 'Event'])
same_event_diff_team = df_distinct.alias('a').join(
    df_distinct.alias('b'),
    (col('a.Event') == col('b.Event')) & (col('a.Year') == col('b.Year')) & (col('a.ID') != col('b.ID')) & (col('a.Team') != col('b.Team'))
).select(
    col('a.Name').alias('Athlete1'),
    col('a.Team').alias('Team1'),
    col('b.Name').alias('Athlete2'),
    col('b.Team').alias('Team2'),
    col('a.Sport').alias('Sport'),
    col('a.Year').alias('Year')
).distinct()
same_event_diff_team.show()

# --- Task 19: Pivot Table for Medal Counts by Year and Team ---
# Objective: Create a pivot table showing medal counts for a few selected teams
# across specific years.
print("\n--- Task 19: Pivot Table for Medal Counts by Year and Team ---")
teams = ['Denmark/Sweden', 'USA', 'Netherlands']
years = [1900, 1988, 1992, 1994]
df_filtered = df_cleaned.filter((col('Team').isin(teams)) & (col('Year').isin(years)) & (col('Medal') != 'No Medal'))
pivot_table = df_filtered.groupBy('Team').pivot('Year').agg(count('Medal')).fillna(0)
pivot_table.show()

# --- Task 20: Finding Athletes Who Competed in Both Summer and Winter Games ---
# Objective: Group by athlete and check if the set of unique 'Season' values contains both
# 'Summer' and 'Winter'.
print("\n--- Task 20: Athletes who competed in both Summer and Winter Games ---")
df_both_seasons = df_cleaned.groupBy('ID', 'Name').agg(collect_set('Season').alias('Seasons_Participated'))
both_seasons_athletes = df_both_seasons.filter(size(col('Seasons_Participated')) == 2).select('ID', 'Name')
both_seasons_athletes.show()
