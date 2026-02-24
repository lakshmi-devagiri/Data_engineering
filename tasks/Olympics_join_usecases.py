from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =============================================================================
# OLYMPICS ANALYSIS TASKS (1–10)
# -----------------------------------------------------------------------------
# Dataset:
#   - noc_regions.csv    : maps NOC (country code) -> Region (country/area name)
#   - athlete_events.csv : Olympic participations (athlete, event, medal, etc.)
#
# Goals:
#   Task 1  : Youngest gold medalist
#   Task 2  : Female medal counts by medal type
#   Task 3  : Cities with most medals
#   Task 4  : Season (Summer/Winter) with most Olympic years
#   Task 5  : Oldest gold medalist
#   Task 6  : Region medal tally by Season + ranking
#   Task 7  : Medal conversion rate by region
#   Task 8  : Athletes who represented multiple regions
#   Task 9  : Best region per sport (by total medals)
#   Task 10 : YoY medal growth by region in Summer Olympics
# =============================================================================

# -----------------------------------------------------------------------------
# 0) Create SparkSession
# -----------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("Olympics_Analysis")
         .master("local[*]")
         .getOrCreate())

# -----------------------------------------------------------------------------
# 1) Load input datasets
#    - inferSchema=True to automatically detect types
#    - mode='DROPMALFORMED' to skip bad rows instead of failing
# -----------------------------------------------------------------------------
regions_df = (spark.read.csv(
    "D:/bigdata/drivers/noc_regions.csv",
    header=True,
    inferSchema=True,
    mode='DROPMALFORMED'
))

events_df = (spark.read.csv(
    "D:/bigdata/drivers/athlete_events.csv",
    header=True,
    inferSchema=True,
    mode='DROPMALFORMED'
))

print("\n" + "="*80)
print("RAW DATA: Regions")
print("="*80)
regions_df.show(5, truncate=False)

print("\n" + "="*80)
print("RAW DATA: Athlete Events")
print("="*80)
events_df.show(5, truncate=False)

# Cache events_df if reused many times (optional but good for repeated queries)
events_df.cache()

# =============================================================================
# TASK 1: Find the youngest person who got a gold medal
# -----------------------------------------------------------------------------
# WHY:
#   - We want only Gold medals (Medal == 'Gold')
#   - Age must be not null to sort by age
#   - Order ascending by Age to get youngest
#   - Limit 5 in case of ties or to show sample
# =============================================================================
print("\n" + "="*80)
print("Task 1: Youngest Gold Medalists")
print("="*80)

gold_medals_df = events_df.filter(
    (col("Medal") == "Gold") & col("Age").isNotNull()
)

youngest_gold_medalist = (
    gold_medals_df
        .orderBy(col("Age").asc())
        .limit(5)
)

youngest_gold_medalist.select("Name", "Sex", "Age", "Year", "City", "Sport", "Event").show(truncate=False)

# =============================================================================
# TASK 2: Count the number of females who got any medals (gold/silver/bronze)
# -----------------------------------------------------------------------------
# WHY:
#   - We only care about *medal winners*, so Medal must be NOT NULL
#   - Filter to females (Sex == 'F')
#   - Group by Medal type to see counts for Gold, Silver, Bronze
# =============================================================================
print("\n" + "="*80)
print("Task 2: Female Medal Counts by Medal Type")
print("="*80)

female_medalists_count = (
    events_df
        .filter((col("Sex") == "F") & col("Medal").isNotNull())
        .groupBy("Medal")
        .agg(count("*").alias("female_medal_count"))
        .orderBy("Medal")
)

female_medalists_count.show(truncate=False)

# =============================================================================
# TASK 3: Find the city with the most medals
# -----------------------------------------------------------------------------
# WHY:
#   - Count only non-null medals per City (count("Medal") ignores nulls)
#   - Higher count => city where many medals were awarded
#   - Order descending and show top 5
# =============================================================================
print("\n" + "="*80)
print("Task 3: Cities with Most Medals")
print("="*80)

city_most_medals = (
    events_df
        .groupBy("City")
        .agg(count("Medal").alias("MedalCount"))
        .orderBy(desc("MedalCount"))
        .limit(5)
)

city_most_medals.show(truncate=False)

# =============================================================================
# TASK 4: Find the season with the most Olympics
# -----------------------------------------------------------------------------
# WHY:
#   - Each Olympic year belongs to a Season (Summer or Winter)
#   - For each Season, count DISTINCT Years (how many Olympics happened)
#   - Rank by NumYears to see which Season has more editions
# =============================================================================
print("\n" + "="*80)
print("Task 4: Most Frequent Season (by number of Olympic Years)")
print("="*80)

most_occurred_season = (
    events_df
        .groupBy("Season")
        .agg(countDistinct("Year").alias("NumYears"))
        .orderBy(desc("NumYears"))
)

most_occurred_season.show(truncate=False)

# =============================================================================
# TASK 5: Find the oldest person who got a gold medal
# -----------------------------------------------------------------------------
# WHY:
#   - Reuse gold_medals_df (already filtered to Gold + Age not null)
#   - Sort by Age descending to get oldest
#   - Using desc_nulls_last for robustness (though Age is non-null here)
# =============================================================================
print("\n" + "="*80)
print("Task 5: Oldest Gold Medalists")
print("="*80)

oldest_gold_medalist = (
    gold_medals_df
        .orderBy(desc_nulls_last("Age"))
        .limit(5)
)

oldest_gold_medalist.select("Name", "Sex", "Age", "Year", "City", "Sport", "Event").show(truncate=False)

# =============================================================================
# PREP: Join events with regions once (used in Tasks 6–10)
# -----------------------------------------------------------------------------
# WHY:
#   - athlete_events has NOC (country code)
#   - noc_regions maps NOC -> region (full country/area name)
#   - Joining once avoids repeating joins per task
# =============================================================================
print("\n" + "="*80)
print("Preparing joined dataset: events + regions")
print("="*80)

joined_df = (
    events_df
        .join(regions_df.select("NOC", "region"), on="NOC", how="inner")
)

joined_df.select("ID", "Name", "Sex", "Age", "NOC", "region", "Year", "Season", "City", "Sport", "Medal") \
         .show(5, truncate=False)

# =============================================================================
# TASK 6: Region medal tally by Season with rank
# -----------------------------------------------------------------------------
# WHY:
#   - For each Season (Summer/Winter) and region, count total medals
#   - Use a window to rank regions within each Season by medal_count
#   - This shows which regions dominate in each Season
# =============================================================================
print("\n" + "="*80)
print("Task 6: Region Medal Tally by Season with Rank")
print("="*80)

medals_by_region_season = (
    joined_df
        .filter(col("Medal").isNotNull())     # only rows where a medal was actually won
        .groupBy("Season", "region")
        .agg(count("*").alias("medal_count"))
)

w_season_rank = Window.partitionBy("Season").orderBy(desc("medal_count"), asc("region"))

task6 = (
    medals_by_region_season
        .withColumn("rank_in_season", dense_rank().over(w_season_rank))
        .orderBy("Season", "rank_in_season")
)

task6.show(20, truncate=False)

# =============================================================================
# TASK 7: Medal conversion rate by region
# -----------------------------------------------------------------------------
# WHY:
#   - For each region:
#       * participants       = distinct athletes (ID) who ever competed
#       * medalist_athletes  = distinct athletes who won at least one medal
#       * conversion_rate_pct = medalist_athletes / participants * 100
#   - Optional filter: only show regions with >=100 participants to avoid noise
# =============================================================================
print("\n" + "="*80)
print("Task 7: Medal Conversion Rate by Region")
print("="*80)

# Distinct athlete participants per region
participants = (
    joined_df
        .select("region", "ID").distinct()
        .groupBy("region").agg(count("*").alias("participants"))
)

# Distinct medal-winning athletes per region
medalist_athletes = (
    joined_df
        .filter(col("Medal").isNotNull())
        .select("region", "ID").distinct()
        .groupBy("region").agg(count("*").alias("medalist_athletes"))
)

task7 = (
    participants
        .join(medalist_athletes, "region", "left")   # keep all regions with participants
        .fillna(0, subset=["medalist_athletes"])     # regions with no medalists -> 0
        .withColumn(
            "conversion_rate_pct",
            round(col("medalist_athletes") / col("participants") * 100, 2)
        )
        .filter(col("participants") >= 100)          # avoid tiny-sample regions
        .orderBy(desc("conversion_rate_pct"))
)

task7.show(20, truncate=False)

# =============================================================================
# TASK 8: Athletes who represented multiple regions (NOC changes)
# -----------------------------------------------------------------------------
# WHY:
#   - Some athletes may have changed NOC/region (e.g., nationality change)
#   - For each athlete (ID, Name), collect the distinct set of regions they've
#     represented and count how many.
#   - Filter where region_count > 1 to find multi-region athletes.
# =============================================================================
print("\n" + "="*80)
print("Task 8: Athletes Who Represented Multiple Regions")
print("="*80)

athlete_regions = joined_df.select("ID", "Name", "region").distinct()

task8 = (
    athlete_regions
        .groupBy("ID", "Name")
        .agg(
            collect_set("region").alias("regions"),
            countDistinct("region").alias("region_count")
        )
        .filter(col("region_count") > 1)
        .orderBy(desc("region_count"), asc("Name"))
)

task8.show(20, truncate=False)

# =============================================================================
# TASK 9: Best region by sport (ranked by total medals)
# -----------------------------------------------------------------------------
# WHY:
#   - For each sport, count medals per region
#   - Use a window to rank regions within each sport by medal_count
#   - Filter rank == 1 to get the top region(s) per sport
# =============================================================================
print("\n" + "="*80)
print("Task 9: Best Region by Sport (by Medal Count)")
print("="*80)

sport_region_medals = (
    joined_df
        .filter(col("Medal").isNotNull())
        .groupBy("Sport", "region")
        .agg(count("*").alias("medal_count"))
)

w_sport_rank = Window.partitionBy("Sport").orderBy(desc("medal_count"), asc("region"))

task9 = (
    sport_region_medals
        .withColumn("sport_rank", dense_rank().over(w_sport_rank))
        .filter(col("sport_rank") == 1)  # keep only best region(s) per sport
        .orderBy("Sport", "region")
)

task9.show(30, truncate=False)

# =============================================================================
# TASK 10: Year-over-year medal growth by region in Summer Olympics
# -----------------------------------------------------------------------------
# WHY:
#   - Focus on Summer games for consistency (many more events)
#   - For each region & year: count medals
#   - Use lag() to get previous year's medals per region
#   - growth_abs  = medals - prev_medals
#   - growth_pct  = growth_abs / prev_medals * 100
#   - Sort by highest growth_pct to see biggest jumps
# =============================================================================
print("\n" + "="*80)
print("Task 10: Year-over-Year Medal Growth by Region (Summer Olympics)")
print("="*80)

summer_yearly = (
    joined_df
        .filter((col("Season") == "Summer") & col("Medal").isNotNull())
        .groupBy("region", "Year")
        .agg(count("*").alias("medals"))
)

w_yoy = Window.partitionBy("region").orderBy("Year")

task10 = (
    summer_yearly
        .withColumn("prev_medals", lag("medals").over(w_yoy))
        .withColumn("growth_abs", col("medals") - col("prev_medals"))
        .withColumn(
            "growth_pct",
            when(
                col("prev_medals") > 0,
                round(col("growth_abs") / col("prev_medals") * 100, 2)
            ).otherwise(lit(None))
        )
        .filter(col("prev_medals").isNotNull())   # remove first year (no previous)
        .orderBy(desc("growth_pct"))
)

task10.show(20, truncate=False)

print("\n" + "="*80)
print("✓ All Olympic tasks (1–10) executed successfully")
print("="*80)
