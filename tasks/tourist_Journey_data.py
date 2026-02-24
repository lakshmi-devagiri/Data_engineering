from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
data = r"D:\bigdata\drivers\tourist_Journey_data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df=(df.withColumn("start_date", to_date("start_date"))
    .withColumn("end_date",   to_date("end_date"))
    .withColumn("country", initcap(trim("country")))
    .withColumn("gender", lower(trim("gender"))))

# Duration in days (inclusive)
df = df.withColumn("days", datediff("end_date", "start_date") + lit(1))

#----------------Task 0: #
per_person = (
    df.groupBy("touristname")
      .agg(
          min("start_date").alias("min_start"),
          max("end_date").alias("max_end"),
          sort_array(collect_list(struct(col("start_date"), col("country")))).alias("seq"),
      )
      # extract countries in chronological order, then drop repeats while preserving order
      .withColumn("countries", array_distinct(expr("transform(seq, x -> x.country)")))
      .withColumn("date", concat_ws("-", col("min_start").cast("string"), col("max_end").cast("string")))
      .select(col("touristname").alias("name"), "date", "countries")
)

per_person.show(truncate=False)
# ---------- TASK 1: Overlapping trips per tourist ----------
w1 = Window.partitionBy("touristname").orderBy("start_date")
with_lead = (df
    .withColumn("next_start", lead("start_date").over(w1))
    .withColumn("next_end",   lead("end_date").over(w1))
    .withColumn("next_country", lead("country").over(w1))
)
overlaps = (with_lead
    .where(col("next_start").isNotNull() & (col("end_date") >= col("next_start")))
    .select(
        "touristname","country","start_date","end_date",
        col("next_country").alias("overlap_country"),
        col("next_start").alias("overlap_start"),
        col("next_end").alias("overlap_end"))
)

# ---------- TASK 2: Merge adjacent trips (≤2 idle days) in same country ----------
w2 = Window.partitionBy("touristname","country").orderBy("start_date")
gap = datediff("start_date", lag("end_date").over(w2)) - lit(1)
g2 = (df
    .withColumn("gap_days", gap)
    .withColumn("grp", sum(when((col("gap_days") > 2) | col("gap_days").isNull(), 1).otherwise(0)).over(w2))
)
merged_adjacent = (g2.groupBy("touristname","country","grp")
    .agg(
        min("start_date").alias("start_date"),
        max("end_date").alias("end_date"),
        sum("days").alias("sum_days"),
        count(lit(1)).alias("merged_segments"))
    .drop("grp")
)
merged_adjacent.show()
# ---------- TASK 3: Gaps between trips & longest home gap ----------
w3 = Window.partitionBy("touristname").orderBy("start_date")
gaps = (df
    .withColumn("prev_end", lag("end_date").over(w3))
    .withColumn("gap_days", when(col("prev_end").isNull(), None).otherwise(datediff("start_date", col("prev_end")) - lit(1)))
)
longest_home_gap = gaps.groupBy("touristname").agg(max("gap_days").alias("max_gap_days"))
longest_home_gap.show()
# ---------- TASK 4: Next destination & days until next ----------
w4 = Window.partitionBy("touristname").orderBy("start_date")
nexts = (df
    .withColumn("next_country", lead("country").over(w4))
    .withColumn("next_start",   lead("start_date").over(w4))
    .withColumn("days_until_next", datediff(col("next_start"), col("end_date")) - lit(1))
)
nexts.show()
# ---------- TASK 5: Top-2 countries by total days per tourist ----------
by_country = df.groupBy("touristname","country").agg(sum("days").alias("total_days"))
w5 = Window.partitionBy("touristname").orderBy(desc("total_days"), asc("country"))
top2_countries = (by_country
    .withColumn("rk", row_number().over(w5))
    .where(col("rk") <= 2)
    .drop("rk")
)
top2_countries.show()
# ---------- TASK 6: Rolling 365-day window of days abroad ----------
daily = (df
   .withColumn("day", explode(sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY"))))
   .select("touristname","country","day")
)
# Use unix timestamp ordering for rangeBetween with seconds
w6 = (Window.partitionBy("touristname")
        .orderBy(unix_timestamp("day"))
        .rangeBetween(-365*86400, 0))
rolling_days_365 = daily.withColumn("days_in_past_365", count(lit(1)).over(w6))
rolling_days_365.show()
# ---------- TASK 7: Simultaneous travelers (same country, overlapping dates) ----------
# Build two aliased copies (rename country to avoid ambiguity)
a = df.select(col("touristname").alias("t1"),
              col("country").alias("country1"),
              col("start_date").alias("s1"),
              col("end_date").alias("e1"))

b = df.select(col("touristname").alias("t2"),
              col("country").alias("country2"),
              col("start_date").alias("s2"),
              col("end_date").alias("e2"))

# Same-country + overlapping dates + unique ordering (t1 < t2)
cond = (
    (col("country1") == col("country2")) &
    (col("t1") < col("t2")) &
    (col("e1") >= col("s2")) &
    (col("e2") >= col("s1"))
)

simultaneous_pairs = (
    a.join(b, cond, "inner")
     .select(col("country1").alias("country"),
             "t1","s1","e1","t2","s2","e2")
     .distinct()
)
simultaneous_pairs.show()
# ---------- TASK 8: Country streaks (longest consecutive days per tourist) ----------
d = (df
   .withColumn("day", explode(sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY"))))
   .select("touristname","country","day")
)
w8 = Window.partitionBy("touristname","country").orderBy("day")
grp8 = (d
  .withColumn("prev_day", lag("day").over(w8))
  .withColumn("is_break", when(datediff("day","prev_day") == 1, 0).otherwise(1))
  .withColumn("grp", sum("is_break").over(w8))
)
streaks = (grp8.groupBy("touristname","country","grp")
    .agg(min("day").alias("streak_start"),
         max("day").alias("streak_end"),
         count(lit(1)).alias("streak_len"))
    .drop("grp")
)
max_streak = (streaks
    .withColumn("r", row_number().over(Window.partitionBy("touristname").orderBy(desc("streak_len"))))
    .where(col("r") == 1)
    .drop("r")
)
max_streak.show()
# ---------- TASK 9: First visit per country + distinct country count ----------
w9 = Window.partitionBy("touristname","country").orderBy("start_date")
first_visits = (df
    .withColumn("r", row_number().over(w9))
    .where(col("r") == 1)
    .select("touristname","country", col("start_date").alias("first_visit"))
)
country_counts = (df.select("touristname","country").distinct()
                    .groupBy("touristname").agg(count(lit(1)).alias("distinct_countries")))
country_counts.show()
# ---------- TASK 10: Data quality flags ----------
dq_issues = (df
  .withColumn("bad_dates", col("start_date").isNull() | col("end_date").isNull())
  .withColumn("negative_days", col("days") <= 0)
  .withColumn("end_before_start", col("end_date") < col("start_date"))
  .where(col("bad_dates") | col("negative_days") | col("end_before_start"))
)
dq_issues.show()
# ---------- TASK 11: Trip length distribution (buckets) ----------
histogram = (df.where(col("days") > 0)
    .withColumn("bucket", when(col("days")<=7,"<=7")
                          .when(col("days")<=14,"8-14")
                          .when(col("days")<=30,"15-30")
                          .when(col("days")<=90,"31-90")
                          .otherwise(">90"))
    .groupBy("bucket").count()
    .orderBy("bucket")
)
histogram.show()
# ---------- TASK 12: Return-to-country prediction features + simple LR ----------
# Feature table per tourist×country
w12 = Window.partitionBy("touristname","country").orderBy("start_date")
features = (df
  .withColumn("prev_end", lag("end_date").over(w12))
  .withColumn("gap", when(col("prev_end").isNull(), None).otherwise(datediff("start_date","prev_end") - lit(1)))
  .groupBy("touristname","country")
  .agg(count(lit(1)).alias("total_trips"),
       sum("days").alias("total_days"),
       avg("gap").alias("avg_gap_days"),
       max("end_date").alias("last_end"))
  .withColumn("recency_days", datediff(current_date(), col("last_end")))
)
features.show()
# Label: any future trip within 180 days after last_end
future = df.select("touristname","country", col("start_date").alias("future_start"))
label = (features.join(future, ["touristname","country"], "left")
   .withColumn("within_180", when(datediff("future_start","last_end")<=180, 1).otherwise(0))
   .groupBy("touristname","country","total_trips","total_days","avg_gap_days","recency_days")
   .agg(max("within_180").alias("label"))
)
label.show()