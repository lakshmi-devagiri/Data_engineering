from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#i have vegetables i want to add one extra column that veg price inc or dec i want, pls note starting with date i want to calculate one column
#another column compare with previous day changes. likt that try
'''
veg_name,date,price
Spinach,01-10-2024,22
Spinach,02-10-2024,29
Spinach,03-10-2024,28
output i want like this.
veg_name,date,price,day_change,week_change
Spinach,01-10-2024,22,0,0
Spinach,02-10-2024,29,7up,7up
Spinach,03-10-2024,28,1down,6up

'''

data = r"D:\bigdata\drivers\veg_price_data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)
df.show()

# Create a DataFrame

from pyspark.sql.window import *
# Define window spec
window_spec = Window.partitionBy("veg_name").orderBy(col("date"))

# Calculate daily price change
df = df.withColumn("prev_price", lag("price").over(window_spec))
df = df.withColumn(
    "day_change",
    when(col("prev_price").isNull(), lit("0"))
    .when(col("price") > col("prev_price"), concat((col("price") - col("prev_price")).cast("string"), lit("up")))
    .when(col("price") < col("prev_price"), concat((col("prev_price") - col("price")).cast("string") , lit("down")))
    .otherwise("0"))

df.show()
# Get the starting price for each vegetable
df = df.withColumn("starting_price", first("price").over(Window.partitionBy("veg_name").orderBy("date")))

# Calculate weekly cumulative change based on starting price
df = df.withColumn(
    "week_change",
    when(col("price") > col("starting_price"), concat((col("price") - col("starting_price")).cast("string") , lit("up")))
    .when(col("price") < col("starting_price"), concat((col("starting_price") - col("price")).cast("string") , lit("down")))
    .otherwise(lit("0"))
)

# Drop unnecessary columns
df = df.drop("prev_price", "starting_price")
# Show the result
df.show()

def parse_dates(df, date_fmt="dd-MM-yyyy"):
    """Parse date column to DateType and price to double."""
    return df.withColumn("date", to_date(col("date"), date_fmt)) \
             .withColumn("price", col("price").cast("double"))

def fill_missing_dates_forward(df):
    """
    Fill missing calendar dates per veg_name between min and max,
    then forward-fill the last observed price.
    """
    df2 = parse_dates(df)
    rng = df2.groupBy("veg_name").agg(min("date").alias("min_date"), max("date").alias("max_date"))
    seq = rng.withColumn("date", explode(sequence(col("min_date"), col("max_date")))).select("veg_name", "date")
    joined = seq.join(df2.select("veg_name", "date", "price"), ["veg_name", "date"], "left")
    w = Window.partitionBy("veg_name").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    filled = joined.withColumn("price_fwd", last("price", True).over(w)).drop("price").withColumnRenamed("price_fwd", "price")
    return filled.select("veg_name", "date", "price")

def moving_average_7d(df):
    """Compute 7-day rolling average (including current day). Assumes daily rows or after fill_missing_dates_forward."""
    d = parse_dates(df)
    w = Window.partitionBy("veg_name").orderBy("date").rowsBetween(-6, 0)
    return d.withColumn("ma_7d", avg("price").over(w))

def detect_spikes_pct(df, pct_threshold=30.0):
    """
    Flag rows where percent change versus previous day exceeds threshold.
    Returns columns: pct_change (percent) and spike_flag (up/down/none).
    """
    d = parse_dates(df)
    d = d.withColumn("prev_price", lag("price").over(Window.partitionBy("veg_name").orderBy("date")))
    d = d.withColumn("pct_change", when(col("prev_price").isNull(), lit(0.0))
                               .otherwise(((col("price") - col("prev_price")) / col("prev_price")) * 100.0))
    d = d.withColumn(
        "spike_flag",
        when(abs(col("pct_change")) >= lit(float(pct_threshold)),
             when(col("pct_change") > 0, lit("spike_up")).otherwise(lit("spike_down"))
        ).otherwise(lit("none"))
    ).drop("prev_price")
    return d

def compute_increase_streaks(df):
    """
    Compute consecutive increasing streak lengths per veg_name.
    For each row, returns 'inc_streak' (1 for first increase day, increments while price > prev_price).
    Resets when price <= prev_price.
    """
    d = parse_dates(df)
    w_ord = Window.partitionBy("veg_name").orderBy("date")
    d = d.withColumn("prev_price", lag("price").over(w_ord))
    # mark reset points: 1 when start of new streak (prev null or price <= prev)
    d = d.withColumn("is_reset", when(col("prev_price").isNull() | (col("price") <= col("prev_price")), lit(1)).otherwise(lit(0)))
    # group id increments at each reset
    w_cum = Window.partitionBy("veg_name").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    d = d.withColumn("streak_grp", sum("is_reset").over(w_cum))
    # row number inside group gives streak length, but set to 0 when not increasing
    w_grp = Window.partitionBy("veg_name", "streak_grp").orderBy("date")
    d = d.withColumn("row_in_grp", row_number().over(w_grp))
    d = d.withColumn("inc_streak", when(col("prev_price").isNull(), lit(0))
                                .when(col("price") > col("prev_price"), col("row_in_grp"))
                                .otherwise(lit(0)))
    return d.drop("prev_price", "is_reset", "streak_grp", "row_in_grp")

def weekly_volatility_and_lag7(df):
    """
    Compute 7-day rolling stddev (volatility) and change vs 7 days ago (lag 7).
    Adds columns: vol_7d, lag7_price, change_vs_7d (absolute) and pct_vs_7d.
    """
    d = parse_dates(df)
    w7 = Window.partitionBy("veg_name").orderBy("date").rowsBetween(-6, 0)
    w_ord = Window.partitionBy("veg_name").orderBy("date")
    d = d.withColumn("vol_7d", stddev_samp("price").over(w7))
    d = d.withColumn("lag7_price", lag("price", 7).over(w_ord))
    d = d.withColumn("change_vs_7d", when(col("lag7_price").isNull(), lit(None)).otherwise(col("price") - col("lag7_price")))
    d = d.withColumn("pct_vs_7d", when(col("lag7_price").isNull(), lit(None)).otherwise((col("price") - col("lag7_price")) / col("lag7_price") * 100.0))
    return d

# Sample usage (replace path as needed):
# load = spark.read.csv(`D:\bigdata\drivers\veg_price_data.csv`, header=True, inferSchema=True)
df_filled = fill_missing_dates_forward(df)
df_ma = moving_average_7d(df_filled)
df_spikes = detect_spikes_pct(df_filled, pct_threshold=25.0)
df_streaks = compute_increase_streaks(df_filled)
df_weekly = weekly_volatility_and_lag7(df_filled)
df_filled.show()
df_ma.show()
df_spikes.show()
df_streaks.show()
df_weekly.show()
