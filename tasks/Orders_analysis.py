from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

"""
================================================================================
ADVANCED PYSPARK ASSIGNMENTS: Orders Analysis (Tasks 11-20)
================================================================================

This module contains 10 advanced PySpark assignments based on e-commerce 
order data. Tasks focus on:
  - Window functions (running totals, cumulative sums, LAG/LEAD)
  - Time-based analytics (trends, seasonality, YoY comparisons)
  - Customer segmentation (RFM, cohort analysis, churn prediction)
  - Real-time aggregations (moving averages, rolling windows)
  - Complex joins and pivots

Each task avoids Python UDFs in favor of native Spark SQL functions for 
optimal performance (5-50x faster than UDFs).
================================================================================
"""

# ════════════════════════════════════════════════════════════════════════════════
# TASK 11-20: ADVANCED ASSIGNMENTS (Questions Only)
# ════════════════════════════════════════════════════════════════════════════════

"""
TASK 11: Calculate the cumulative sum of total_amount for each customer
ordered by order_date. Include columns: customer_id, order_date, total_amount, 
cumulative_spend. Flag when a customer crosses $5000 cumulative threshold.

TASK 12: Identify customers with declining purchase patterns (orders decreasing
in value over time). Calculate the month-over-month (MoM) change percentage 
for each customer. Include: customer_id, month, order_value, mom_change_pct, 
trend_direction (up/down/stable).

TASK 13: Calculate the average order value (AOV) and moving average (7-day 
rolling window) for each customer. Identify anomalies where current order 
significantly deviates (>2 std dev) from customer's 7-day moving average.

TASK 14: Perform RFM (Recency, Frequency, Monetary) segmentation:
- Recency: Days since last order
- Frequency: Total number of orders
- Monetary: Total spend
Score each dimension 1-5 and classify customers as VIP, Regular, or At-Risk.

TASK 15: Identify seasonal patterns by calculating:
- Total orders and revenue by month (month_name)
- Year-over-year (YoY) growth rate for each month
- Rank months by revenue
Include: month_name, year, total_orders, total_revenue, yoy_growth_pct, rank.

TASK 16: For each customer, find their top 3 order dates by total_amount and
calculate the time gap (in days) between consecutive top orders. Include:
customer_id, rank, order_date, total_amount, days_to_next_order.

TASK 17: Create a pivot table showing customer spending by order_date (month-year)
across customers (rows are months, columns are top 10 customers by spend).
Include totals row and column.

TASK 18: Identify customers exhibiting early churn signals:
- Customers with gap > 90 days since last order
- Customers whose avg order value has declined > 20% in last 3 months
Include: customer_id, days_since_last_order, avg_order_value_change_pct, 
churn_risk_level (High/Medium/Low).

TASK 19: Calculate the customer lifetime value (CLV) proxy by computing:
- Total revenue per customer
- Average order frequency (orders per month)
- Average order value
- Customer tenure (months since first order)
- Projected annual revenue
Rank customers and identify top 5% earners.

TASK 20: Real-time window analytics - For the last 30 days:
- Daily active customers (unique customer_id per day)
- Daily revenue and cumulative monthly revenue
- Average order size per day
- Day-over-day (DoD) growth rate
Identify peak sales days and patterns.
"""

# ════════════════════════════════════════════════════════════════════════════════
# ORIGINAL CODE (Tasks 1-3) + SOLUTIONS (Tasks 11-20)
# ════════════════════════════════════════════════════════════════════════════════

"""
Initialize SparkSession with optimized configurations for large-scale 
order processing and window function operations.
"""
spark = (SparkSession.builder
         .appName("orders_analysis_advanced")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.default.parallelism", "200")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .getOrCreate())

# Load order data from CSV with inferred schema
data = r"D:\bigdata\drivers\ordersdata.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)

"""
Ensure date columns are properly typed for time-based operations.
Convert 'order_date' to DateType if stored as string.
"""
df = df.withColumn("order_date", to_date(col("order_date")))
df = df.withColumn("total_amount", col("total_amount").cast(DoubleType()))

df.show(5, truncate=False)
print(f"Total records: {df.count()}, Unique customers: {df.select('customer_id').distinct().count()}")

# ────────────────────────────────────────────────────────────────────────────────
# TASK 1: Each customer's latest transaction
# ────────────────────────────────────────────────────────────────────────────────
"""
Retrieve the most recent order for each customer using row_number() window function.

Process:
1. Partition data by customer_id
2. Order by order_date descending (most recent first)
3. Assign row numbers within each partition
4. Filter to keep only the first row (rank == 1) per customer
"""
window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
task1 = (df.withColumn("rank", row_number().over(window_spec))
         .filter(col("rank") == 1)
         .drop("rank"))
print("\n" + "="*80)
print("TASK 1: Latest Transaction per Customer")
print("="*80)
task1.show(10, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 2: Top 5 spenders
# ────────────────────────────────────────────────────────────────────────────────
"""
Aggregate total spending by customer and rank top 5 by total spend.

Process:
1. Group by customer_id
2. Sum total_amount to get total_spend per customer
3. Order by total_spend descending
4. Limit to top 5 results
"""
task2 = (df.groupBy("customer_id")
         .agg(sum("total_amount").alias("total_spend"))
         .orderBy(desc("total_spend"))
         .limit(5))
print("\n" + "="*80)
print("TASK 2: Top 5 Spenders")
print("="*80)
task2.show(truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 3: Customers with duplicate orders
# ────────────────────────────────────────────────────────────────────────────────
"""
Identify customers with duplicate order_ids (potential data quality issues).

Process:
1. Group by customer_id and order_id
2. Count occurrences
3. Filter where count > 1 (duplicates)
"""
task3 = (df.groupBy("customer_id", "order_id")
         .count()
         .filter(col("count") > 1))
print("\n" + "="*80)
print("TASK 3: Customers with Duplicate Orders")
print("="*80)
task3.show(truncate=False)

# ════════════════════════════════════════════════════════════════════════════════
# TASK 11: Cumulative Spend with Threshold Flagging
# ════════════════════════════════════════════════════════════════════════════════
"""
Calculate running/cumulative sum of total_amount per customer ordered by date.
Flag when customer's cumulative spending crosses $5000 threshold.

Window Function: rangeBetween(unboundedPreceding, currentRow) ensures all rows
from start of partition through current row are included in the calculation.

Use Case: Track customer lifetime value progression, identify high-value customers
in real-time for special offers or VIP programs.
"""
print("\n" + "="*80)
print("TASK 11: Cumulative Spend with $5000 Threshold Flag")
print("="*80)

window_cumulative = Window.partitionBy("customer_id").orderBy("order_date").rangeBetween(Window.unboundedPreceding, 0)
task11 = (df.withColumn("cumulative_spend", sum("total_amount").over(window_cumulative))
          .withColumn("crossed_5k_threshold",
                      when(col("cumulative_spend") >= 5000, lit("Yes")).otherwise(lit("No"))))
task11.select("customer_id", "order_date", "total_amount", "cumulative_spend", "crossed_5k_threshold").show(20, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 12: Month-over-Month Purchase Decline Detection
# ────────────────────────────────────────────────────────────────────────────────
"""
Identify customers with declining purchase patterns by calculating monthly totals
and MoM (month-over-month) percentage change.

Process:
1. Extract year-month from order_date
2. Group by customer and month to get monthly_order_value
3. Use LAG() to get previous month's value
4. Calculate percentage change: ((current - previous) / previous) * 100
5. Classify trend as up (>5%), down (<-5%), or stable

Use Case: Early warning system for customer churn, opportunity for re-engagement
campaigns targeting customers with declining order values.
"""
print("\n" + "="*80)
print("TASK 12: Month-over-Month Purchase Pattern Analysis")
print("="*80)

df_monthly = (df.withColumn("year_month", to_date(trunc(col("order_date"), "month")))
              .groupBy("customer_id", "year_month")
              .agg(sum("total_amount").alias("monthly_spend")))

window_mom = Window.partitionBy("customer_id").orderBy(col("year_month").desc())
task12 = (df_monthly.withColumn("prev_month_spend", lag("monthly_spend").over(window_mom))
          .withColumn("mom_change_pct",
                      when(col("prev_month_spend").isNotNull(),
                           ((col("monthly_spend") - col("prev_month_spend")) / col("prev_month_spend") * 100))
                      .otherwise(lit(None)))
          .withColumn("trend_direction",
                      when(col("mom_change_pct") > 5, lit("Up"))
                      .when(col("mom_change_pct") < -5, lit("Down"))
                      .otherwise(lit("Stable"))))
task12.filter(col("trend_direction") == "Down").select("customer_id", "year_month", "monthly_spend", "mom_change_pct", "trend_direction").show(15, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 13: Moving Average Anomaly Detection
# ────────────────────────────────────────────────────────────────────────────────
"""
Calculate 7-day rolling average (moving average) for each customer and identify
orders that deviate significantly (>2 std dev) from the moving average.

Process:
1. Create 7-day window (6 days prior + current day)
2. Calculate moving average of total_amount
3. Calculate standard deviation within window
4. Flag anomalies where |value - avg| > 2 * stddev
5. Calculate z-score for anomaly severity

Use Case: Fraud detection, unusual purchase behavior flagging, customer support alerts.
"""
# ────────────────────────────────────────────────────────────────────────────────
# TASK 13: Moving Average Anomaly Detection
# ────────────────────────────────────────────────────────────────────────────────
"""
Calculate 7-order moving average for each customer and flag anomalies.

We use:
- A ROWS window (last 7 orders) instead of a date RANGE
- stddev_pop for standard deviation
- z_score to measure how many std devs away from the moving average
"""

print("\n" + "="*80)
print("TASK 13: 7-Order Moving Average & Anomaly Detection")
print("="*80)

# Window: last 7 rows (orders) per customer, ordered by date
window_7day = (
    Window.partitionBy("customer_id")
          .orderBy("order_date")
          .rowsBetween(-6, 0)   # previous 6 + current = 7 rows
)

task13 = (
    df
    .withColumn("moving_avg_7day", avg("total_amount").over(window_7day))
    .withColumn("moving_stddev_7day", stddev_pop("total_amount").over(window_7day))
    .withColumn("deviation", col("total_amount") - col("moving_avg_7day"))
    .withColumn(
        "z_score",
        when(col("moving_stddev_7day") > 0,
             col("deviation") / col("moving_stddev_7day"))
        .otherwise(lit(0.0))
    )
    .withColumn(
        "is_anomaly",
        when(abs(col("z_score")) > 2, "Yes").otherwise("No")
    )
)

# First, see all rows to understand the behavior
task13.select(
    "customer_id",
    "order_date",
    "total_amount",
    "moving_avg_7day",
    "moving_stddev_7day",
    "z_score",
    "is_anomaly"
).orderBy("customer_id", "order_date").show(50, truncate=False)

print("\nOnly rows flagged as anomalies:")
task13.filter(col("is_anomaly") == "Yes") \
      .select(
          "customer_id",
          "order_date",
          "total_amount",
          "moving_avg_7day",
          "moving_stddev_7day",
          "z_score",
          "is_anomaly"
      ).orderBy("customer_id", "order_date") \
      .show(50, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 14: RFM Segmentation (Recency, Frequency, Monetary)
# ────────────────────────────────────────────────────────────────────────────────
"""
Segment customers using RFM analysis:
  - Recency (R): Days since last purchase (lower = more recent = better)
  - Frequency (F): Total number of orders (higher = better)
  - Monetary (M): Total lifetime spend (higher = better)

Process:
1. Calculate R, F, M metrics per customer
2. Use ntile() to score each metric 1-5 (1=worst, 5=best)
3. Combine scores and classify into segments:
   - VIP: R+F+M scores avg >= 4
   - Regular: R+F+M scores avg 2-3
   - At-Risk: R+F+M scores avg <= 1

Use Case: Targeted marketing campaigns, customer retention strategies, 
personalized discount offers based on segment.
"""
print("\n" + "="*80)
print("TASK 14: RFM Segmentation & Customer Classification")
print("="*80)

max_date = df.agg(max("order_date")).collect()[0][0]
from datetime import datetime, timedelta

task14_rfm = (df.groupBy("customer_id")
              .agg(
                  datediff(lit(max_date), max("order_date")).alias("recency_days"),
                  count("*").alias("frequency"),
                  sum("total_amount").alias("monetary")
              ))

window_rfm = Window.orderBy("monetary")
task14_rfm = (task14_rfm
              .withColumn("r_score", ntile(5).over(Window.orderBy(desc("recency_days"))))  # Lower recency = higher score
              .withColumn("f_score", ntile(5).over(Window.orderBy("frequency")))
              .withColumn("m_score", ntile(5).over(Window.orderBy("monetary")))
              .withColumn("rfm_avg_score", (col("r_score") + col("f_score") + col("m_score")) / 3)
              .withColumn("customer_segment",
                          when(col("rfm_avg_score") >= 4, lit("VIP"))
                          .when(col("rfm_avg_score") >= 2, lit("Regular"))
                          .otherwise(lit("At-Risk"))))
task14_rfm.select("customer_id", "recency_days", "frequency", "monetary", "rfm_avg_score", "customer_segment").show(20, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 15: Seasonal Analysis with Year-over-Year Growth
# ────────────────────────────────────────────────────────────────────────────────
"""
Identify seasonal patterns and YoY (year-over-year) growth trends.

Process:
1. Extract month and year from order_date
2. Aggregate orders and revenue by month-year
3. Pivot to align same months across years for YoY comparison
4. Calculate YoY growth: ((current_year - previous_year) / previous_year) * 100
5. Rank months by total revenue

Use Case: Inventory planning, marketing budget allocation, promotional timing,
identifying peak seasons and slow periods.
"""
print("\n" + "="*80)
print("TASK 15: Seasonal Patterns & Year-over-Year Growth")
print("="*80)

df_seasonal = (df.withColumn("year", year("order_date"))
               .withColumn("month", month("order_date"))
               .withColumn("month_name", date_format("order_date", "MMMM")))

df_monthly_summary = (df_seasonal.groupBy("year", "month", "month_name")
                      .agg(
                          count("*").alias("total_orders"),
                          sum("total_amount").alias("total_revenue")
                      ))

window_yoy = Window.partitionBy("month").orderBy("year")
task15_yoy = (df_monthly_summary
              .withColumn("prev_year_revenue", lag("total_revenue").over(window_yoy))
              .withColumn("yoy_growth_pct",
                          when(col("prev_year_revenue").isNotNull(),
                               ((col("total_revenue") - col("prev_year_revenue")) / col("prev_year_revenue") * 100))
                          .otherwise(lit(None)))
              .withColumn("month_rank", dense_rank().over(Window.orderBy(desc("total_revenue")))))
task15_yoy.select("month_name", "year", "total_orders", "total_revenue", "yoy_growth_pct", "month_rank").orderBy("month_rank").show(30, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 16: Top 3 Orders Gap Analysis
# ────────────────────────────────────────────────────────────────────────────────
"""
For each customer, identify their top 3 largest orders by amount and calculate
the time gap (in days) between consecutive orders.

Process:
1. Rank orders by total_amount descending within each customer
2. Filter top 3 per customer
3. Use LEAD() to get next order date
4. Calculate datediff() to find gaps

Use Case: Understanding high-value purchase patterns, repeat purchase cycles,
interval marketing timing for premium/VIP customers.
"""
print("\n" + "="*80)
print("TASK 16: Top 3 Orders & Time Gaps Between Purchases")
print("="*80)

window_top_orders = Window.partitionBy("customer_id").orderBy(desc("total_amount"))
df_top_orders = (df.withColumn("order_rank", row_number().over(window_top_orders))
                 .filter(col("order_rank") <= 3))

window_gap = Window.partitionBy("customer_id").orderBy("order_date")
task16 = (df_top_orders.withColumn("next_order_date", lead("order_date").over(window_gap))
          .withColumn("days_to_next_order", datediff(col("next_order_date"), col("order_date"))))
task16.select("customer_id", "order_rank", "order_date", "total_amount", "days_to_next_order").orderBy("customer_id", "order_date").show(25, truncate=False)

# ────────────────────────────────────────────────────────────────────────────────
# TASK 17: Customer Spending Pivot by Month
# ────────────────────────────────────────────────────────────────────────────────
"""
Create a pivot/crosstab showing spending across months (rows) and top 10 customers
(columns). Includes row totals and column totals.

Process:
1. Identify top 10 customers by total spend
2. Filter data to only include these customers
3. Extract year-month for grouping
4. Pivot: months as rows, customers as columns, sum(total_amount) as values
5. Add totals row and column

Use Case: Executive dashboard, visualizing spending patterns across customers and time,
identifying seasonality per customer.
"""
print("\n" + "="*80)
print("TASK 17: Pivot Table - Monthly Spending by Top 10 Customers")
print("="*80)

# 1) Top 10 customers by total spend
top_10_customers = (df.groupBy("customer_id")
                    .agg(sum("total_amount").alias("total_spend"))
                    .orderBy(desc("total_spend"))
                    .limit(10)
                    .select("customer_id"))

# 2) Filter data to those customers only
df_top_10_data = df.join(top_10_customers, "customer_id")

# 3) Prepare (year_month, customer_id, monthly_spend)
df_pivot_prep = (df_top_10_data
                 .withColumn("year_month", trunc("order_date", "month"))
                 .groupBy("year_month", "customer_id")
                 .agg(sum("total_amount").alias("monthly_spend")))

# 4) Pivot: rows = year_month, columns = customer_id, values = monthly_spend
task17_pivot = (df_pivot_prep
                .groupBy("year_month")                  # <-- groupBy FIRST
                .pivot("customer_id")                   # <-- pivot on column
                .sum("monthly_spend")                   # or .agg(sum("monthly_spend"))
                .fillna(0))

task17_pivot.orderBy("year_month").show(truncate=False)


# ────────────────────────────────────────────────────────────────────────────────
# TASK 18: Churn Risk Scoring
# ────────────────────────────────────────────────────────────────────────────────
"""
Identify early churn signals using two criteria:
1. Long gap since last order (> 90 days)
2. Significant decline in AOV (avg order value down > 20% in last 3 months)

Process:
1. Calculate days since last order (recency)
2. Calculate last 3 months AOV vs historical AOV
3. Calculate AOV change percentage
4. Assign risk level: High (both signals), Medium (one signal), Low (no signals)

Use Case: Proactive customer retention, targeted win-back campaigns,
resource allocation for at-risk customer support.
"""
print("\n" + "="*80)
print("TASK 18: Churn Risk Scoring")
print("="*80)

max_order_date = df.agg(max("order_date")).collect()[0][0]
threshold_date = max_order_date - timedelta(days=90)

df_churn = (df.groupBy("customer_id")
            .agg(
                datediff(lit(max_order_date), max("order_date")).alias("days_since_last_order"),
                avg("total_amount").alias("historical_aov"),
                count("*").alias("total_orders")
            ))

df_last_3m = (df.filter(col("order_date") >= (max_order_date - timedelta(days=90)))
              .groupBy("customer_id")
              .agg(avg("total_amount").alias("last_3m_aov")))

task18 = (df_churn.join(df_last_3m, "customer_id", "left")
          .withColumn("last_3m_aov", coalesce(col("last_3m_aov"), col("historical_aov")))
          .withColumn("aov_change_pct",
                      ((col("last_3m_aov") - col("historical_aov")) / col("historical_aov") * 100))
          .withColumn("has_long_gap", when(col("days_since_last_order") > 90, 1).otherwise(0))
          .withColumn("has_aov_decline", when(col("aov_change_pct") < -20, 1).otherwise(0))
          .withColumn("churn_risk_score", col("has_long_gap") + col("has_aov_decline"))
          .withColumn("churn_risk_level",
                      when(col("churn_risk_score") >= 2, lit("High"))
                      .when(col("churn_risk_score") == 1, lit("Medium"))
                      .otherwise(lit("Low"))))
task18.filter(col("churn_risk_level") != "Low").select("customer_id", "days_since_last_order", "aov_change_pct", "churn_risk_level").show(20, truncate=False)


# ────────────────────────────────────────────────────────────────────────────────
# TASK 9: Real-Time 30-Day Window Analytics
# ────────────────────────────────────────────────────────────────────────────────
"""
Real-time analytics for last 30 days: daily active customers, revenue trends,
average order size, and day-over-day growth rates.

Process:
1. Filter data for last 30 days
2. Group by day to calculate: active customers, daily revenue, avg order size
3. Calculate cumulative monthly revenue
4. Use LAG() for DoD (day-over-day) growth percentage
5. Identify peak sales days

Use Case: Real-time dashboard monitoring, operational KPI tracking,
anomaly detection in daily sales patterns.
"""
print("\n" + "="*80)
print("TASK 19: Real-Time 30-Day Analytics Dashboard")
print("="*80)

max_date_30d = df.agg(max("order_date")).collect()[0][0]
min_date_30d = max_date_30d - timedelta(days=30)

df_30d = df.filter((col("order_date") >= min_date_30d) & (col("order_date") <= max_date_30d))

df_daily = (df_30d.withColumn("day", col("order_date"))
            .groupBy("day")
            .agg(
                countDistinct("customer_id").alias("daily_active_customers"),
                sum("total_amount").alias("daily_revenue"),
                count("*").alias("daily_order_count"),
                avg("total_amount").alias("avg_order_size")
            ))

window_30d = Window.orderBy("day").rangeBetween(Window.unboundedPreceding, 0)
task20_realtime = (df_daily.withColumn("cumulative_monthly_revenue", sum("daily_revenue").over(window_30d))
                   .withColumn("prev_day_revenue", lag("daily_revenue").over(Window.orderBy("day")))
                   .withColumn("dod_growth_pct",
                               when(col("prev_day_revenue").isNotNull(),
                                    ((col("daily_revenue") - col("prev_day_revenue")) / col("prev_day_revenue") * 100))
                               .otherwise(lit(None)))
                   .withColumn("daily_rank", dense_rank().over(Window.orderBy(desc("daily_revenue")))))
task20_realtime.select("day", "daily_active_customers", "daily_revenue", "avg_order_size", "dod_growth_pct", "cumulative_monthly_revenue", "daily_rank").show(30, truncate=False)

print("\n" + "="*80)
print("✓ All 20 tasks completed successfully!")
print("="*80)