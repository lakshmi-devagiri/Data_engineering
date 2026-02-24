from pyspark.sql import SparkSession, functions as F, types as T
import re

spark = (SparkSession.builder.appName("data_cleaning_examples").master("local[*]").getOrCreate())
#learning time ur doing like this but in office ur doing like this (after 200 lines pls check)

# ──────────────────────────────────────────────────────────────────────────────
# 1) Sample messy data (4 rows)
# ──────────────────────────────────────────────────────────────────────────────
data=r"D:\bigdata\drivers\Data_Cleaning_purpose.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data)

# ──────────────────────────────────────────────────────────────────────────────
# 2) Rename columns (snake_case-preserving concise sanitization)
# ──────────────────────────────────────────────────────────────────────────────

# Convert column names to snake_case-ish identifiers:
# - collapse whitespace to underscore
# - allow underscores, letters and digits
# - remove other punctuation
# - lowercase
# - replace empty resulting names with 'col'
# - ensure deterministic uniqueness
cols = []
for c in df.columns:
    if c is None:
        s = 'col'
    else:
        # collapse whitespace to single underscore, strip edges
        s = re.sub(r"\s+", "_", c.strip())
        # keep letters, digits and underscores only
        s = re.sub(r"[^0-9A-Za-z_]", "", s)
        s = s.strip("_")
        s = s.lower()
        if s == '':
            s = 'col'
    cols.append(s)

# Deduplicate deterministically by appending suffixes when collisions happen
seen = {}
final_cols = []
for name in cols:
    if name in seen:
        seen[name] += 1
        final_cols.append(f"{name}_{seen[name]}")
    else:
        seen[name] = 0
        final_cols.append(name)

# Apply all renames in one call (preferred for performance)
df = df.toDF(*final_cols)

# Now columns should be snake_case-like and match later references (e.g. full_name)

# ──────────────────────────────────────────────────────────────────────────────
# 3) Basic trims & normalizations
# ──────────────────────────────────────────────────────────────────────────────
df = (df
      # strip outer spaces
      .withColumn("full_name", F.trim("full_name"))
      .withColumn("email", F.trim(F.lower("email")))
      .withColumn("city", F.trim("city"))
      .withColumn("title", F.trim("title"))

      # collapse multiple inner spaces in names, titles: convert 2+ spaces -> single
      .withColumn("full_name", F.regexp_replace("full_name", r"\s{2,}", " "))
      .withColumn("title", F.regexp_replace("title", r"\s{2,}", " "))

      # Title case city (simple): lower + capitalize first letter
      .withColumn("city", F.concat(F.upper(F.substring("city", 1, 1)),
                                   F.lower(F.substring("city", 2, 1000))))
     )

# ──────────────────────────────────────────────────────────────────────────────
# 4) Email: validate, extract domain
# ──────────────────────────────────────────────────────────────────────────────
email_pat = r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"
df = (df
      .withColumn("email_is_valid", F.col("email").rlike(email_pat))
      .withColumn("email_domain", F.regexp_extract("email", r"@([a-z0-9.\-]+\.[a-z]{2,})$", 1))
     )

# ──────────────────────────────────────────────────────────────────────────────
# 5) Phone: keep digits, normalize to +91XXXXXXXXXX if 10/12 digits present
# ──────────────────────────────────────────────────────────────────────────────
digits = F.regexp_replace(F.col("phone"), r"\D", "")  # remove non-digits

df = (df
      .withColumn("phone_digits", digits)
      # Normalize:
      # - If 10 digits: assume India -> +91{10}
      # - If starts with '91' + next 10: -> +91{10}
      # - Else leave as-is (store with '+' if it already had)
      .withColumn(
          "phone_norm",
          F.when(F.length("phone_digits") == 10,
                 F.concat(F.lit("+91"), F.col("phone_digits")))
           .when((F.length("phone_digits") == 12) & (F.col("phone_digits").substr(1,2) == '91'),
                 F.concat(F.lit("+"), F.substring("phone_digits", 1, 2), F.substring("phone_digits", 3, 10)))
           .otherwise(F.col("phone"))
      )
     )

# ──────────────────────────────────────────────────────────────────────────────
# 6) Dates: parse multiple formats into a single date column
#    Try YYYY/MM/DD, DD-MM-YYYY, MM-DD-YYYY, YYYY.MM.DD in order
# ──────────────────────────────────────────────────────────────────────────────
d1 = F.to_date("joined_date_str", "yyyy/MM/dd")
d2 = F.to_date("joined_date_str", "dd-MM-yyyy")
d3 = F.to_date("joined_date_str", "MM-dd-yyyy")
d4 = F.to_date("joined_date_str", "yyyy.MM.dd")

df = df.withColumn("joined_date",
                   F.coalesce(d1, d2, d3, d4))

# ──────────────────────────────────────────────────────────────────────────────
# 7) Salary: strip currency symbols/commas and cast to integer
# ──────────────────────────────────────────────────────────────────────────────
df = (df
      .withColumn("salary_clean",
                  F.regexp_replace("salary_str", r"[^\d]", ""))  # keep only digits
      .withColumn("salary_int",
                  F.col("salary_clean").cast(T.IntegerType()))
     )

# ──────────────────────────────────────────────────────────────────────────────
# 8) Name split with regexp_extract (first / last)
# ──────────────────────────────────────────────────────────────────────────────
# Pattern: first token, last token (handles 2+ names loosely)
df = (df
      .withColumn("first_name", F.regexp_extract("full_name", r"^\s*([^\s]+)", 1))
      .withColumn("last_name",  F.regexp_extract("full_name", r"([^\s]+)\s*$", 1))
     )

# ──────────────────────────────────────────────────────────────────────────────
# 9) Rule-based flags using when/otherwise
# ──────────────────────────────────────────────────────────────────────────────
df = (df
      # invalid email or no domain
      .withColumn("needs_email_fix",
                  F.when(~F.col("email_is_valid") | (F.col("email_domain") == ""), F.lit(True)).otherwise(F.lit(False)))
      # phone sanity check: should start with +91 and be 13 chars (+91 + 10 digits)
      .withColumn("needs_phone_fix",
                  F.when(~F.col("phone_norm").rlike(r"^\+91\d{10}$"), F.lit(True)).otherwise(F.lit(False)))
     )

# ──────────────────────────────────────────────────────────────────────────────
# 10) Show result (selected columns)
# ──────────────────────────────────────────────────────────────────────────────
out_cols = [
    "full_name", "first_name", "last_name",
    "email", "email_is_valid", "email_domain",
    "phone", "phone_norm",
    "city",
    "joined_date_str", "joined_date",
    "salary_str", "salary_int",
    "title",
    "needs_email_fix", "needs_phone_fix"
]

print("\n=== CLEANED DATA ===")
df.select(*out_cols).show(truncate=False)

# -----------------------------------------------------------------------------
# TASKS (start time and sample output)
# -----------------------------------------------------------------------------
# Start time: 2025-10-25 09:00 IST
#
# This script is organized as numbered tasks you can run step-by-step.
# Sample high-level task list:
#  1) task_1_load(path)           -> load raw CSV into a DataFrame
#  2) task_2_sanitize_columns(df) -> sanitize header names to snake_case
#  3) task_3_basic_clean(df)      -> trim, normalize whitespace, title-case city
#  4) task_4_email_validation(df) -> validate email, extract domain
#  5) task_5_phone_normalize(df)  -> extract digits and normalize phone formats
#  6) task_6_parse_dates_salary(df)-> parse joined_date and clean salary
#  7) task_7_split_names(df)      -> first_name, last_name
#  8) task_8_rule_flags(df)       -> rule-based validation flags
#  9) task_9_show_result(df)      -> print final selected output
#
# Example sample output (approximation of df.select(...).show() after all tasks):
# +------------------+----------+---------+----------------------+-----------+-------------+
# |full_name         |first_name|last_name|email                 |phone_norm |salary_int   |
# +------------------+----------+---------+----------------------+-----------+-------------+
# |Venu Katragadda   |Venu      |Katragadda|venu_k@example.com   |+919901788833|120000      |
# |Asha Kumar        |Asha      |Kumar    |asha.kumar@ex-ample.net|+914012345678|90000     |
# +------------------+----------+---------+----------------------+-----------+-------------+
#
# Run tasks from __main__ or import functions and call them from tests.
# -----------------------------------------------------------------------------

def task_1_load(path: str):
    """Task 1: Load raw CSV into Spark DataFrame.

    Args:
        path: local file path or URI to CSV file.
    Returns:
        Spark DataFrame with raw columns as read from CSV.
    """
    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(path)
    return df_raw


def task_2_sanitize_columns(df):
    """Task 2: Sanitize column names to snake_case-like identifiers.

    - collapse whitespace to underscore
    - remove punctuation (keep letters/digits/underscore)
    - lowercase
    - ensure deterministic uniqueness
    """
    cols = []
    for c in df.columns:
        if c is None:
            s = 'col'
        else:
            s = re.sub(r"\s+", "_", c.strip())
            s = re.sub(r"[^0-9A-Za-z_]", "", s)
            s = s.strip("_")
            s = s.lower()
            if s == '':
                s = 'col'
        cols.append(s)

    seen = {}
    final_cols = []
    for name in cols:
        if name in seen:
            seen[name] += 1
            final_cols.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            final_cols.append(name)

    return df.toDF(*final_cols)


def task_3_basic_clean(df):
    """Task 3: Basic string trimming and normalization."""
    return (df
            # strip outer spaces
            .withColumn("full_name", F.trim("full_name"))
            .withColumn("email", F.trim(F.lower("email")))
            .withColumn("city", F.trim("city"))
            .withColumn("title", F.trim("title"))

            # collapse multiple inner spaces in names, titles: convert 2+ spaces -> single
            .withColumn("full_name", F.regexp_replace("full_name", r"\s{2,}", " "))
            .withColumn("title", F.regexp_replace("title", r"\s{2,}", " "))

            # Title case city (simple): lower + capitalize first letter
            .withColumn("city", F.concat(F.upper(F.substring("city", 1, 1)),
                                         F.lower(F.substring("city", 2, 1000))))
           )


def task_4_email_validation(df):
    """Task 4: Add email_is_valid boolean and email_domain extraction."""
    email_pat = r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"
    return (df
            .withColumn("email_is_valid", F.col("email").rlike(email_pat))
            .withColumn("email_domain", F.regexp_extract("email", r"@([a-z0-9.\-]+\.[a-z]{2,})$", 1))
           )


def task_5_phone_normalize(df):
    """Task 5: Normalize phone column to a canonical phone_norm and keep digits."""
    digits = F.regexp_replace(F.col("phone"), r"\D", "")
    return (df
            .withColumn("phone_digits", digits)
            .withColumn(
                "phone_norm",
                F.when(F.length("phone_digits") == 10, F.concat(F.lit("+91"), F.col("phone_digits")))
                 .when((F.length("phone_digits") == 12) & (F.col("phone_digits").substr(1,2) == '91'),
                       F.concat(F.lit("+"), F.substring("phone_digits", 1, 2), F.substring("phone_digits", 3, 10)))
                 .otherwise(F.col("phone"))
            )
           )


def task_6_parse_dates_salary(df):
    """Task 6: Parse joined_date from several formats and clean salary to integer."""
    d1 = F.to_date("joined_date_str", "yyyy/MM/dd")
    d2 = F.to_date("joined_date_str", "dd-MM-yyyy")
    d3 = F.to_date("joined_date_str", "MM-dd-yyyy")
    d4 = F.to_date("joined_date_str", "yyyy.MM.dd")

    return (df
            .withColumn("joined_date", F.coalesce(d1, d2, d3, d4))
            .withColumn("salary_clean", F.regexp_replace("salary_str", r"[^\d]", ""))
            .withColumn("salary_int", F.col("salary_clean").cast(T.IntegerType()))
           )


def task_7_split_names(df):
    """Task 7: Extract first_name and last_name with regexp_extract."""
    return (df
            .withColumn("first_name", F.regexp_extract("full_name", r"^\s*([^\s]+)", 1))
            .withColumn("last_name", F.regexp_extract("full_name", r"([^\s]+)\s*$", 1))
           )


def task_8_rule_flags(df):
    """Task 8: Compute rule-based flags for email/phone sanity."""
    return (df
            .withColumn("needs_email_fix", F.when(~F.col("email_is_valid") | (F.col("email_domain") == ""), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("needs_phone_fix", F.when(~F.col("phone_norm").rlike(r"^\+91\d{10}$"), F.lit(True)).otherwise(F.lit(False)))
           )


def task_9_show_result(df):
    """Task 9: Select the output columns and show."""
    out_cols = [
        "full_name", "first_name", "last_name",
        "email", "email_is_valid", "email_domain",
        "phone", "phone_norm",
        "city",
        "joined_date_str", "joined_date",
        "salary_str", "salary_int",
        "title",
        "needs_email_fix", "needs_phone_fix"
    ]
    print("\n=== CLEANED DATA ===")
    df.select(*out_cols).show(truncate=False)


if __name__ == '__main__':
    csv_path = r"D:\bigdata\drivers\Data_Cleaning_purpose.csv"
    df = task_1_load(csv_path)
    df = task_2_sanitize_columns(df)
    df = task_3_basic_clean(df)
    df = task_4_email_validation(df)
    df = task_5_phone_normalize(df)
    df = task_6_parse_dates_salary(df)
    df = task_7_split_names(df)
    df = task_8_rule_flags(df)
    task_9_show_result(df)

    # Stop Spark (optional when running in notebooks/REPL)
    spark.stop()
