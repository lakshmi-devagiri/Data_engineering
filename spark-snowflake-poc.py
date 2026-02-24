from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os, re
import csv

# ── Spark ──────────────────────────────────────────────────────────────────────
# Initialize a Spark session for processing CSV files and interacting with Snowflake
spark = (SparkSession.builder
         .appName("csv_auto_delim_to_snowflake")
         .master("local[*]")
         .getOrCreate())

# Snowflake connection options
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ✅ Ensure all date/timestamp operations use India timezone
spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

sfOptions = {
    "sfURL"      : "myyvgan-xl78278.snowflakecomputing.com",  # Snowflake account URL
    "sfUser"     : "INTURIBHAVYA1",                          # Snowflake username
    "sfPassword" : "January-01-2000",                        # Snowflake password (consider securing this)
    "sfDatabase" : "venudb",                                 # Target database
    "sfSchema"   : "public",                                 # Target schema
    "sfWarehouse": "COMPUTE_WH",                             # Target warehouse
    "sfRole"     : "ACCOUNTADMIN"                            # Role for Snowflake access
}

from pyspark.sql import functions as F, types as T
import re

# ---- Patterns ----
# Regular expressions for detecting Aadhaar, PAN, and email patterns
AADHAAR_DIGITS_RE = re.compile(r"^\D*?(\d\D*){12}$")   # Matches 12 digits anywhere, allowing spaces/dashes
PAN_RE            = re.compile(r"^[A-Z]{5}\d{4}[A-Z]$", re.I)  # Matches valid PAN card format
EMAIL_RE          = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")  # Matches valid email format

# Column name hints for identifying sensitive data
NAME_HINTS = {
    "aadhaar": re.compile(r"aadh?aa?r", re.I),  # Matches variations of "Aadhaar"
    "pan":     re.compile(r"\bpan(card)?\b", re.I),  # Matches "PAN" or "PAN card"
    "email":   re.compile(r"e-?mail|mail_id|mail", re.I)  # Matches variations of "email"
}

# ---- Maskers (UDFs) ----
@F.udf("string")
def mask_email(s):
    """
    Masks email addresses by keeping the first and last character of the local part.
    """
    if not s:
        return s
    txt = str(s).strip()
    if not EMAIL_RE.match(txt):
        return txt  # Leave non-email as-is
    local, _, domain = txt.partition("@")
    keep = local[:2] if len(local) >= 2 else local[:1]
    return f"{keep}{'*' * 6}@{domain}"

@F.udf("string")
def mask_aadhaar(s):
    """
    Masks Aadhaar numbers by replacing all but the last 4 digits with 'XXXX-XXXX-'.
    """
    if not s:
        return s
    txt = re.sub(r"\D", "", str(s))  # Keep only digits
    if len(txt) != 12:
        return str(s)  # Not Aadhaar -> leave
    return f"XXXX-XXXX-{txt[-4:]}"  # Show only last 4 digits

@F.udf("string")
def mask_pan(s):
    """
    Masks PAN numbers by keeping the first 3 and last 1 characters, replacing the middle with asterisks.
    """
    if not s:
        return s
    txt = str(s).strip().upper()
    if not PAN_RE.match(txt):
        return str(s)  # Not PAN -> leave
    return f"{txt[:3]}*****{txt[-1:]}"  # Keep first 3 & last 1

def _sample_matches(df, col, regex, normalize_digits=False, n=50):
    """
    Returns the fraction of non-null sample values in a column that match a given regex.
    Optionally normalizes digits for Aadhaar detection.
    """
    vals = [r[col] for r in df.select(col).where(F.col(col).isNotNull()).limit(n).collect()]
    if not vals:
        return 0.0
    cnt = 0
    for v in vals:
        s = str(v)
        if normalize_digits:
            s2 = re.sub(r"\D", "", s)
            ok = len(s2) == 12  # Aadhaar: exactly 12 digits
        else:
            ok = bool(regex.match(s))
        cnt += 1 if ok else 0
    return cnt / max(1, len(vals))

def _column_hint(colname, kind):
    """
    Checks if a column name matches a predefined hint for Aadhaar, PAN, or email.
    """
    return bool(NAME_HINTS[kind].search(colname))

# ---------------- Regex Patterns ----------------
# Additional regex patterns for masking sensitive data
EMAIL_RE = re.compile(r'(^[^@]+)@(.+)$')  # Matches email addresses
PAN_RE = re.compile(r'\b[A-Z]{5}\d{4}[A-Z]\b', re.I)  # Matches PAN card format

# ---------------- UDF Definitions ----------------
def mask_email(value):
    """
    Masks email addresses by keeping the first and last character of the local part.
    """
    if not value:
        return value
    match = EMAIL_RE.match(value.strip())
    if match:
        local, domain = match.groups()
        if len(local) <= 2:
            masked_local = "*" * len(local)
        else:
            masked_local = local[0] + "*" * (len(local) - 2) + local[-1]
        return f"{masked_local}@{domain}"
    return value

def mask_pan(value):
    """
    Masks PAN numbers by keeping the first 3 and last 2 characters, replacing the middle with asterisks.
    """
    if not value:
        return value
    txt = value.strip().upper()
    if PAN_RE.match(txt):
        return txt[:3] + "*****" + txt[-2:]
    return value

mask_email_udf = F.udf(mask_email, StringType())  # Register email masking UDF
mask_pan_udf = F.udf(mask_pan, StringType())  # Register PAN masking UDF

# ---------------- Aadhaar Mask (using regexp_replace) ----------------
def mask_aadhaar_column(df, colname):
    """
    Masks Aadhaar numbers in a column by replacing all but the last 4 digits with 'XXXXXXXX'.
    """
    df = (
        df.withColumn(colname, F.regexp_replace(F.col(colname), "[- ]", ""))
          .withColumn(
              colname,
              F.concat(
                  F.lit("XXXXXXXX"),
                  F.substring(F.col(colname).cast("string"), -4, 4)
              )
          )
    )
    return df

# ---------------- Master Function ----------------
def mask_sensitive_columns(df):
    """
    Automatically masks Aadhaar, PAN, and Email columns based on data patterns or column names.
    """
    for c, t in df.dtypes:
        if t.lower() != "string":
            continue

        cname = c.lower()
        # Aadhaar name-based
        if "aadhar" in cname or "aadhaar" in cname:
            df = mask_aadhaar_column(df, c)
            print(f"🔒 Masked Aadhaar column: {c}")
            continue

        # Email name-based
        if "email" in cname or "mail" in cname:
            df = df.withColumn(c, mask_email_udf(F.col(c)))
            print(f"🔒 Masked Email column: {c}")
            continue

        # PAN name-based
        if "pan" in cname:
            df = df.withColumn(c, mask_pan_udf(F.col(c)))
            print(f"🔒 Masked PAN column: {c}")
            continue

        # Optional: pattern-based (in case column name is not obvious)
        # Detect Aadhaar-like pattern (12 digits)
        sample = df.select(F.col(c)).where(F.col(c).rlike(r'\d{12}')).limit(1).count()
        try:
            sample = (
                df.select(F.col(c).cast("string"))
                .where(F.col(c).cast("string").rlike(r'\d{12}'))
                .limit(1)
                .count()
            )

            if sample > 0:
                # Re-read the DataFrame as mask_aadhaar_column overwrites the column
                df = mask_aadhaar_column(df, c)
                print(f"🔒 Masked Aadhaar pattern in: {c}")

        except Exception as e:
            # Catch any unexpected Spark error during the RLIKE check
            # and continue processing other columns
            print(f"⚠️ Warning: Spark error during RLIKE check on column {c}. Skipping pattern mask. Error: {e}")
            continue

        return df

# Folder containing CSV files
folder_path = r"C:\bigdata\drivers"

# Optional: toggle writing into Snowflake
WRITE_TO_SNOWFLAKE = False  # Set to True to enable writing to Snowflake

# ── Helpers ────────────────────────────────────────────────────────────────────
# ✅ Delimiter Detection Function
CANDIDATE_DELIMS = [',', ';', '\t', '|', '^', '~']


def detect_delimiter(file_path: str):
    """
    Detects the delimiter and header presence in a CSV file using csv.Sniffer.
    Returns (delimiter, has_header).
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.read(128 * 1024)
    except Exception:
        return ',', True

    # 1. csv.Sniffer attempt (Preferred)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=''.join(CANDIDATE_DELIMS))
        has_header = csv.Sniffer().has_header(sample)
        return dialect.delimiter, has_header
    except Exception:
        pass  # Fall through to the heuristic if Sniffer fails

    # 2. Fallback Heuristic for Delimiter and Header detection

    lines = [ln for ln in sample.splitlines() if ln.strip()]
    lines = lines[:50]
    if len(lines) < 2:
        # If we have only one line, assume header if it contains text
        return ',', True if len(lines) == 1 and any(c.isalpha() for c in lines[0]) else True

        # --- Delimiter Detection ---
    best_delim = ','
    best_score = -1
    for d in CANDIDATE_DELIMS:
        counts = [ln.count(d) for ln in lines if len(ln) > 3]
        if not counts:
            continue
        mean_cnt = sum(counts) / len(counts)
        consistency = sum(1 for c in counts if c == round(mean_cnt))
        score = (mean_cnt * 2) + consistency
        if score > best_score:
            best_score = score
            best_delim = d

    # --- Header Detection (The corrected part) ---
    has_header = False

    try:
        fields_l1 = lines[0].split(best_delim)
        fields_l2 = lines[1].split(best_delim)
        f1_count = len(fields_l1)
        f2_count = len(fields_l2)

        # 1. Check if the number of fields differs (Classic check)
        if f1_count != f2_count:
            has_header = True

        # 2. Check text content disparity
        # Count how many fields in L1 and L2 contain at least one alphabetical character.
        l1_is_alpha_fields = sum(1 for f in fields_l1 if any(c.isalpha() for c in f.strip()))
        l2_is_alpha_fields = sum(1 for f in fields_l2 if any(c.isalpha() for c in f.strip()))

        # If the first line is significantly more "textual" than the second, assume header.
        # This handles cases like 'Team1,Team2,Winner' vs 'ind,pak,ind'.
        # Threshold: L1 must have at least 2 alpha fields, and significantly more than L2.
        if l1_is_alpha_fields > 1 and l1_is_alpha_fields >= f1_count * 0.75 and l1_is_alpha_fields > l2_is_alpha_fields:
            has_header = True

    except Exception:
        # If splitting fails for any reason, assume header (safer default)
        has_header = True

    return best_delim, has_header

def sanitize_table_name(name: str) -> str:
    """
    Sanitizes a file name to create a valid Snowflake table name.
    """
    base = os.path.splitext(os.path.basename(name))[0]
    s = re.sub(r'[^0-9A-Za-z_]', '_', base)
    if re.match(r'^\d', s):
        s = f"T_{s}"
    return s.upper()

def human_delim(d: str) -> str:
    """
    Converts a delimiter character to a human-readable name.
    """
    return {"\t": "TAB", ",": "COMMA", ";": "SEMICOLON", "|": "PIPE", "^": "CARET", "~": "TILDE"}.get(d, d)

# -------------------------------------------------------------------------
# ✅ Auto Date Conversion Function
# -------------------------------------------------------------------------

def auto_convert_dates(df):
    """
    Detects date-like columns and converts them to proper DateType or TimestampType.
    Supports mixed formats like:
    - 16/04/2025, 05/May/2025, 06012025, 20-Jan-2025, 20/Sep/2025, 14102025
    - 11/22/2023 05:57:23 AM, 2023-11-22T05:57:23, etc.
    """

    # Common possible date and datetime formats
    date_formats = [
        "dd/MM/yyyy", "dd-MMM-yyyy", "dd-MMM-yy", "dd/MMM/yyyy",
        "dd-MM-yyyy", "yyyy-MM-dd", "dd-MMM-yyyy", "dd/MMM/yy",
        "dd/MM/yyyy", "dd-MM-yy", "d-MMM-yyyy", "d/M/yyyy",
        "ddMMMyyyy", "ddMMyyyy", "ddMMyy","d-M-yyyy"
    ]

    datetime_formats = [
        "MM/dd/yyyy hh:mm:ss a", "MM/dd/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd-MM-yyyy HH:mm:ss"
    ]

    # Helper to normalize formats like 06012025 → 06/01/2025
    def normalize_compact_date(col):
        return F.when(
            F.col(col).rlike(r'^\d{8}$'),
            F.concat_ws('/', F.substring(col, 1, 2), F.substring(col, 3, 2), F.substring(col, 5, 4))
        ).otherwise(F.col(col))

    # Detect likely date or datetime columns
    for c, t in df.dtypes:
        if t.lower() != "string":
            continue

        # Check if column has at least one valid date-like or datetime-like value
        sample_count = df.filter(F.col(c).rlike(r'\d{1,2}[-/A-Za-z0-9]+202\d')).limit(1).count()
        if sample_count == 0:
            continue

        print(f"📅 Converting column to DateType or TimestampType: {c}")

        # Clean up and normalize 8-digit numeric dates
        df = df.withColumn(c, normalize_compact_date(c))

        # Try datetime formats first
        parsed_datetime = F.coalesce(*[F.to_timestamp(F.col(c), f) for f in datetime_formats])
        parsed_date = F.coalesce(*[F.to_date(F.col(c), f) for f in date_formats])

        # Use datetime if any value matches, otherwise fallback to date
        df = df.withColumn(c, F.when(parsed_datetime.isNotNull(), parsed_datetime.cast(TimestampType()))
                              .otherwise(parsed_date.cast(DateType())))

    return df
# ── Discover files ─────────────────────────────────────────────────────────────
csv_files = [os.path.join(folder_path, f)
             for f in os.listdir(folder_path)
             if f.lower().endswith(".csv")]

print("CSV Files Found:", csv_files)


# ── Process each file ─────────────────────────────────────────────────────────
for fpath in csv_files:
    tab = sanitize_table_name(fpath)
    delim, has_header = detect_delimiter(fpath)

    print("\n────────────────────────────────────────────────────────")
    print(f"Reading file        : {fpath}")
    print(f"Detected delimiter  : {repr(delim)} ({human_delim(delim)})")
    print(f"Detected header     : {has_header}")
    print(f"Target Snowflake tbl: {sfOptions['sfDatabase']}.{sfOptions['sfSchema']}.{tab}")

    # Read with detected delimiter
    df = (spark.read.format("csv")
          .option("header", str(has_header).lower())
          .option("delimiter", delim)
          .option("quote", '"')
          .option("escape", '"')
          .option("multiLine", "true")
          .option("inferSchema", "true")
          .load(fpath))

    # Make column names Snowflake-friendly
    safe_cols = [re.sub(r'[^0-9A-Za-z_]', '_', c).upper() for c in df.columns]
    df = df.toDF(*safe_cols)
    df = mask_sensitive_columns(df)  # Apply masking
    df = auto_convert_dates(df)

    df.show(5, truncate=False)

    if WRITE_TO_SNOWFLAKE:
        # Write to Snowflake
        (df.write
           .format(SNOWFLAKE_SOURCE_NAME)
           .options(**sfOptions)
           .option("dbtable", tab)
           .mode("overwrite")
           .save())
        print(f"Wrote to Snowflake table: {tab}")

print("\nDone.")