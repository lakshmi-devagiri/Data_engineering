from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.appName("csvdatapoc").master("local").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
data=r"C:\bigdata\drivers\10000Records.csv"
df=spark.read.option("header","true").option("inferSchema","true").csv(data)
df=df.withColumn("full_name", concat_ws(" ", col("Name Prefix"), col("First Name"), col("Middle Initial"),col("Last Name")).alias("full_name")).drop("Name Prefix","Middle Initial","Last Name","First Name")
cols=[ re.sub('[^a-zA-Z0-9]','',n) for n in df.columns]
#clean header/schema/columns
print(cols)
#df=df.withColumnRenamed("Emp ID","emp_id").withColumnRenamed("E Mail","email")
#if u want to rename only 1 column use this withColumnRenamed
#i want to rename all columns us df.toDF(*list_of_columns)
df=df.toDF(*cols)
df=df.na.fill(0)
#df=df.withColumn("DateofBirth", to_date(col("DateOfBirth"),"M/d/yyyy"))
# Define common date regex patterns

# Define possible date formats
possible_formats = [
    # --- Common numeric formats ---
    "yyyy-MM-dd",
    "dd-MM-yyyy",
    "MM-dd-yyyy",
    "yyyy/MM/dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "yyyy.MM.dd",
    "dd.MM.yyyy",

    # --- With single-digit day/month (no leading zero) ---
    "d-M-yyyy",
    "d/M/yyyy",
    "yyyy-M-d",
    "yyyy/M/d",

    # --- Text month (short / full) ---
    "dd-MMM-yyyy",   # 13-Nov-2025
    "dd-MMMM-yyyy",  # 13-November-2025
    "d-MMM-yyyy",
    "d-MMMM-yyyy",
    "yyyy-MMM-dd",
    "yyyy-MMMM-dd",

    # --- With slash and text month ---
    "dd/MMM/yyyy",
    "dd/MMMM/yyyy",
    "d/M/yyyy","M/d/yyyy",
    "d/MMMM/yyyy",
    "yyyy/MMM/dd",
    "yyyy/MMMM/dd",

    # --- Alternate separators ---
    "dd_MM_yyyy",
    "yyyy_MM_dd",
    "dd MMM yyyy",   # with space separator (e.g., 13 Nov 2025)
    "d MMM yyyy",
    "yyyy MMM dd",

    # --- Compact or mixed styles ---
    "ddMMyyyy",
    "yyyyMMdd",
    "ddMMMyyyy",
    "yyyyMMMd"
]

# Define helper to convert using multiple formats
def dateto(col_expr, formats=possible_formats):
    return coalesce(*[to_date(col_expr, f) for f in formats])

# Define regex pattern to detect date-like strings
date_regex = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}")

def convert_all_date_columns(df):
    for cname, ctype in df.dtypes:
        if ctype == "string":
            # Sample one value to test pattern
            sample = df.select(cname).where(F.col(cname).isNotNull()).limit(1).collect()
            if sample:
                val = str(sample[0][0])
                if re.match(date_regex, val):  # column looks like date
                    df = df.withColumn(cname, dateto(col(cname)))
    return df

# ✅ Example usage
df = convert_all_date_columns(df)
df.printSchema()
df.show()