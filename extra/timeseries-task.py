from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()

# Databricks notebook source
#dbutils.widgets.text("suffix", "")
suffix=""
#dbutils.widgets.text("number_of_months_to_process", "")
number_of_months_to_process=""
#dbutils.widgets.text("month_to_process", "")
month_to_process=int("")

# COMMAND ----------

'''suffix = getArgument("suffix")
number_of_months_to_process = getArgument("number_of_months_to_process")
month_to_process = int(getArgument("month_to_process"))'''

database = f"entegra_supplier_central{suffix}"

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta
from delta.tables import DeltaTable
#pip install delta-spark==3.1.0 or pip install delta-spark==3.3.2
import uuid


# COMMAND ----------

def format_year_month(year_month):
    """Convert YYYY-MM input to YYYY-MM-01 format"""
    if year_month and len(year_month) == 7 and year_month[4] == '-':
        return f"{year_month}-01"
    return year_month


def calculate_date_range(months_to_process):
    """Calculate date range based on number of months to process"""
    if not months_to_process or not months_to_process.isdigit():
        return None, None

    months_to_process = int(months_to_process)
    if months_to_process <= 0:
        return None, None

    # Get current date and format as YYYY-MM-01
    current_date = spark.sql("SELECT current_date() as dt").collect()[0]['dt']
    end_ym_formatted = current_date.strftime("%Y-%m") + "-01"

    # Calculate start date by subtracting months_to_process from current date
    start_date = spark.sql(f"""
        SELECT add_months(to_date('{end_ym_formatted}'), -{months_to_process}) as dt
    """).collect()[0]['dt']
    start_ym_formatted = start_date.strftime("%Y-%m") + "-01"
    print(start_ym_formatted, end_ym_formatted)
    return start_ym_formatted, end_ym_formatted


# COMMAND ----------

# Read in the raw data and rename the purchasing unit colunms from suffix nm and nbr to desc and cd respectively for easy string concatenation later on
raw_data_df = spark.sql(
    f"""
    SELECT * FROM {database}.raw_pos_data_active_clients
    WHERE CNTRCT_FLG = 'Y'
""").repartition("year_month", "MANUF_NBR", "MANUF_NM")


# COMMAND ----------

def calculate_top_spend_my_clients_in_my_products_permutations(df, product_hierarchy, gpo_hierarchy, number_months,
                                                               start_dates, table_name, table_exists, database_name):
    # Iterate through the month options
    for months in number_months:

        # Iterate through the start dates
        for start_date in start_dates:
            try:

                # Set the end date for the current period so that there are the correct number of months between the start and end dates inclusive
                end_date = start_date + relativedelta(months=months - 1)

                # Skip the iteration if the end date is after the most recent month of data because there will not be enough months of data in this iteration
                # if end_date > most_recent_month_of_data:
                #    continue

                # Filter the raw data such that only the dates relevant to this iteration are kept
                filtered_df = raw_data_df.filter(
                    F.col("year_month").between(F.lit(start_date), F.lit(end_date))
                )

                # Iterate over product hierarchy columns
                for product_column in product_hierarchy:

                    # Get product hierarchy slice up to this level
                    product_slice = product_hierarchy[:product_hierarchy.index(product_column) + 1]

                    # Iterate over GPO hierarchy columns
                    for gpo_column in gpo_hierarchy:

                        # Get GPO hierarchy slice up to this level
                        gpo_slice = gpo_hierarchy[:gpo_hierarchy.index(gpo_column) + 1]

                        # Build groupby columns dynamically to include the cd and desc columns for all product and gpo levels of the current iteration and higher
                        groupby_cols = []
                        for col in gpo_slice:
                            groupby_cols.append(f"{col}_CD")
                            groupby_cols.append(f"{col}_DESC")
                        for col in product_slice:
                            groupby_cols.append(f"{col}_CD")
                            groupby_cols.append(f"{col}_DESC")

                        # Create a list of columns that will be used in select statements that cast the groupby columns to strings so there are aren't datatype mismatches upon future iterations
                        select_cols = [F.col(c).cast("string") for c in groupby_cols]

                        # Create a list of partition columns that will be used to rank the data within the the specific manufacturer and products at the current level
                        partition_cols = ["MANUF_NBR", f"{product_column}_CD"]

                        # Dictionary that will help the data to either ignore the segment in the groupby operations or force segment to be added to the sets of columns built so far
                        groupby_variants = [
                            {"name": "without_segment", "include_cols": False},
                            {"name": "with_segment", "include_cols": True}
                        ]

                        # Iterate through the 2 options in the segment dictionary
                        for groupby_variant in groupby_variants:

                            # boolean that is True on iterations that will include segment
                            include_segment = groupby_variant["include_cols"]

                            # Condition to add segment to the the curated column sets during relevant iterations
                            if include_segment:
                                groupby_cols += ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"]
                                partition_cols += ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"]
                                select_cols += ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"]

                            '''
                            Group by the curated groupby columns and manufacturer
                            Aggregate and sum total cases, total dollars, contract dollars and non-contract dollars
                            Rank the data using a window function that partitions the data using the partition cols and orders from most to least total dollars
                            Add columns called gpo_granularity, product_granularity, year_month and months, that will be used to identify the hierarchy levels of the current iteration as well as the start date and number of months used in the aggregation
                            Select the relevant columns making sure to use the select cols
                            '''
                            ranked_aggregates = (
                                filtered_df
                                .groupBy(
                                    "MANUF_NBR", "MANUF_NM", *groupby_cols
                                ).agg(
                                    F.sum("TOT_CASES").alias("TOTAL_CASES"),
                                    F.sum("TOT_DOL").alias("TOTAL_DOL_AMT"),
                                    F.sum(F.when(F.col("CNTRCT_FLG") == "Y", F.col("TOT_DOL")).otherwise(0)).alias(
                                        "CONTRACT_DOL_AMT"),
                                    F.sum(F.when(F.col("CNTRCT_FLG") == "N", F.col("TOT_DOL")).otherwise(0)).alias(
                                        "NON_CONTRACT_DOL_AMT")
                                ).withColumn(
                                    "Rank", F.row_number().over(
                                        Window.partitionBy(*partition_cols).orderBy(F.col("TOTAL_DOL_AMT").desc()))
                                ).withColumn(
                                    "months", F.lit(months)
                                ).withColumn(
                                    "year_month", F.lit(start_date)
                                ).withColumn(
                                    "gpo_granularity",
                                    F.lit(f"{gpo_column}")
                                ).withColumn(
                                    "product_granularity",
                                    F.lit(f"{product_column}")
                                ).select("MANUF_NBR", "MANUF_NM", *select_cols, "TOTAL_CASES", "CONTRACT_DOL_AMT",
                                         "NON_CONTRACT_DOL_AMT", "TOTAL_DOL_AMT", "Rank", "months", "year_month",
                                         "gpo_granularity", "product_granularity")
                            )

                            # Determine current index in the hierarchy
                            product_index = product_hierarchy.index(product_column)
                            gpo_index = gpo_hierarchy.index(gpo_column)

                            # Handle product hierarchy columns to ensure the schemas match by adding null columns for hierarchy levels that disapeared in the aggregations
                            for other_product_column in product_hierarchy:
                                other_index = product_hierarchy.index(other_product_column)

                                if other_index > product_index:
                                    # Lower level → null out CD and DESC
                                    ranked_aggregates = (
                                        ranked_aggregates
                                        .withColumn(f"{other_product_column}_CD", F.lit(None).cast("string"))
                                        .withColumn(f"{other_product_column}_DESC", F.lit(None).cast("string"))

                                    )

                            # Handle GPO hierarchy columns to ensure the schemas match by adding null columns for hierarchy levels that disapeared in the aggregations
                            for other_gpo_column in gpo_hierarchy:
                                other_index = gpo_hierarchy.index(other_gpo_column)

                                if other_index > gpo_index:
                                    # Lower level → null out CD and DESC
                                    ranked_aggregates = (
                                        ranked_aggregates
                                        .withColumn(f"{other_gpo_column}_CD", F.lit(None).cast("string"))
                                        .withColumn(f"{other_gpo_column}_DESC", F.lit(None).cast("string"))
                                    )

                            # If segment was not used to group the data in the current iteration, add a segment column to ensure the schemas match
                            if "PURCH_UNIT_INDSTR_SEGMNT_DESC" not in ranked_aggregates.columns:
                                ranked_aggregates = ranked_aggregates.withColumn("PURCH_UNIT_INDSTR_SEGMNT_DESC",
                                                                                 F.lit('all')).withColumn(
                                    "PURCH_GPO_SEGMNT_DESC", F.lit('all'))

                            # Save to Delta table
                            if not table_exists:
                                (ranked_aggregates
                                 .write
                                 .format("delta")
                                 .mode("overwrite")
                                 .option("overwriteSchema", "true")
                                 .saveAsTable(
                                    f'{database_name}.temp_{table_name}_{month_to_process}_months_{uuid.uuid4().hex[:8]}'))
                                table_exists = True
                            else:
                                (ranked_aggregates
                                 .write
                                 .format("delta")
                                 .option("mergeSchema", "True")
                                 .mode("append")
                                 .saveAsTable(
                                    f'{database_name}.temp_{table_name}_{month_to_process}_months_{uuid.uuid4().hex[:8]}'))

                            ranked_aggregates.unpersist()

                            print('Delta table saved for:')
                            print(gpo_column)
                            print(product_column)
                            print(start_date)
                            print(months)
                            print(groupby_variant["name"])

                    filtered_df.unpersist(blocking=True)
            except Exception as e:
                print(e)

    return (print('Done!'))


# COMMAND ----------

def process_triplets(triplet, filtered_df, start_date, months, table_name, table_exists, database_name):
    # Iterate throguh each of the 14 segment, gpo, product granularities
    # for triplet in filter_dicts:

    # create groupby and partition columns that can have segment, gpo, or product columns appended to them depending on the particular iteration
    groupby_cols = []
    partition_cols = ["MANUF_NBR"]

    # If the iteration indicates to group by segment, include it in the groupby and partition columns
    if triplet["segment"] is not None:
        groupby_cols = groupby_cols + triplet["segment"]
        partition_cols = partition_cols + triplet["segment"]

    # If the iteration indicates to group by GPO, include the current and higher levels in the groupby columns and add _CD and _DESC for each level
    if triplet["gpo"]:
        for col in triplet["gpo"]:
            groupby_cols.append(f"{col}_CD")
            groupby_cols.append(f"{col}_DESC")
    # If no GPO level was specified, consider the current level GPO Group
    else:
        groupby_cols.extend(["PURCH_GPO_ROLLUP_CD", "PURCH_GPO_CD", "PURCH_GPO_GRP_CD"])
        groupby_cols.extend(["PURCH_GPO_ROLLUP_DESC", "PURCH_GPO_DESC", "PURCH_GPO_GRP_DESC"])

    # If the iteration indicates to group by product, include the current and higher levels in the groupby columns and add _CD and _DESC for each level and add the current level to the partition columns
    if triplet["product"]:
        for col in triplet["product"]:
            groupby_cols.append(f"{col}_CD")
            groupby_cols.append(f"{col}_DESC")
        partition_cols.append(f"{triplet['product'][-1]}_CD")

    # Create a list of columns that will be used in select statements that cast the groupby columns to strings so there are aren't datatype mismatches upon future iterations
    select_cols = [F.col(c).cast("string") for c in groupby_cols]

    '''
    Group by the curated groupby columns and manufacturer
    Aggregate and sum total cases, total dollars, contract dollars and non-contract dollars
    Rank the data using a window function that partitions the data using the partition cols and orders from most to least total dollars
    Add columns called gpo_granularity, product_granularity, year_month and months, that will be used to identify the hierarchy levels of the current iteration as well as the start date and number of months used in the aggregation
    Select the relevant columns making sure to use the select cols
    '''
    ranked_aggregates = (
        filtered_df
        .groupBy(
            "MANUF_NBR", "MANUF_NM", *groupby_cols
        ).agg(
            F.sum("TOT_CASES").alias("TOTAL_CASES"),
            F.sum("TOT_DOL").alias("TOTAL_DOL_AMT"),
            F.sum(F.when(F.col("CNTRCT_FLG") == "Y", F.col("TOT_DOL")).otherwise(0)).alias("CONTRACT_DOL_AMT"),
            F.sum(F.when(F.col("CNTRCT_FLG") == "N", F.col("TOT_DOL")).otherwise(0)).alias("NON_CONTRACT_DOL_AMT")
        ).withColumn(
            "Rank",
            F.row_number().over(Window.partitionBy(*partition_cols).orderBy(F.col("TOTAL_DOL_AMT").desc()))
        ).withColumn(
            "months", F.lit(months)
        ).withColumn(
            "year_month", F.lit(start_date)
        ).select("MANUF_NBR", "MANUF_NM", *select_cols, "TOTAL_CASES", "CONTRACT_DOL_AMT", "NON_CONTRACT_DOL_AMT",
                 "TOTAL_DOL_AMT", "Rank", "months", "year_month")
    )

    # Determine the levels included in the current iteration and set the gpo hierarchy as the full hierarchy if none was specified
    current_product_hierarchy = triplet["product"]
    if triplet["gpo"]:
        current_gpo_hierarchy = triplet["gpo"]
    else:
        current_gpo_hierarchy = gpo_hierarchy

    # Handle product hierarchy columns to ensure the schemas match by adding null columns for hierarchy levels that disapeared in the aggregations
    for column in product_hierarchy:
        if column not in current_product_hierarchy:
            ranked_aggregates = ranked_aggregates \
                .withColumn(f"{column}_CD", F.lit(None).cast("string")) \
                .withColumn(f"{column}_DESC", F.lit(None).cast("string"))

    # Handle GPO hierarchy columns to ensure the schemas match by adding null columns for hierarchy levels that disapeared in the aggregations
    for column in gpo_hierarchy:
        if column not in current_gpo_hierarchy:
            ranked_aggregates = ranked_aggregates \
                .withColumn(f"{column}_CD", F.lit(None).cast("string")) \
                .withColumn(f"{column}_DESC", F.lit(None).cast("string"))

    # If segment was not used to group the data in the current iteration, add a segment column to ensure the schemas match
    if triplet["segment"] is None:
        ranked_aggregates = ranked_aggregates.withColumn("PURCH_UNIT_INDSTR_SEGMNT_DESC", F.lit('all')).withColumn(
            "PURCH_GPO_SEGMNT_DESC", F.lit('all'))

    # Add the gpo_granularity column that will be used to identify the hierarchy levels of the current iteration and if this iteration did not group by gpo at all, make the current iterations gpo_granularity value 'all'
    if not triplet["gpo"]:
        ranked_aggregates = ranked_aggregates.withColumn("gpo_granularity", F.lit('all'))
    else:
        ranked_aggregates = ranked_aggregates.withColumn("gpo_granularity", F.lit(triplet["gpo"][-1]))

    # Add the product_granularity column that will be used to identify the hierarchy levels of the current iteration and if this iteration did not group by product at all, make the current iterations gpo_granularity value 'all'
    if not triplet["product"]:
        ranked_aggregates = ranked_aggregates.withColumn("product_granularity", F.lit('all'))
    else:
        ranked_aggregates = ranked_aggregates.withColumn("product_granularity", F.lit(triplet["product"][-1]))

    # Save to Delta table
    if not table_exists:
        (ranked_aggregates
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(f'{database_name}.temp_{table_name}_{month_to_process}_months_{uuid.uuid4().hex[:8]}'))
        table_exists = True
    else:
        (ranked_aggregates
         .write
         .format("delta")
         .option("mergeSchema", "True")
         .mode("append")
         .saveAsTable(f'{database_name}.temp_{table_name}_{month_to_process}_months_{uuid.uuid4().hex[:8]}'))

    ranked_aggregates.unpersist()

    print('Delta table saved for:')
    print(triplet["gpo"])
    print(current_product_hierarchy)
    print(start_date)
    print(months)
    print(triplet["segment"])


# COMMAND ----------

from multiprocessing.pool import ThreadPool
from functools import partial


# COMMAND ----------

def calculate_top_spend_my_clients_in_my_products_all(raw_data_df, filter_dicts, number_months, start_dates, table_name,
                                                      table_exists, gpo_hierarchy, product_hierarchy, database_name):
    # Iterate through the month options
    for months in number_months:

        # Iterate through the start dates
        for start_date in start_dates:
            try:

                # Set the end date for the current period so that there are the correct number of months between the start and end dates inclusive
                end_date = start_date + relativedelta(months=months - 1)

                # Skip the iteration if the end date is after the most recent month of data because there will not be enough months of data in this iteration
                # if end_date > most_recent_month_of_data:
                #    continue

                # Filter the raw data such that only the dates relevant to this iteration are kept
                filtered_df = raw_data_df.filter(
                    F.col("year_month").between(F.lit(start_date), F.lit(end_date))
                )

                worker = partial(process_triplets, filtered_df=filtered_df, start_date=start_date, months=months,
                                 table_name=table_name, table_exists=table_exists, database_name=database_name)
                ThreadPool(10).map(worker, filter_dicts)

                filtered_df.unpersist(blocking=True)
            except Exception as e:
                print(e)

    return (print('Done!'))


# COMMAND ----------


number_months = [month_to_process]

from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from datetime import date
from dateutil.relativedelta import relativedelta


def daterange(start_date: date, end_date: date):
    try:
        step = max(1, int(month_to_process))
    except Exception:
        step = 1

    start = start_date.replace(day=1)
    end = end_date.replace(day=1)

    if not (start < end):
        return  # nothing to yield

    cur_end = end
    while cur_end > start:
        cur_start = max(start, cur_end - relativedelta(months=step))
        yield (cur_start, cur_end)
        cur_end = cur_start


# inputs
number_of_months_to_process = int(number_of_months_to_process)  # from widget
month_to_process = int(month_to_process)  # window length (usually 1)

anchor = date.today().replace(day=1)  # first day of current month
start_date = anchor - relativedelta(months=number_of_months_to_process)
end_date = anchor

for s, e in daterange(start_date, end_date):
    filtered_df = raw_data_df.filter(
        (F.col("year_month") >= F.lit(s)) & (F.col("year_month") < F.lit(e))
    )

current_date = date.today()
first_day_of_current_month = current_date.replace(day=1)
start_date = first_day_of_current_month - relativedelta(months=month_to_process)
start_dates = [start_date]
start_dates

# COMMAND ----------

# Define the hierarchy levels, the database name that we want to check/store the delta table in, the table name to search for/write to, and the triplets of granlarities for iterating across
product_hierarchy = ["PROD_FMLY", "PROD_SUB_FMLY_TYP", "PROD_SUB_SUB_FMLY_TYP"]
gpo_hierarchy = ["PURCH_GPO_ROLLUP", "PURCH_GPO", "PURCH_GPO_GRP"]

table_name = 'top_spend_my_clients_in_my_market'

filter_dicts = [
    {"segment": None, "gpo": [], "product": []},
    {"segment": None, "gpo": [], "product": ["PROD_FMLY"]},
    {"segment": None, "gpo": [], "product": ["PROD_FMLY", "PROD_SUB_FMLY_TYP"]},
    {"segment": None, "gpo": [], "product": ["PROD_FMLY", "PROD_SUB_FMLY_TYP", "PROD_SUB_SUB_FMLY_TYP"]},
    {"segment": None, "gpo": ["PURCH_GPO_ROLLUP"], "product": []},
    {"segment": None, "gpo": ["PURCH_GPO_ROLLUP", "PURCH_GPO"], "product": []},
    {"segment": None, "gpo": ["PURCH_GPO_ROLLUP", "PURCH_GPO", "PURCH_GPO_GRP"], "product": []},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"], "gpo": [], "product": []},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"], "gpo": [], "product": ["PROD_FMLY"]},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"], "gpo": [],
     "product": ["PROD_FMLY", "PROD_SUB_FMLY_TYP"]},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"], "gpo": [],
     "product": ["PROD_FMLY", "PROD_SUB_FMLY_TYP", "PROD_SUB_SUB_FMLY_TYP"]},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"], "gpo": ["PURCH_GPO_ROLLUP"], "product": []},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"], "gpo": ["PURCH_GPO_ROLLUP", "PURCH_GPO"],
     "product": []},
    {"segment": ["PURCH_UNIT_INDSTR_SEGMNT_DESC", "PURCH_GPO_SEGMNT_DESC"],
     "gpo": ["PURCH_GPO_ROLLUP", "PURCH_GPO", "PURCH_GPO_GRP"], "product": []},
]

# COMMAND ----------

# Get all tables in the database and check if the table exists
all_tables = spark.sql(f"SHOW TABLES IN {database}").select("tableName").rdd.flatMap(lambda x: x).collect()

table_exists = table_name in all_tables

# Call the function with all the relevant parameters
calculate_top_spend_my_clients_in_my_products_permutations(raw_data_df, product_hierarchy, gpo_hierarchy, number_months,
                                                           start_dates, table_name, table_exists, database)

# COMMAND ----------

# check the tables again in case the previous function run created the table for the first time
all_tables = spark.sql(f"SHOW TABLES IN {database}").select("tableName").rdd.flatMap(lambda x: x).collect()

table_exists = table_name in all_tables

# Call the function with all the relevant parameters
calculate_top_spend_my_clients_in_my_products_all(raw_data_df, filter_dicts, number_months, start_dates, table_name,
                                                  table_exists, gpo_hierarchy, product_hierarchy, database)