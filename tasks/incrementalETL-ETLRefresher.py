import os
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

logger = get_logger()

# --- config & env -------------------------------------------------------------
bucketname, key = read_env_type()
config_map = configurationObject(bucketname, key)
tables = config_map['tables']

env_name = (os.getenv("aws_env") or "dev").lower()
env = "prod" if env_name == "dr" else env_name

# Where we store a small watermark of the last processed version per (name, mode)
WATERMARK_TBL = f"content_ext_{env}.uspf_financials._cdc_watermarks"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TBL} (
  tbl_name STRING,
  tbl_mode STRING,
  last_version BIGINT
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite=true)
""")

def _get_widget(k, default=""):
    try:
        return dbutils.widgets.get(k)
    except Exception:
        return default

# --- helpers -----------------------------------------------------------------
def get_cfg_for_run(tables):
    name = _get_widget("name")
    mode = _get_widget("mode")
    for t in tables:
        if t["name"] == name and t["mode"] == mode:
            return t, name, mode
    raise ValueError("No matching table config for provided widgets 'name' and 'mode'.")

def get_last_version(name, mode):
    rows = spark.table(WATERMARK_TBL).where((F.col("tbl_name")==name) & (F.col("tbl_mode")==mode)).limit(1).collect()
    return rows[0]["last_version"] if rows else None

def upsert_last_version(name, mode, ver):
    # Simple upsert for the watermark
    spark.sql(f"""
      MERGE INTO {WATERMARK_TBL} AS t
      USING (SELECT '{name}' AS tbl_name, '{mode}' AS tbl_mode, {int(ver)} AS last_version) AS s
      ON t.tbl_name = s.tbl_name AND t.tbl_mode = s.tbl_mode
      WHEN MATCHED THEN UPDATE SET t.last_version = s.last_version
      WHEN NOT MATCHED THEN INSERT (tbl_name, tbl_mode, last_version) VALUES (s.tbl_name, s.tbl_mode, s.last_version)
    """)

def delete_elements_from_data_point(data_element_df, environment):
    table_prefix = f"content_ext_{environment}.uspf_financials"
    data_element_df.createOrReplaceTempView("de_to_delete")

    spark.sql(f"""
      MERGE INTO {table_prefix}.t_data_point AS t
      USING (
        SELECT de.data_element_id
        FROM {table_prefix}.t_data_element de
        JOIN de_to_delete d ON d.data_element_id = de.data_element_id
      ) s
      ON t.data_element_id = s.data_element_id
      WHEN MATCHED THEN DELETE
    """)

    spark.sql(f"""
      MERGE INTO {table_prefix}.t_data_element AS t
      USING de_to_delete s
      ON t.data_element_id = s.data_element_id
      WHEN MATCHED THEN DELETE
    """)
    logger.info("Cascade delete completed for t_data_element → t_data_point.")

# --- main refresh using CDF ---------------------------------------------------
def refresh_table_with_cdf(tables, env):
    table, name, mode = get_cfg_for_run(tables)
    logger.info(f"Processing table '{name}' in mode '{mode}' (env={env})")

    src_tbl = table["source_table"]
    dst_tbl = table["dest_table"]
    pks     = table["primary_keys"]      # e.g., ["data_element_id"]
    # Columns you never overwrite on update (optional)
    upd_exclusions = set(table.get("upd_keys", []))

    # Ensure CDF is enabled (no-op if already set)
    spark.sql(f"ALTER TABLE {src_tbl} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    # Determine the starting version
    from_ver = get_last_version(name, mode)
    if from_ver is None:
        # First run: start from table creation (version 0), or choose a specific version
        from_ver = 0

    # Pull only changes we need (post-images + deletes)
    cdf_df = spark.sql(f"""
      SELECT * FROM table_changes('{src_tbl}', {from_ver})
      WHERE _change_type IN ('insert','update_postimage','delete')
    """)

    if cdf_df.rdd.isEmpty():
        logger.info("No changes since last watermark. Nothing to do.")
        return

    # Keep the latest commit version we saw
    max_ver = cdf_df.agg(F.max("_commit_version").alias("v")).collect()[0]["v"]

    # Build dynamic column lists for MERGE
    dst_schema = spark.table(dst_tbl).schema
    dst_cols   = [f.name for f in dst_schema if f.name not in {"_change_type","_commit_version","_commit_timestamp"}]
    # We must ensure all PKs exist in cdf_df/destination
    merge_cond = " AND ".join([f"t.{k} = s.{k}" for k in pks])

    # UPDATE set list (exclude PKs and excluded columns)
    updatable_cols = [c for c in dst_cols if c not in set(pks) | upd_exclusions]
    update_set_sql = ", ".join([f"t.{c} = s.{c}" for c in updatable_cols] + [
        "t.update_dttm = current_timestamp()", "t.update_user = current_user()"
    ])

    # INSERT column list (all destination columns that exist in source CDF)
    insert_cols = dst_cols
    insert_cols_sql = ", ".join([f"t.{c}" for c in insert_cols])
    values_cols_sql = ", ".join([f"s.{c}" for c in insert_cols])

    # Register CDF as a view for SQL MERGE
    cdf_df.createOrReplaceTempView("cdf_view")

    spark.sql(f"""
      MERGE INTO {dst_tbl} AS t
      USING cdf_view AS s
      ON {merge_cond}
      WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
      WHEN MATCHED AND s._change_type IN ('update_postimage') THEN
        UPDATE SET {update_set_sql}
      WHEN NOT MATCHED AND s._change_type IN ('insert','update_postimage') THEN
        INSERT ({insert_cols_sql}) VALUES ({values_cols_sql})
    """)

    logger.info(f"Upserted/deleted changes from {src_tbl} into {dst_tbl} via CDF up to version {max_ver}.")

    # Special cascade for data elements: delete rows that came as deletes
    if dst_tbl.split(".")[-1].lower() == "t_data_element":
        deletes_df = cdf_df.filter(F.col("_change_type") == "delete").select("data_element_id").dropDuplicates()
        if not deletes_df.rdd.isEmpty():
            delete_elements_from_data_point(deletes_df, env)
            # optional: update a "last modified" table if you keep one
            if "last_upd_time_tbl" in table:
                update_last_timestamp(table["last_upd_time_tbl"], "dataElementLastModified")

    # Advance watermark
    upsert_last_version(name, mode, max_ver)
    logger.info("Incremental process completed successfully.")

# --- run ---------------------------------------------------------------------
refresh_table_with_cdf(tables, env)
