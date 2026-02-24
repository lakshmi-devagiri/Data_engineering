from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
import json
import os
from pyspark.sql.functions import col, md5, concat_ws, lit, sha2
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logger = get_logger()

# Load environment-specific configuration
bucketname, key = read_env_type()
config_map = configurationObject(bucketname, key)
tables = config_map['tables']
env_name = os.getenv("aws_env").lower()

if env_name == 'dr':
    env = 'prod'
else:
    env = env_name


def delete_elements_from_data_point(data_element_df, environment):
    try:
        # Define table prefix for current environment
        table_prefix = f"content_ext_{environment}.uspf_financials"

        # Create a temporary view of the data element DataFrame
        data_element_df.createOrReplaceTempView("data_element_view")

        # Select data_element_id values to delete
        data_element_id_query = f"""
            SELECT data_element_id 
            FROM {table_prefix}.t_data_element 
            WHERE data_element_code IN (
                SELECT data_element_code FROM data_element_view
            )
        """
        data_element_id_df = spark.sql(data_element_id_query)
        data_element_id_df.createOrReplaceTempView("data_element_id_view")

        # Deleting from the data point table using a SQL MERGE statement
        delete_from_data_point_query = f"""
            MERGE INTO {table_prefix}.t_data_point AS t 
            USING data_element_id_view AS s 
            ON t.data_element_id = s.data_element_id 
            WHEN MATCHED THEN DELETE
        """
        spark.sql(delete_from_data_point_query)
        logger.info("Deleted records from t_data_point table related to the specified data elements.")

        # Deleting from the data element table using a SQL MERGE statement
        delete_from_data_element_query = f"""
            MERGE INTO {table_prefix}.t_data_element AS t 
            USING data_element_id_view AS s 
            ON t.data_element_id = s.data_element_id 
            WHEN MATCHED THEN DELETE
        """
        spark.sql(delete_from_data_element_query)
        logger.info("Deleted records from t_data_element table for the specified data elements.")

    except Exception as e:
        logger.error(f"An error occurred while deleting elements: {str(e)}")


def refresh_table(tables, env):
    """Refresh the specified tables based on given configurations and environment."""

    for table in tables:
        # Fetch table name and mode from widgets
        name = dbutils.widgets.get("name")
        mode = dbutils.widgets.get("mode")

        # Check if table matches the current name and mode
        if table['name'] == name and table['mode'] == mode:
            # Logging table details
            logger.info(f"Processing of USPF Table '{name}' has commenced.")

            if table['name'] == "USPF-period":
                process_period_data(table, env)
            else:
                try:
                    # Load source and destination DataFrames
                    if not table['auto_inc_key']:
                        destination_df = spark.table(table["dest_table"])
                        source_df = spark.read.table(table["source_table"])
                    else:
                        # Drop auto-increment keys for incremental load
                        auto_inc_key = table['auto_inc_key']
                        destination_df = spark.table(table["dest_table"]).drop(*auto_inc_key).persist()
                        source_df = spark.read.table(table["source_table"]).drop(*auto_inc_key).persist()

                    # Create hashkeys for all columns except insertion keys
                    all_other_columns = [col(c) for c in source_df.columns if c not in table["ins_keys"]]
                    source_df_hash = source_df.withColumn("hashkey", sha2(concat_ws(",", *all_other_columns),
                                                                          256)).dropDuplicates()
                    destination_df_hash = destination_df.withColumn("hashkey", sha2(concat_ws(",", *all_other_columns),
                                                                                    256)).dropDuplicates()

                    # Determine changed records with full outer join
                    changed_records = source_df_hash.alias("s") \
                        .join(destination_df_hash.alias("d"), col("s.hashkey") == col("d.hashkey"),
                              "full_outer").persist()

                    # Identify inserts and deletes
                    source_df_hash = changed_records.filter("d.hashkey IS NULL").select("s.*")
                    destination_df_hash = changed_records.filter("s.hashkey IS NULL").select("d.*")

                    # Cleanup persisted data
                    changed_records.unpersist()
                    source_df.unpersist()
                    destination_df.unpersist()

                    # Determine updates and soft deletes
                    upd_ins_df = source_df_hash.alias("src") \
                        .join(destination_df_hash.alias("trg"), (col("src.hashkey") == col("trg.hashkey")), "leftanti")
                    upd_del_df = destination_df_hash.alias("src") \
                        .join(source_df_hash.alias("trg"), (col("src.hashkey") == col("trg.hashkey")), "leftanti")

                    # Insert new records
                    ins_df = upd_ins_df.alias("src").join(upd_del_df.alias("trg"), table["primary_keys"], "leftanti")
                    upd_df = upd_ins_df.alias("src") \
                        .join(ins_df.alias("trg"), table["primary_keys"] and (col("src.hashkey") == col("trg.hashkey")),
                              "anti") \
                        .drop("update_dttm", "update_user")
                    soft_del_df = upd_del_df.alias("src").join(upd_df.alias("trg"), table["primary_keys"],
                                                               "anti").select("src.*")

                    # Cleanup
                    destination_df.unpersist()
                    source_df.unpersist()

                    # Update last modified timestamp for data element tables
                    if table["dest_table"].split('.')[-1].lower() == 't_data_element':
                        lastModifiedTable = table["last_upd_time_tbl"]
                        update_last_timestamp(lastModifiedTable, "dataElementLastModified")

                    # Prepare merge conditions
                    trg_tbl = table["dest_table"]
                    merge_cond = ' AND '.join([f"t.{f} = s.{f}" for f in table["primary_keys"]])

                    # Check if there are records to merge
                    if ins_df.isEmpty() and upd_df.isEmpty() and soft_del_df.isEmpty():
                        logger.info("No new records to merge; incremental process has completed successfully!")
                        return

                    # Log counts for inserts, updates, and deletes
                    # Uncomment these lines if needed for debugging
                    # ins_cnt = ins_df.count()
                    # upd_cnt = upd_df.count()
                    # soft_del_cnt = soft_del_df.count()
                    # logger.info(f"Change summary for table {table['dest_table']}:\nTotal Updates: {upd_cnt}\nTotal Deletes: {soft_del_cnt}\nTotal Inserts: {ins_cnt}")

                    # Merge inserts
                    if not ins_df.isEmpty():
                        logger.info(f"Proceeding to merge the inserts into {trg_tbl}.")
                        ins_df = ins_df.drop(*table['ins_keys']).drop("hashkey")
                        insert_schema = ', '.join([f"t.{field.name}" for field in ins_df.schema.fields])
                        values_schema = ', '.join([f"s.{field.name}" for field in ins_df.schema.fields])
                        values_schema += ", CURRENT_TIMESTAMP, CURRENT_USER(), NULL, NULL"

                        ins_df.createOrReplaceTempView("ins_view")
                        ins_query = f"""
                            MERGE INTO {trg_tbl} AS t 
                            USING ins_view AS s 
                            ON {merge_cond} 
                            WHEN NOT MATCHED THEN 
                            INSERT ({insert_schema}) 
                            VALUES ({values_schema})
                        """
                        spark.sql(ins_query)
                        logger.info(f"All new inserts have been successfully merged into {trg_tbl}.")

                    # Merge updates
                    if not upd_df.isEmpty():
                        logger.info(f"Proceeding to merge the updates into {trg_tbl}.")
                        upd_df = upd_df.drop(*table['upd_keys'])
                        upd_df.createOrReplaceTempView("upd_view")
                        update_fields = ', '.join(
                            [f"t.{field.name} = s.{field.name}" for field in destination_df.schema.fields if
                             field.name in [f.name for f in upd_df.schema.fields]])
                        update_fields += f", t.update_dttm = CURRENT_TIMESTAMP, t.update_user = CURRENT_USER()"

                        upd_query = f"""
                            MERGE INTO {trg_tbl} AS t 
                            USING upd_view AS s 
                            ON {merge_cond} 
                            WHEN MATCHED THEN 
                            UPDATE SET {update_fields}
                        """
                        spark.sql(upd_query)
                        logger.info(f"All the updates have been successfully merged into {trg_tbl}.")

                    # Handle soft deletes
                    if not soft_del_df.isEmpty():
                        logger.info(
                            f"Some records got deleted in {trg_tbl}, proceeding to delete from current table and its dependent tables.")
                        soft_del_df.createOrReplaceTempView("soft_del_view")

                        if table["dest_table"].split('.')[-1].lower() == 't_data_element':
                            de_df = soft_del_df.select("data_element_code")
                            delete_elements_from_data_point(de_df, env)
                        else:
                            soft_del_query = f"""
                                MERGE INTO {trg_tbl} AS t 
                                USING soft_del_view AS s 
                                ON {merge_cond} 
                                WHEN MATCHED THEN DELETE
                            """
                            spark.sql(soft_del_query)
                            logger.info(f"Records have been deleted from {trg_tbl}.")

                except Exception as e:
                    logger.error(f"Refresh failed for {table['dest_table']}: {str(e)}")
                    raise Exception(f"Refresh failed for {table['dest_table']}: {str(e)}")


# Execute refresh process
refresh_table(tables, env)
