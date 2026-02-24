import os
import re
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import *

# ---------- Kafka / Confluent Cloud ----------
host = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
topic = "venu_topic"
api_key = "XHOL7RHJAF75UL5G"
api_secret = "cfltz+jL2ONC2a3XEMat+3dGP+Fe1JlK7lF7j0BTGtY1cRdEt02IJC2/IBscTnaQ"

# ---------- Local checkpoint (change for Windows if you like) ----------
checkpoint_path = f"D:/bigdata/temp/kafka_checkpoints/{topic}/console_run"

# ---------- SparkSession with Kafka packages ----------
# IMPORTANT: match the Spark version you installed.
# If you're on Spark 3.5.x, keep 3.5.1; for 3.4.x use 3.4.3, etc.
spark = (
    SparkSession.builder
        .appName("kafka-stream-local")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"  # <-- adjust to your Spark version
        )
        .getOrCreate()
)


# ---------- Read from Kafka (LOCAL JAAS CLASS!) ----------
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")       # pick 'latest' if you only want new data
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="{api_key}" password="{api_secret}";'
        )
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("kafka.client.dns.lookup", "use_all_dns_ips")
        .load()
)

raw = df.selectExpr("CAST(value AS STRING) AS value")

ndf=raw.selectExpr("CAST(value AS STRING)")
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

res=ndf.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))

ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

#''' ---------- Continuous processing every 5 seconds ----------
'''
(
    res.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")   # <--- works locally; not supported on Databricks serverless
        .start()
        .awaitTermination()
)
'''