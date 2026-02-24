import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, when

# ====== EDIT THESE ======
BOOTSTRAP = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
TOPIC = "oct10"
API_KEY = "OLVGW7D2XFVAT3YC".strip()
API_SECRET = "cfltLiPQRc4YpIQhCX63SB3yBWLdfpqqt15ZpiEwReHtm+jqwrkv4nYUtuVR8y1w".strip()

# Show in console or write to MS SQL
MODE = "mssql"          # "show" or "mssql"

# MS SQL (used when MODE="mssql")
JDBC_URL = "jdbc:sqlserver://yashoda.ci9mki4eidd9.us-east-1.rds.amazonaws.com:1433;databaseName=venudb;trustServerCertificate=true;"
SQL_USER = "admin"
SQL_PASS = "Mypassword.1"
TARGET_TABLE = "dbo.AccessLogs"
# ========================

SPARK_VERSION = "3.5.4"  # match your local Spark
spark = (
    SparkSession.builder
    .appName("KafkaParseShowOrWrite")
    .config(
        "spark.jars.packages",
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION},"
        f"com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Build all Kafka options before .load()
opts = {
    "kafka.bootstrap.servers": BOOTSTRAP,
    "subscribe": TOPIC,
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "kafka.client.dns.lookup": "use_all_dns_ips",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    # UNSHADED for PyCharm/local:
    "kafka.sasl.jaas.config":
        f'org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{API_KEY}" password="{API_SECRET}";',
    "kafka.ssl.endpoint.identification.algorithm": "https",
}

raw = spark.readStream.format("kafka").options(**opts).load()

# Cast payload to string
lines = raw.selectExpr("CAST(value AS STRING) AS raw", "timestamp AS ingest_ts")

# Apache Combined Log regex (no UDFs)
# 1=ip, 2=ts_raw, 3=method, 4=path, 5=protocol, 6=status, 7=bytes, 8=referrer, 9=user_agent
pattern = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) ([^ ]+) ([^"]+)" (\d{3}) (\S+) "([^"]*)" "([^"]*)"'

parsed = (
    lines
    .select(
        regexp_extract(col("raw"), pattern, 1).alias("ip"),
        regexp_extract(col("raw"), pattern, 2).alias("ts_raw"),
        regexp_extract(col("raw"), pattern, 3).alias("method"),
        regexp_extract(col("raw"), pattern, 4).alias("path"),
        regexp_extract(col("raw"), pattern, 5).alias("protocol"),
        regexp_extract(col("raw"), pattern, 6).cast("int").alias("status"),
        regexp_extract(col("raw"), pattern, 7).alias("bytes_str"),
        regexp_extract(col("raw"), pattern, 8).alias("referrer"),
        regexp_extract(col("raw"), pattern, 9).alias("user_agent"),
        col("ingest_ts"),
    )
    .withColumn("bytes", when(col("bytes_str") == "-", None).otherwise(col("bytes_str").cast("long")))
    .withColumn("event_ts", to_timestamp(col("ts_raw"), "dd/MMM/yyyy:HH:mm:ss Z"))
    .select("event_ts","ingest_ts","ip","method","path","protocol","status","bytes","referrer","user_agent")
)

if MODE.lower() == "show":
    # -------- Option A: SHOW structured output in console --------
    (parsed.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
        .awaitTermination()
    )

elif MODE.lower() == "mssql":
    # -------- Option B: WRITE to MS SQL via foreachBatch --------
    def write_to_sql(batch_df, batch_id):
        (batch_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", TARGET_TABLE)
            .option("user", SQL_USER)
            .option("password", SQL_PASS)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("batchsize", "5000")
            .mode("append")
            .save()
        )

    (parsed.writeStream
        .outputMode("append")
        .foreachBatch(write_to_sql)
        .trigger(processingTime="10 seconds")  # continuous; use availableNow=True to drain once
        .start().awaitTermination()
    )

else:
    raise ValueError('MODE must be "show" or "mssql"')
