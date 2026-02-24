from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").config("spark.jars", r"D:\bigdata\spark-3.5.4-bin-hadoop3\jars\spark-sql-kafka-0-10_2.12-3.5.4.jar,D:\bigdata\spark-3.5.4-bin-hadoop3\jars\spark-token-provider-kafka-0-10_2.12-3.5.4.jar,D:\bigdata\spark-3.5.4-bin-hadoop3\jars\kafka-clients-3.7.2.jar").getOrCreate()
BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY = "EFJGEWCZUBUABSOQ"
API_SECRET = r"cfltCeV+PKKl430ChVpDcScmHq93RnWf76BCczK0WLcGrY5m6yVu+Vy/iNmcM1lg"
SCHEMA_REGISTRY_URL = "https://psrc-13gydo7.ap-south-1.aws.confluent.cloud"
SCHEMA_REGISTRY_API_KEY = "WCUAFY6I76B3UINZ"
SCHEMA_REGISTRY_API_SECRET = "cfltRmb3Axpc9qHVJLdsnZbM11V14oUTJRtI1lf6BnjEyLkeP4F9EgvNAxaoJXgg"
topic = "kafkatopic"

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{API_KEY}" password="{API_SECRET}";')
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.client.dns.lookup", "use_all_dns_ips")
    .load()
)


bank_schema = StructType([
    StructField("age", IntegerType()),
    StructField("job", StringType()),
    StructField("marital", StringType()),
    StructField("education", StringType()),
    StructField("default", StringType()),
    StructField("balance", IntegerType()),
    StructField("housing", StringType()),
    StructField("loan", StringType()),
    StructField("contact", StringType()),
    StructField("day", IntegerType()),
    StructField("month", StringType()),
    StructField("duration", IntegerType()),
    StructField("campaign", IntegerType()),
    StructField("pdays", IntegerType()),
    StructField("previous", IntegerType()),
    StructField("poutcome", StringType()),
    StructField("y", StringType()),
])

#in producer api if u use this value = value_serializer(record, SerializationContext(topic, MessageField.VALUE)) use below
#json_df = (df.select(expr("decode(substring(value, 6), 'UTF-8')").alias("json")))
json_df = df.select(col("value").cast("string").alias("json"))
# 2) Parse JSON into columns
records = json_df.select(from_json(col("json"), bank_schema).alias("r")).select("r.*")


records.writeStream.format("console").option("truncate","false").outputMode("append") .option("truncate", "false").start().awaitTermination()