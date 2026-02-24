from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mar23") \
  .load()
df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")
data=[
{
    "results": [
        {
            "user": {
                "gender": "male",
                "name": {
                    "title": "mr",
                    "first": "jonas",
                    "last": "moulin"
                },
                "location": {
                    "street": "8552 rue abel-ferry",
                    "city": "paris",
                    "state": "pyrénées-orientales",
                    "zip": 17741
                },
                "email": "jonas.moulin@example.com",
                "username": "lazylion902",
                "password": "grand",
                "salt": "JHmF7EXw",
                "md5": "f71dce3467480d32f278028316b1be7a",
                "sha1": "9985deabc5f96651c052e160af07157da2da4c71",
                "sha256": "0b54eaabc21bdf123a63dc7e48a127ab0b36d8272f50114af4b7f89bc657fb81",
                "registered": 1071425565,
                "dob": 113218636,
                "phone": "03-60-65-17-07",
                "cell": "06-35-98-28-27",
                "INSEE": "1730825324306 32",
                "picture": {
                    "large": "https://randomuser.me/api/portraits/men/58.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/58.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/58.jpg"
                }
            }
        }
    ],
    "nationality": "FR",
    "seed": "090792d801777ff407",
    "version": "0.8"
}
]

schema1=spark.read.json(spark.sparkContext.parallelize(data)).schema


import re
ndf1=ndf.select(from_json(col("value"), schema1).alias("data")).select("data.*")
ndf.writeStream.outputMode("append").format("console").start().awaitTermination()
