from pyspark.sql import SparkSession
import requests
import time

# -------------------------------
# 1. Create Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("Pokemon_API_Ingestion") \
    .master("local[*]") \
    .getOrCreate()

# -------------------------------
# 2. API Base URL
# -------------------------------
BASE_URL = "https://pokeapi.co/api/v2/pokemon"

# -------------------------------
# 3. API Fetch Function
# -------------------------------
def fetch_pokemon(name, retries=3):
    url = f"{BASE_URL}/{name}"

    for _ in range(retries):
        try:
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                data = response.json()
                return (
                    int(data["id"]),
                    data["name"],
                    int(data["height"]),
                    int(data["weight"])
                )
        except Exception:
            time.sleep(1)

    return None

# -------------------------------
# 4. Pokémon List
# -------------------------------
pokemon_list = [
    "pikachu",
    "bulbasaur",
    "charmander",
    "squirtle",
    "jigglypuff"
]

# -------------------------------
# 5. Parallelize & Call API
# -------------------------------
rdd = spark.sparkContext.parallelize(pokemon_list)

result_rdd = rdd.map(fetch_pokemon).filter(lambda x: x is not None)

# -------------------------------
# 6. Create DataFrame
# -------------------------------
columns = ["id", "name", "height", "weight"]
pokemon_df = spark.createDataFrame(result_rdd, columns)

# -------------------------------
# 7. Show Result
# -------------------------------
pokemon_df.show(truncate=False)

# -------------------------------
# 8. Save Output (Local Machine)
# -------------------------------
pokemon_df.write.mode("overwrite").parquet("pokemon_api_data")

# -------------------------------
# 9. Stop Spark
# -------------------------------
spark.stop()
