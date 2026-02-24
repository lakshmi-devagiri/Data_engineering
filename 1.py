import pandas as pd

df = pd.read_csv(
    "spark_posts_csv/part-00000-1a529ce3-d4ce-45f0-9fa5-6a4cdac302b9-c000.csv"
)

print(df.head())