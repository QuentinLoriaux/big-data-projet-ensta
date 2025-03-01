from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("Steam reviews") \
    .getOrCreate()

print(spark.version)

csv_file_path = "../dataset/steam_reviews.csv"

df = spark.read.option("header", True).csv(csv_file_path, inferSchema=True)

start_time = time.time()
filtered_df = df.filter(col("review_score") < 1)
end_time = time.time()

filtered_df.show()

print(f"Operation took {end_time - start_time:.4f} seconds")
# df.printSchema()

