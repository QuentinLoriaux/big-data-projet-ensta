from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder.appName("Steam_reviews").getOrCreate()

df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
df.printSchema()


reviews_score_good = df.filter(df.review_score == 1).select("review_text").limit(5)
print("review_score = 1:")
reviews_score_good.show(truncate=False)


reviews_score_bad = df.filter(df.review_score == -1).select("review_text").limit(5)
print("review_score = -1:")
reviews_score_bad.show(truncate=False)

start_time = time.time()
filtered_df = df.filter(col("review_score") < 1)
end_time = time.time()
filtered_df.show()
print(f"duree du traitement : {end_time - start_time:.4f} seconds")
