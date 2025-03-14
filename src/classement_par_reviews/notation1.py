import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, sum as spark_sum

spark = SparkSession.builder.appName("ReviewFilter").getOrCreate()
df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)

start_time = time.time()
df = df.withColumn("word_count", size(split(col("review_text"), " ")))
word_count_df = df.groupBy("app_name").agg(
    spark_sum((col("review_score") == 1).cast("int") * col("word_count") * (col("review_votes") + 1) ).alias("positive_word_count"),
    spark_sum((col("review_score") == -1).cast("int") * col("word_count") * (col("review_votes") + 1) ).alias("negative_word_count")
)
word_count_df = word_count_df.withColumn("final_count", col("positive_word_count") - col("negative_word_count"))
sorted_games = word_count_df.orderBy(col("final_count").desc())
end_time = time.time()

# Affichage resultats
sorted_games.show()
print(f"Le traitement a dure : {end_time - start_time:.4f} seconds")

