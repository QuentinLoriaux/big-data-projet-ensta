import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, sum as spark_sum

spark = SparkSession.builder.appName("ReviewFilter").getOrCreate()

csv_file_path = "../dataset/steam_reviews.csv"

df = spark.read.option("header", True).csv(csv_file_path, inferSchema=True)

start_time = time.time()

df = df.withColumn("word_count", size(split(col("review_text"), " ")))
word_count_df = df.groupBy("app_name").agg(
    spark_sum((col("review_score") == 1).cast("int") * col("word_count")).alias("positive_word_count"),
    spark_sum((col("review_score") == -1).cast("int") * col("word_count")).alias("negative_word_count")
)
word_count_df = word_count_df.withColumn("final_count", col("positive_word_count") - col("negative_word_count"))
sorted_games = word_count_df.orderBy(col("final_count").desc())
sorted_games.show()

end_time = time.time()

print(f"Operation took {end_time - start_time:.4f} seconds")

