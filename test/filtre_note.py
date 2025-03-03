from pyspark.sql import SparkSession
import pyspark

print(pyspark.__version__)
spark = SparkSession.builder.appName("Steam_reviews").getOrCreate()
print(spark.version)
df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()


reviews_score_good = df.filter(df.review_score == 1).select("review_text").limit(5)
print("review_score = 1:")
reviews_score_good.show(truncate=False)


reviews_score_bad = df.filter(df.review_score == -1).select("review_text").limit(5)
print("review_score = -1:")
reviews_score_bad.show(truncate=False)

