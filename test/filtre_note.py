from pyspark.sql import SparkSession
import pyspark

print(pyspark.__version__)
spark = SparkSession.builder.appName("Steam_reviews").getOrCreate()
print(spark.version)
df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()


reviews_score_1 = df.filter(df.review_score == 1).select("review_text").limit(5)
print("review_score = 1:")
reviews_score_1.show(truncate=False)


reviews_score_0 = df.filter(df.review_score == 0).select("review_text").limit(5)
print("review_score = 0:")
reviews_score_0.show(truncate=False)

