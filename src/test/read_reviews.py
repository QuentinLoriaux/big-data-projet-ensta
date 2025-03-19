from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Steam reviews").getOrCreate()

df = spark.read.csv("../../dataset/steam_reviews.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

