from pyspark.sql import SparkSession
import pyspark
print(pyspark.__version__)

spark = SparkSession.builder \
    .appName("Steam reviews") \
    .getOrCreate()

print(spark.version)

df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()

