from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark
print(pyspark.__version__)

spark = SparkSession.builder \
    .appName("Steam reviews") \
    .getOrCreate()

print(spark.version)

csv_file_path = "../dataset/steam_reviews.csv"

df = spark.read.option("header", True).csv(csv_file_path, inferSchema=True)
filtered_df = df.filter(col("review_score") < 1)

filtered_df.show()
df.printSchema()
