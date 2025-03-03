from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, count
spark = SparkSession.builder.appName("Swear").getOrCreate()

comm = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
filtered_comm = comm.filter(col("review_score") == -1)
with open("../dataset/swearWords.txt", "r") as f:
    swear = [line.strip() for line in f.readlines()]

swear_broadcast = spark.sparkContext.broadcast(swear)

res_comm = filtered_comm.withColumn("word", explode(split(lower(col("review_text")), " ")))

res = res_comm.filter(col("word").isin(swear_broadcast.value))

count = res.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc())

count.show()
