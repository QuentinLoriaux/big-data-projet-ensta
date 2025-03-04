from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, count
spark = SparkSession.builder.appName("Swear").getOrCreate()
import time

## Version non optimisée pour comparer
start_time = time.time()

comm = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
with open("../dataset/swearWords.txt", "r") as f:
    swear = [line.strip() for line in f.readlines()]
swear_broadcast = spark.sparkContext.broadcast(swear)

res_comm = comm.withColumn("word_good", explode(split(lower(col("review_text")), " ")))
res = res_comm.filter(col("word_good").isin(swear_broadcast.value))
count = res.groupBy("word_good").agg(count("word_good").alias("count")).orderBy(col("count").desc())


count.show()

end_time = time.time()
print(f"Operation took {end_time - start_time:.4f} seconds")