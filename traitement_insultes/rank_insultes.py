from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, count
import time
spark = SparkSession.builder.appName("Swear").getOrCreate()


## Version non optimisée pour comparer

## temps ~ 25 sec

start_time = time.time()

df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
with open("../dataset/swearWords.txt", "r") as f:
    swear = [line.strip() for line in f.readlines()]
swear_broadcast = spark.sparkContext.broadcast(swear)

res_df = df.withColumn("word", explode(split(lower(col("review_text")), " ")))
res = res_df.filter(col("word").isin(swear_broadcast.value))
count = res.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc())


count.show()

end_time = time.time()
print(f"Operation took {end_time - start_time:.4f} seconds")