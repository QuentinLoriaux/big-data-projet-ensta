from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
spark = SparkSession.builder.appName("SwearWordCount").getOrCreate()

# Version optimisée
start_time = time.time()

df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
with open("../dataset/swearWords.txt", "r") as f:
    swear_list = [line.strip().lower() for line in f.readlines()]
swear_broadcast = spark.sparkContext.broadcast(set(swear_list))

rdd = df.select("review_text").rdd.flatMap(lambda x: x).filter(lambda x: x is not None)

rdd_tokens = rdd.flatMap(lambda text: text.lower().split())
rdd_swears = rdd_tokens.filter(lambda word: word in swear_broadcast.value)

rdd_word_counts = rdd_swears.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

swear_df = spark.createDataFrame(rdd_word_counts, ["word", "count"]).orderBy("count", ascending=False)

swear_df.show()

end_time = time.time()
print(f"Operation took {end_time - start_time:.4f} seconds")
