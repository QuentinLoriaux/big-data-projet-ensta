from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, count
spark = SparkSession.builder.appName("Swear").getOrCreate()

## A OPTIMISER

comm = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)
with open("../dataset/swearWords.txt", "r") as f:
    swear = [line.strip() for line in f.readlines()]
swear_broadcast = spark.sparkContext.broadcast(swear)

# insulte positive :
filtered_comm = comm.filter(col("review_score") == 1)
res_comm = filtered_comm.withColumn("word_good", explode(split(lower(col("review_text")), " ")))
res = res_comm.filter(col("word_good").isin(swear_broadcast.value))
count_positive = res.groupBy("word_good").agg(count("word_good").alias("count"))

# insulte negative :
filtered_comm = comm.filter(col("review_score") == -1)
res_comm2 = filtered_comm.withColumn("word_bad", explode(split(lower(col("review_text")), " ")))
res2 = res_comm2.filter(col("word_bad").isin(swear_broadcast.value))
count_negative = res2.groupBy("word_bad").agg(count("word_bad").alias("count2"))


sum = [count_positive.agg({"count": "sum"}).collect()[0][0], count_negative.agg({"count2": "sum"}).collect()[0][0]]

res1 = 100*count_positive["count"]/sum[0]
res2 = 100*count_negative["count2"]/sum[1]

count_total = count_positive.join(count_negative, count_positive.word_good == count_negative.word_bad, how="outer").select(
    count_positive.word_good.alias("word"),
    (res1).alias("count_positive"),
    (res2).alias("count_negative"),
    (res2/res1).alias("compare")
).filter(col("compare").isNotNull()).orderBy(col("compare").asc())
count_total.show()
