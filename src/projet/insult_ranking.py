import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, count



def map_reduce_insult(ext="parquet"):

    spark = SparkSession.builder.appName("swear").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    if ext == "parquet":
        df = spark.read.parquet("../../dataset/steam_reviews.parquet")
    else:
        df = spark.read.csv("../../dataset/steam_reviews.csv", header=True, inferSchema=True)


    with open("../../dataset/swearWords.txt", "r") as f:
        swear_list = [line.strip().lower() for line in f.readlines()]
    swear_broadcast = spark.sparkContext.broadcast(set(swear_list))

    rdd = df.select("review_text").rdd.flatMap(lambda x: x).filter(lambda x: x is not None)

    rdd_tokens = rdd.flatMap(lambda text: text.lower().split())
    rdd_swears = rdd_tokens.filter(lambda word: word in swear_broadcast.value)

    rdd_word_counts = rdd_swears.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    return spark.createDataFrame(rdd_word_counts, ["word", "count"]).orderBy("count", ascending=False)




def spark_insult(ext="parquet"):

    spark = SparkSession.builder.appName("swear").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    if ext == "parquet":
        df = spark.read.parquet("../../dataset/steam_reviews.parquet")
    else:
        df = spark.read.csv("../../dataset/steam_reviews.csv", header=True, inferSchema=True)


    with open("../../dataset/swearWords.txt", "r") as f:
        swear = [line.strip() for line in f.readlines()]
    swear_broadcast = spark.sparkContext.broadcast(swear)

    res_df = df.withColumn("word", explode(split(lower(col("review_text")), " ")))
    res = res_df.filter(col("word").isin(swear_broadcast.value))
    return  res.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc())




if __name__ == "__main__":
    from __init__ import benchmark

    filetype = "parquet"
    if len(sys.argv) < 3:
        print("Usage: python script.py <csv|parquet> <map_reduce|spark>")
    else :
        if sys.argv[1] == "csv":
            filetype = "csv"
        if sys.argv[2] == "map_reduce":
            benchmark(lambda: map_reduce_insult(filetype))
            # benchmark(lambda: map_reduce_insult(filetype))
            # benchmark(lambda: map_reduce_insult(filetype))
            # benchmark(lambda: map_reduce_insult(filetype))
            # benchmark(lambda: map_reduce_insult(filetype))
        else:
            benchmark(lambda: spark_insult(filetype))
            # benchmark(lambda: spark_insult(filetype))
            # benchmark(lambda: spark_insult(filetype))
            # benchmark(lambda: spark_insult(filetype))
            # benchmark(lambda: spark_insult(filetype))

