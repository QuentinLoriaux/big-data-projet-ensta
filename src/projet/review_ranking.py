import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, sum as spark_sum



def naive_notation(ext="parquet"):
    spark = SparkSession.builder.appName("notation").getOrCreate()

    if ext == "parquet":
        df = spark.read.parquet("../../dataset/steam_reviews.parquet")
    else:
        df = spark.read.csv("../../dataset/steam_reviews.csv", header=True, inferSchema=True)

    count_df = df.groupBy("app_name").agg(
    spark_sum((col("review_score").cast("int"))).alias("total_score_count"),
    )
    
    return count_df.orderBy(col("total_score_count").desc())




def token_aware_notation(ext="parquet"):

    spark = SparkSession.builder.appName("notation").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    if ext == "parquet":
        df = spark.read.parquet("../../dataset/steam_reviews.parquet")
    else:
        df = spark.read.csv("../../dataset/steam_reviews.csv", header=True, inferSchema=True)

    df = df.withColumn("word_count", size(split(col("review_text"), " ")))
    word_count_df = df.groupBy("app_name").agg(
        spark_sum((col("review_score") == 1).cast("int") * col("word_count") * (col("review_votes") + 1) ).alias("positive_word_count"),
        spark_sum((col("review_score") == -1).cast("int") * col("word_count") * (col("review_votes") + 1) ).alias("negative_word_count")
    )
    word_count_df = word_count_df.withColumn("final_count", col("positive_word_count") - col("negative_word_count"))
    return word_count_df.orderBy(col("final_count").desc())




if __name__ == "__main__":
    from __init__ import benchmark

    filetype = "parquet"
    if len(sys.argv) < 2:
        print("Usage: python script.py <csv|parquet>\n Defaulting to parquet")
    else :
        if sys.argv[1] == "csv":
            filetype = "csv"
    

    # benchmark(lambda: naive_notation(filetype))
    benchmark(lambda: token_aware_notation(filetype))

