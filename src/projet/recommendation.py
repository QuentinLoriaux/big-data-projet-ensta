import sqlite3
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, explode, log1p, from_json, sum  as spark_sum
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType

# On utilise toujours Parquet pour le dataset de reviews car il est meilleur en tout point que le csv

def popularity_ranking(batch_size=10_000, partition_number=10):
    """
    batch_size: Taille du batch
    partition_number: Nombre de partitions
    """

    sqlite_db_path = "../../dataset/users"

    
    columns = ["games"]
    game_schema = ArrayType(
    StructType([
        StructField("appid", IntegerType(), False),
        StructField("playtime_forever", IntegerType(), True),
        StructField("is_recommended", IntegerType(), True)
    ])
    )

    spark = SparkSession.builder.appName("recommendation")\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.shuffle.spill", "true") 
    spark.sparkContext.setCheckpointDir(dirName="/tmp/spark-checkpoints")
    
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM accounts")
    total_rows = cursor.fetchone()[0]/30

    df_final = None
    offset = 0

    while offset < total_rows:
        print(f"Processing {offset}/{total_rows}")
        cursor.execute(f"SELECT games FROM accounts LIMIT {batch_size} OFFSET {offset}")
        data = cursor.fetchall()
        offset += batch_size

        if not data:
            break

        df = spark.createDataFrame(data, schema=columns)

        # Transformation de la colonne games en colonnes
        df = df.withColumn("games", from_json(col("games"), game_schema))
        df = df.withColumn("game", explode(col("games"))).select(
            col("game.appid").alias("appid"),
            col("game.playtime_forever"),
            col("game.is_recommended")
        )

        # Calcul du score de popularité pondéré
        df = df.groupBy("appid").agg(
            spark_sum("is_recommended").alias("total_recommendations"),
            spark_sum("playtime_forever").alias("total_playtime")
        )

        if df_final is None:   
            df_final = df
        else:
            df_final = df_final.union(df).groupBy("appid").agg(
                spark_sum("total_recommendations").alias("total_recommendations"),
                spark_sum("total_playtime").alias("total_playtime")
            ).repartition(partition_number).checkpoint(eager=True) # On libère la mémoire des batchs utilisés pour ne conserver que le résultat
        
        df.unpersist()
        del df
        spark.sparkContext.getOrCreate()._jvm.System.gc() 


    df_final = df_final.withColumn(
        "score", col("total_recommendations") * log1p(col("total_playtime"))
    )

    # jointure pour retrouver le nom des jeux
    df = spark.read.parquet("../../dataset/steam_reviews.parquet")
    df_final = df_final.join(df, df_final.appid == df.app_id).select(
        df.app_name, df_final.score
    ).distinct().orderBy(col("score").desc())

    conn.close()

    return df_final




def user_specific_recommendation():
    return None





if __name__ == "__main__":
    from __init__ import benchmark



    benchmark(lambda: popularity_ranking(), setSpark=False)
    benchmark(lambda: popularity_ranking(), setSpark=False)
    benchmark(lambda: popularity_ranking(), setSpark=False)
    benchmark(lambda: popularity_ranking(), setSpark=False)
    benchmark(lambda: popularity_ranking(), setSpark=False)



